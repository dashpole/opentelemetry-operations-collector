// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build windows

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/windowseventlog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/windowseventlogreceiver"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"
)

const (
	securityChannelName = "Security"
	benchChannelName    = "TangoBenchLog"
	benchSourceName     = "TangoBenchSource"
	eventLogRegBase     = `SYSTEM\CurrentControlSet\Services\EventLog\`
)

var (
	modadvapi32            = windows.NewLazySystemDLL("advapi32.dll")
	procRegisterEventSrcW  = modadvapi32.NewProc("RegisterEventSourceW")
	procDeregisterEventSrc = modadvapi32.NewProc("DeregisterEventSource")
	procReportEventW       = modadvapi32.NewProc("ReportEventW")
)

type storageHost struct {
	component.Host
	extensions map[component.ID]component.Component
}

func (h *storageHost) GetExtensions() map[component.ID]component.Component {
	return h.extensions
}

type batchStat struct {
	batchIndex int
	events     int
	at         time.Time
	duration   time.Duration
}

type backlogSink struct {
	mu             sync.Mutex
	targetEvents   int
	totalEvents    int
	startTime      time.Time
	firstBatchTime time.Time
	lastBatchEnd   time.Time
	doneTime       time.Time
	batches        []batchStat
	doneOnce       sync.Once
	doneCh         chan struct{}
}

func newBacklogSink(target int, start time.Time) *backlogSink {
	return &backlogSink{
		targetEvents: target,
		startTime:    start,
		lastBatchEnd: start,
		doneCh:       make(chan struct{}),
	}
}

func (s *backlogSink) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (s *backlogSink) ConsumeLogs(_ context.Context, ld plog.Logs) error {
	now := time.Now()
	count := ld.LogRecordCount()
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.totalEvents >= s.targetEvents {
		return nil
	}
	if s.firstBatchTime.IsZero() {
		s.firstBatchTime = now
	}
	batchDur := now.Sub(s.lastBatchEnd)
	s.lastBatchEnd = now
	s.totalEvents += count
	s.batches = append(s.batches, batchStat{
		batchIndex: len(s.batches) + 1,
		events:     count,
		at:         now,
		duration:   batchDur,
	})
	if s.totalEvents >= s.targetEvents {
		s.doneOnce.Do(func() {
			s.doneTime = now
			close(s.doneCh)
		})
	}
	return nil
}

type liveArrivalSink struct {
	ch chan time.Time
}

func (s *liveArrivalSink) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (s *liveArrivalSink) ConsumeLogs(_ context.Context, ld plog.Logs) error {
	now := time.Now()
	for i := 0; i < ld.LogRecordCount(); i++ {
		select {
		case s.ch <- now:
		default:
		}
	}
	return nil
}

type singleRecordSink struct {
	ch chan plog.LogRecord
}

func (s *singleRecordSink) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (s *singleRecordSink) ConsumeLogs(_ context.Context, ld plog.Logs) error {
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				// Skip the initial EventID 1102 ("The audit log was cleared") written by wevtutil cl
				// so we inspect a genuine Microsoft-Windows-Security-Auditing event with <EventData>.
				if orig, ok := lr.Attributes().Get("log.record.original"); ok {
					if strings.Contains(orig.Str(), "<EventID>1102</EventID>") {
						continue
					}
				}
				rec := plog.NewLogRecord()
				lr.CopyTo(rec)
				select {
				case s.ch <- rec:
				default:
				}
				return nil
			}
		}
	}
	return nil
}

func ensureBenchEventLogChannel() error {
	logKeyPath := eventLogRegBase + benchChannelName
	logKey, _, err := registry.CreateKey(registry.LOCAL_MACHINE, logKeyPath, registry.ALL_ACCESS)
	if err != nil {
		return fmt.Errorf("create registry key %s: %w", logKeyPath, err)
	}
	_ = logKey.SetDWordValue("MaxSize", 64*1024*1024)
	_ = logKey.SetDWordValue("Retention", 0)
	_ = logKey.Close()

	srcKeyPath := logKeyPath + `\` + benchSourceName
	srcKey, _, err := registry.CreateKey(registry.LOCAL_MACHINE, srcKeyPath, registry.ALL_ACCESS)
	if err != nil {
		return fmt.Errorf("create registry key %s: %w", srcKeyPath, err)
	}
	_ = srcKey.SetExpandStringValue("EventMessageFile", `%SystemRoot%\System32\EventCreate.exe`)
	_ = srcKey.SetDWordValue("TypesSupported", 7)
	_ = srcKey.Close()

	_ = exec.Command("wevtutil.exe", "cl", benchChannelName).Run()
	return nil
}

// populateSecurityChannel populates the Windows Security channel with >= targetEvents
// genuine Microsoft-Windows-Security-Auditing events (EventID 5156 / 5158 / 4624 / 4672)
// by temporarily enabling Filtering Platform Connection auditing while making loopback
// TCP connections, then disabling the audit policy so the Security channel remains static
// during the serial benchmark runs.
func populateSecurityChannel(targetEvents int) error {
	_ = exec.Command("wevtutil.exe", "sl", securityChannelName, "/ms:134217728").Run()
	_ = exec.Command("wevtutil.exe", "cl", securityChannelName).Run()

	if err := exec.Command("auditpol.exe", "/set", "/subcategory:Filtering Platform Connection", "/success:enable", "/failure:enable").Run(); err != nil {
		return fmt.Errorf("enable Filtering Platform Connection audit: %w", err)
	}
	defer func() {
		_ = exec.Command("auditpol.exe", "/set", "/subcategory:Filtering Platform Connection", "/success:disable", "/failure:disable").Run()
	}()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer ln.Close()

	addr := ln.Addr().String()
	// Each loopback TCP connect/accept generates ~2-3 WFP Security events (5156).
	pairs := (targetEvents / 2) + 400
	for i := 0; i < pairs; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			continue
		}
		srv, err := ln.Accept()
		if err == nil {
			_ = srv.Close()
		}
		_ = conn.Close()
	}

	_ = exec.Command("auditpol.exe", "/set", "/subcategory:Filtering Platform Connection", "/success:disable", "/failure:disable").Run()
	// Wait briefly for Windows Event Log service to flush all pending WFP audit events.
	time.Sleep(1000 * time.Millisecond)
	return nil
}

func openEventSource() (windows.Handle, error) {
	srcPtr, err := windows.UTF16PtrFromString(benchSourceName)
	if err != nil {
		return 0, err
	}
	r1, _, e1 := procRegisterEventSrcW.Call(0, uintptr(unsafe.Pointer(srcPtr)))
	if r1 == 0 {
		return 0, fmt.Errorf("RegisterEventSourceW failed: %v", e1)
	}
	return windows.Handle(r1), nil
}

func closeEventSource(h windows.Handle) {
	if h != 0 {
		_, _, _ = procDeregisterEventSrc.Call(uintptr(h))
	}
}

func writeBenchEvent(h windows.Handle, eventID uint32, msg string) error {
	msgPtr, err := windows.UTF16PtrFromString(msg)
	if err != nil {
		return err
	}
	strs := []*uint16{msgPtr}
	const eventLogInformationType = 0x0004
	r1, _, e1 := procReportEventW.Call(
		uintptr(h),
		uintptr(eventLogInformationType),
		0,
		uintptr(eventID),
		0,
		1,
		0,
		uintptr(unsafe.Pointer(&strs[0])),
		0,
	)
	if r1 == 0 {
		return fmt.Errorf("ReportEventW failed: %v", e1)
	}
	return nil
}

func newStorageHost(ctx context.Context, dir string) (*storageHost, func(), error) {
	if err := os.RemoveAll(dir); err != nil {
		return nil, nil, err
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, nil, err
	}

	fsFactory := filestorage.NewFactory()
	fsCfg := fsFactory.CreateDefaultConfig().(*filestorage.Config)
	fsCfg.Directory = dir

	fsID := component.MustNewID("file_storage")
	set := extensiontest.NewNopSettings(fsFactory.Type())
	set.ID = fsID
	ext, err := fsFactory.Create(ctx, set, fsCfg)
	if err != nil {
		return nil, nil, err
	}
	host := &storageHost{
		Host: componenttest.NewNopHost(),
		extensions: map[component.ID]component.Component{
			fsID: ext,
		},
	}
	if err := ext.Start(ctx, host); err != nil {
		return nil, nil, err
	}
	cleanup := func() {
		_ = ext.Shutdown(ctx)
		_ = os.RemoveAll(dir)
	}
	return host, cleanup, nil
}

func buildBaselineConfig(channelName string) *windowseventlogreceiver.WindowsLogConfig {
	factory := windowseventlogreceiver.NewFactory()
	cfg := factory.CreateDefaultConfig().(*windowseventlogreceiver.WindowsLogConfig)
	cfg.InputConfig.Channel = channelName
	return cfg
}

func buildPrototypeConfigFromPolicy(ctx context.Context, channelName string) (*windowseventlogreceiver.WindowsLogConfig, error) {
	pol := &windowseventlog.WindowsEventLogSourcePolicy{
		ID:       "tango-bench-policy",
		Channels: []string{channelName},
	}
	conf, err := pol.Evaluate(ctx)
	if err != nil {
		return nil, err
	}
	subKey := "receivers::windowseventlog/" + strings.ToLower(channelName)
	sub, err := conf.Sub(subKey)
	if err != nil {
		return nil, err
	}
	factory := windowseventlogreceiver.NewFactory()
	cfg := factory.CreateDefaultConfig().(*windowseventlogreceiver.WindowsLogConfig)
	if err := sub.Unmarshal(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

type backlogResult struct {
	name                 string
	events               int
	batches              int
	initialStartupWait   time.Duration
	totalWallDuration    time.Duration
	activeDrainDuration  time.Duration
	sustainedEventsPerS  float64
	wallEventsPerS       float64
	meanSteadyBatchMs    float64
	medianSteadyBatchMs  float64
	p95SteadyBatchMs     float64
	activePerEventUs     float64
	suppressRender       bool
	eventDriven          bool
	maxReads             int
	ignoreChanErrors     bool
}

func runBacklogBenchmark(ctx context.Context, label string, cfg *windowseventlogreceiver.WindowsLogConfig, targetEvents int, storageDir string, configuredWait time.Duration) (*backlogResult, error) {
	host, cleanup, err := newStorageHost(ctx, storageDir)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	fsID := component.MustNewID("file_storage")
	cfg.BaseConfig.StorageID = &fsID
	cfg.InputConfig.StartAt = "beginning"

	factory := windowseventlogreceiver.NewFactory()
	start := time.Now()
	sink := newBacklogSink(targetEvents, start)

	set := receivertest.NewNopSettings(factory.Type())
	set.ID = component.NewIDWithName(factory.Type(), strings.ToLower(cfg.InputConfig.Channel))

	r, err := factory.CreateLogs(ctx, set, cfg, sink)
	if err != nil {
		return nil, err
	}
	if err := r.Start(ctx, host); err != nil {
		return nil, err
	}

	select {
	case <-sink.doneCh:
	case <-time.After(120 * time.Second):
		_ = r.Shutdown(ctx)
		return nil, fmt.Errorf("%s timed out waiting for %d events (got %d)", label, targetEvents, sink.totalEvents)
	}

	totalWall := sink.doneTime.Sub(sink.startTime)
	firstBatchWait := sink.firstBatchTime.Sub(sink.startTime)
	// Active drain begins right after the initial poll_interval (1s) or wait_timeout (5s) elapses.
	activeDrain := totalWall - configuredWait
	if activeDrain <= 0 {
		activeDrain = sink.doneTime.Sub(sink.firstBatchTime)
	}
	_ = r.Shutdown(ctx)

	// Compute steady-state inter-batch latencies (batches[1:] exclude the initial startup wait before batch[0]).
	steadyBatches := sink.batches
	if len(steadyBatches) > 1 {
		steadyBatches = steadyBatches[1:]
	}
	batchMs := make([]float64, 0, len(steadyBatches))
	var sumBatchMs float64
	for _, b := range steadyBatches {
		ms := float64(b.duration.Microseconds()) / 1000.0
		batchMs = append(batchMs, ms)
		sumBatchMs += ms
	}
	slices.Sort(batchMs)
	meanBatch := sumBatchMs / float64(len(batchMs))
	medianBatch := batchMs[len(batchMs)/2]
	p95Idx := int(float64(len(batchMs)) * 0.95)
	if p95Idx >= len(batchMs) {
		p95Idx = len(batchMs) - 1
	}
	p95Batch := batchMs[p95Idx]

	sustainedEps := float64(sink.totalEvents) / activeDrain.Seconds()
	wallEps := float64(sink.totalEvents) / totalWall.Seconds()
	activeEventUs := float64(activeDrain.Microseconds()) / float64(sink.totalEvents)

	return &backlogResult{
		name:                label,
		events:              sink.totalEvents,
		batches:             len(sink.batches),
		initialStartupWait:  firstBatchWait,
		totalWallDuration:   totalWall,
		activeDrainDuration: activeDrain,
		sustainedEventsPerS: sustainedEps,
		wallEventsPerS:      wallEps,
		meanSteadyBatchMs:   meanBatch,
		medianSteadyBatchMs: medianBatch,
		p95SteadyBatchMs:    p95Batch,
		activePerEventUs:    activeEventUs,
		suppressRender:      cfg.InputConfig.SuppressRenderingInfo,
		eventDriven:         cfg.InputConfig.EventDrivenScraping,
		maxReads:            cfg.InputConfig.MaxReads,
		ignoreChanErrors:    cfg.InputConfig.IgnoreChannelErrors,
	}, nil
}

type liveLatencyResult struct {
	name      string
	samplesMs []float64
	minMs     float64
	medianMs  float64
	meanMs    float64
	p95Ms     float64
	maxMs     float64
}

func runLiveLatencyBenchmark(ctx context.Context, label string, cfg *windowseventlogreceiver.WindowsLogConfig, numSamples int, storageDir string) (*liveLatencyResult, error) {
	host, cleanup, err := newStorageHost(ctx, storageDir)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	fsID := component.MustNewID("file_storage")
	cfg.BaseConfig.StorageID = &fsID
	cfg.InputConfig.StartAt = "end"

	factory := windowseventlogreceiver.NewFactory()
	sink := &liveArrivalSink{ch: make(chan time.Time, numSamples*2)}

	set := receivertest.NewNopSettings(factory.Type())
	set.ID = component.NewIDWithName(factory.Type(), strings.ToLower(cfg.InputConfig.Channel))

	r, err := factory.CreateLogs(ctx, set, cfg, sink)
	if err != nil {
		return nil, err
	}
	if err := r.Start(ctx, host); err != nil {
		return nil, err
	}
	defer func() { _ = r.Shutdown(ctx) }()

	// Wait 5.5s so both Baseline (1s poll_interval) and Prototype (5s initial wait_timeout
	// before initial EvtNext drain arms SignalEvent) are in steady-state tailing.
	time.Sleep(5500 * time.Millisecond)

	h, err := openEventSource()
	if err != nil {
		return nil, err
	}
	defer closeEventSource(h)

	// Use non-harmonic prime intervals so event arrivals spread evenly across poll/flush cycles.
	sleepIntervalsMs := []int{137, 211, 307, 173, 251, 193, 283, 157, 229, 269}
	samples := make([]float64, 0, numSamples)
	var sumMs float64

	for i := 0; i < numSamples; i++ {
		delayMs := sleepIntervalsMs[i%len(sleepIntervalsMs)]
		time.Sleep(time.Duration(delayMs) * time.Millisecond)
		sendTime := time.Now()
		msg := fmt.Sprintf("TangoLiveEvent mode=%s sample=%d sent=%s", label, i+1, sendTime.Format(time.RFC3339Nano))
		if err := writeBenchEvent(h, uint32(201+i), msg); err != nil {
			return nil, err
		}
		select {
		case recvTime := <-sink.ch:
			latMs := float64(recvTime.Sub(sendTime).Microseconds()) / 1000.0
			samples = append(samples, latMs)
			sumMs += latMs
		case <-time.After(10 * time.Second):
			return nil, fmt.Errorf("%s timed out waiting for live event %d", label, i+1)
		}
	}

	sorted := slices.Clone(samples)
	slices.Sort(sorted)
	p95Idx := int(float64(len(sorted)) * 0.95)
	if p95Idx >= len(sorted) {
		p95Idx = len(sorted) - 1
	}

	return &liveLatencyResult{
		name:      label,
		samplesMs: samples,
		minMs:     sorted[0],
		medianMs:  sorted[len(sorted)/2],
		meanMs:    sumMs / float64(len(sorted)),
		p95Ms:     sorted[p95Idx],
		maxMs:     sorted[len(sorted)-1],
	}, nil
}

func runMissingChannelCheck(ctx context.Context) error {
	const missingChannel = "TangoNonExistentChannel_9999"
	factory := windowseventlogreceiver.NewFactory()

	baseCfg := buildBaselineConfig(missingChannel)
	baseCore, baseLogs := observer.New(zapcore.DebugLevel)
	baseSet := receivertest.NewNopSettings(factory.Type())
	baseSet.Logger = zap.New(baseCore)

	baseRcvr, err := factory.CreateLogs(ctx, baseSet, baseCfg, &singleRecordSink{ch: make(chan plog.LogRecord, 1)})
	if err != nil {
		return err
	}
	baseStartErr := baseRcvr.Start(ctx, componenttest.NewNopHost())
	_ = baseRcvr.Shutdown(ctx)

	protoCfg, err := buildPrototypeConfigFromPolicy(ctx, missingChannel)
	if err != nil {
		return err
	}
	protoCore, protoLogs := observer.New(zapcore.DebugLevel)
	protoSet := receivertest.NewNopSettings(factory.Type())
	protoSet.Logger = zap.New(protoCore)

	protoRcvr, err := factory.CreateLogs(ctx, protoSet, protoCfg, &singleRecordSink{ch: make(chan plog.LogRecord, 1)})
	if err != nil {
		return err
	}
	protoStartErr := protoRcvr.Start(ctx, componenttest.NewNopHost())
	_ = protoRcvr.Shutdown(ctx)

	fmt.Println("=== CHECK 3: FAIL-OPEN MISSING CHANNEL (TangoNonExistentChannel_9999) ===")
	fmt.Printf("Baseline (ignore_channel_errors=%v): r.Start() error = %v (logged entries: %d)\n",
		baseCfg.InputConfig.IgnoreChannelErrors, baseStartErr, baseLogs.Len())
	fmt.Printf("Prototype (ignore_channel_errors=%v): r.Start() error = %v (logged entries: %d)\n",
		protoCfg.InputConfig.IgnoreChannelErrors, protoStartErr, protoLogs.Len())
	for _, entry := range protoLogs.All() {
		if entry.Level >= zapcore.WarnLevel {
			fmt.Printf("  Prototype WARN Log: %s %v\n", entry.Message, entry.ContextMap())
		}
	}
	fmt.Println()
	return nil
}

func captureOneRecord(ctx context.Context, cfg *windowseventlogreceiver.WindowsLogConfig) (plog.LogRecord, error) {
	cfg.InputConfig.StartAt = "beginning"
	cfg.InputConfig.MaxReads = 2
	cfg.InputConfig.IncludeLogRecordOriginal = true

	factory := windowseventlogreceiver.NewFactory()
	sink := &singleRecordSink{ch: make(chan plog.LogRecord, 1)}
	set := receivertest.NewNopSettings(factory.Type())

	r, err := factory.CreateLogs(ctx, set, cfg, sink)
	if err != nil {
		return plog.LogRecord{}, err
	}
	if err := r.Start(ctx, componenttest.NewNopHost()); err != nil {
		return plog.LogRecord{}, err
	}
	defer func() { _ = r.Shutdown(ctx) }()

	select {
	case rec := <-sink.ch:
		return rec, nil
	case <-time.After(10 * time.Second):
		return plog.LogRecord{}, fmt.Errorf("timed out capturing single record")
	}
}

func runPayloadComparison(ctx context.Context, channelName string) error {
	baseCfg := buildBaselineConfig(channelName)
	baseRec, err := captureOneRecord(ctx, baseCfg)
	if err != nil {
		return fmt.Errorf("baseline captureOneRecord: %w", err)
	}

	protoCfg, err := buildPrototypeConfigFromPolicy(ctx, channelName)
	if err != nil {
		return fmt.Errorf("buildPrototypeConfigFromPolicy: %w", err)
	}
	protoRec, err := captureOneRecord(ctx, protoCfg)
	if err != nil {
		return fmt.Errorf("prototype captureOneRecord: %w", err)
	}

	baseOrigVal, _ := baseRec.Attributes().Get("log.record.original")
	protoOrigVal, _ := protoRec.Attributes().Get("log.record.original")
	baseXML := baseOrigVal.Str()
	protoXML := protoOrigVal.Str()

	baseBodyJSON, _ := json.MarshalIndent(baseRec.Body().Map().AsRaw(), "", "  ")
	protoBodyJSON, _ := json.MarshalIndent(protoRec.Body().Map().AsRaw(), "", "  ")

	fmt.Printf("=== CHECK 4: PAYLOAD & SCHEMA PRESERVATION ON %q CHANNEL (RAW XML + DEFAULT STRUCTURED MAP) ===\n", channelName)
	fmt.Printf("Baseline Raw XML (suppress_rendering_info=false) [%d bytes]:\n%s\n\n", len(baseXML), baseXML)
	fmt.Printf("Prototype Raw XML (suppress_rendering_info=true) [%d bytes]:\n%s\n\n", len(protoXML), protoXML)
	fmt.Printf("Raw XML Payload Reduction: %d bytes -> %d bytes (%.1f%% smaller, %.2fx compression)\n\n",
		len(baseXML), len(protoXML),
		100.0*(1.0-float64(len(protoXML))/float64(len(baseXML))),
		float64(len(baseXML))/float64(len(protoXML)))

	fmt.Printf("Baseline Structured Body().Map() (SeverityNumber=%d, SeverityText=%q):\n%s\n\n",
		baseRec.SeverityNumber(), baseRec.SeverityText(), string(baseBodyJSON))
	fmt.Printf("Prototype Structured Body().Map() (SeverityNumber=%d, SeverityText=%q):\n%s\n\n",
		protoRec.SeverityNumber(), protoRec.SeverityText(), string(protoBodyJSON))
	return nil
}

func main() {
	numEvents := flag.Int("events", 3000, "number of events to pre-populate in Security channel for backlog benchmark")
	numLive := flag.Int("live", 10, "number of live events for arrival latency benchmark")
	flag.Parse()

	ctx := context.Background()

	fmt.Println("================================================================================")
	fmt.Println("GBOC Tab 4: End-to-End windowseventlogreceiver Benchmark on Windows Server 2022")
	fmt.Println("================================================================================")
	fmt.Printf("1. Populating >= %d real Microsoft-Windows-Security-Auditing events in %q channel...\n", *numEvents, securityChannelName)

	popStart := time.Now()
	if err := populateSecurityChannel(*numEvents); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: populateSecurityChannel failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Populated %q channel in %v.\n\n", securityChannelName, time.Since(popStart))

	// Check 4: Payload & Schema Preservation on real Security channel
	if err := runPayloadComparison(ctx, securityChannelName); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: runPayloadComparison failed: %v\n", err)
		os.Exit(1)
	}

	// Check 1: Serial Backlog Throughput & Batch Latency on real Security channel
	fmt.Printf("=== CHECK 1: SERIAL BACKLOG THROUGHPUT & BATCH LATENCY (%d SECURITY EVENTS) ===\n", *numEvents)
	baseCfg := buildBaselineConfig(securityChannelName)
	baseRes, err := runBacklogBenchmark(ctx, "Baseline (Upstream v0.162.0 Defaults)", baseCfg, *numEvents, `C:\proto\storage_baseline`, 1*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: Baseline backlog benchmark failed: %v\n", err)
		os.Exit(1)
	}

	protoCfg, err := buildPrototypeConfigFromPolicy(ctx, securityChannelName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: buildPrototypeConfigFromPolicy failed: %v\n", err)
		os.Exit(1)
	}
	protoRes, err := runBacklogBenchmark(ctx, "Prototype (WindowsEventLogSourcePolicy Defaults)", protoCfg, *numEvents, `C:\proto\storage_proto`, 5*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: Prototype backlog benchmark failed: %v\n", err)
		os.Exit(1)
	}

	for _, r := range []*backlogResult{baseRes, protoRes} {
		fmt.Printf("[%s]\n", r.name)
		fmt.Printf("  Config: suppress_rendering_info=%v, event_driven_scraping=%v, max_reads=%d, ignore_channel_errors=%v\n",
			r.suppressRender, r.eventDriven, r.maxReads, r.ignoreChanErrors)
		fmt.Printf("  Events Ingested         : %d across %d ConsumeLogs emitter batches\n", r.events, r.batches)
		fmt.Printf("  Initial Startup Wait    : %v (firstBatchTime - r.Start)\n", r.initialStartupWait)
		fmt.Printf("  Active Drain Duration   : %v (%.2f ms)\n", r.activeDrainDuration, float64(r.activeDrainDuration.Microseconds())/1000.0)
		fmt.Printf("  Total Wall Duration     : %v (%.2f ms, including initial wait)\n", r.totalWallDuration, float64(r.totalWallDuration.Microseconds())/1000.0)
		fmt.Printf("  Sustained Drain Rate    : %.2f events/sec (active per-event cost: %.2f us/event)\n", r.sustainedEventsPerS, r.activePerEventUs)
		fmt.Printf("  Steady-State Batch Time : mean=%.2f ms/batch, median=%.2f ms/batch, p95=%.2f ms/batch\n\n",
			r.meanSteadyBatchMs, r.medianSteadyBatchMs, r.p95SteadyBatchMs)
	}
	fmt.Printf("--> SUSTAINED BACKLOG DRAIN SPEEDUP: %.2fx (%.2f events/sec -> %.2f events/sec)\n",
		protoRes.sustainedEventsPerS/baseRes.sustainedEventsPerS, baseRes.sustainedEventsPerS, protoRes.sustainedEventsPerS)
	fmt.Printf("--> PER-EVENT COST REDUCTION       : %.2fx (%.2f us/event -> %.2f us/event)\n\n",
		baseRes.activePerEventUs/protoRes.activePerEventUs, baseRes.activePerEventUs, protoRes.activePerEventUs)

	// Check 2: Serial Live Arrival-to-Delivery Latency on TangoBenchLog
	if err := ensureBenchEventLogChannel(); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: ensureBenchEventLogChannel failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("=== CHECK 2: STEADY-STATE LIVE EVENT ARRIVAL-TO-DELIVERY LATENCY (%d EVENTS) ===\n", *numLive)
	baseLiveCfg := buildBaselineConfig(benchChannelName)
	baseLiveRes, err := runLiveLatencyBenchmark(ctx, "Baseline (poll_interval=1s, event_driven_scraping=false)", baseLiveCfg, *numLive, `C:\proto\storage_live_base`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: Baseline live latency benchmark failed: %v\n", err)
		os.Exit(1)
	}

	protoLiveCfg, err := buildPrototypeConfigFromPolicy(ctx, benchChannelName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: buildPrototypeConfigFromPolicy failed: %v\n", err)
		os.Exit(1)
	}
	protoLiveRes, err := runLiveLatencyBenchmark(ctx, "Prototype (event_driven_scraping=true)", protoLiveCfg, *numLive, `C:\proto\storage_live_proto`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: Prototype live latency benchmark failed: %v\n", err)
		os.Exit(1)
	}

	for _, lr := range []*liveLatencyResult{baseLiveRes, protoLiveRes} {
		fmt.Printf("[%s]\n", lr.name)
		fmt.Printf("  Samples (ms): ")
		for i, v := range lr.samplesMs {
			if i > 0 {
				fmt.Print(", ")
			}
			fmt.Printf("%.2f", v)
		}
		fmt.Println()
		fmt.Printf("  Summary     : min=%.2f ms, median(p50)=%.2f ms, mean=%.2f ms, p95=%.2f ms, max=%.2f ms\n\n",
			lr.minMs, lr.medianMs, lr.meanMs, lr.p95Ms, lr.maxMs)
	}
	fmt.Printf("--> LIVE DELIVERY LATENCY REDUCTION: median %.2f ms -> %.2f ms (%.1fx faster), mean %.2f ms -> %.2f ms (%.1fx faster)\n\n",
		baseLiveRes.medianMs, protoLiveRes.medianMs, baseLiveRes.medianMs/protoLiveRes.medianMs,
		baseLiveRes.meanMs, protoLiveRes.meanMs, baseLiveRes.meanMs/protoLiveRes.meanMs)

	// Check 3: Fail-Open Missing Channel
	if err := runMissingChannelCheck(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: runMissingChannelCheck failed: %v\n", err)
		os.Exit(1)
	}
}
