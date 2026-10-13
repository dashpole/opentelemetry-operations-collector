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

package filelog_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/processor/googlepolicyprocessor"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/filelog"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/gcpdestination"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/pkg/googlepolicy"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/googleclientauthextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/filelogreceiver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configauth"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

type authHost struct {
	component.Host
	exts map[component.ID]component.Component
}

func (h *authHost) GetExtensions() map[component.ID]component.Component {
	return h.exts
}

type exportingLogConsumer struct {
	sink        *consumertest.LogsSink
	next        consumer.Logs
	projectID   string
	exportedCnt atomic.Int64
	exportErr   atomic.Value
}

func (c *exportingLogConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *exportingLogConsumer) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	clone := plog.NewLogs()
	ld.CopyTo(clone)
	for i := 0; i < clone.ResourceLogs().Len(); i++ {
		rl := clone.ResourceLogs().At(i)
		rl.Resource().Attributes().PutStr("gcp.project_id", c.projectID)
	}
	_ = c.sink.ConsumeLogs(ctx, clone)
	if c.next != nil {
		if err := c.next.ConsumeLogs(ctx, clone); err != nil {
			c.exportErr.Store(err)
			return err
		}
		c.exportedCnt.Add(int64(clone.LogRecordCount()))
	}
	return nil
}

func TestE2E_FileLogEnterpriseLogs_ExcludeNegativeControl_OSDAC_AndLiveGCPExport(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create simulated enterprise directory hierarchy:
	// 1. Oracle OFA audit trail directory (<tmpdir>/u01/app/oracle/admin/ORCL/adump)
	// 2. Linux COTS log directory (<tmpdir>/var/adm)
	adumpDir := filepath.Join(tmpDir, "u01", "app", "oracle", "admin", "ORCL", "adump")
	varAdmDir := filepath.Join(tmpDir, "var", "adm")
	require.NoError(t, os.MkdirAll(adumpDir, 0755))
	require.NoError(t, os.MkdirAll(varAdmDir, 0755))

	oracleAudFile := filepath.Join(adumpDir, "ora_9876.aud")
	oracleAudLine := "Audit file /u01/app/oracle/admin/ORCL/adump/ora_9876.aud | ACTION : [100] 'LOGON' | DATABASE USER:[1] '/' | PRIVILEGE : [6] 'SYSDBA' | STATUS:[1] '0'"
	require.NoError(t, os.WriteFile(oracleAudFile, []byte(oracleAudLine+"\n"), 0644))

	cotsLogFile := filepath.Join(varAdmDir, "messages.log")
	cotsLogLine := "Oct 13 19:25:00 enterprise-host sep-agent[4321]: Threat scan completed clean on /u01/app/oracle"
	require.NoError(t, os.WriteFile(cotsLogFile, []byte(cotsLogLine+"\n"), 0644))

	compressedArchiveFile := filepath.Join(adumpDir, "ora_old.aud.gz")
	require.NoError(t, os.WriteFile(compressedArchiveFile, []byte("COMPRESSED_ARCHIVE_PAYLOAD_DO_NOT_INGEST\n"), 0644))

	privateKeyFile := filepath.Join(adumpDir, "server.key")
	require.NoError(t, os.WriteFile(privateKeyFile, []byte("-----BEGIN PRIVATE KEY----- SECRET_KEY_MATERIAL_DO_NOT_INGEST\n"), 0644))

	unreadableDACFile := filepath.Join(adumpDir, "root_only.aud")
	require.NoError(t, os.WriteFile(unreadableDACFile, []byte("ROOT_ONLY_SECRET_AUDIT_DO_NOT_INGEST\n"), 0000))
	t.Cleanup(func() {
		_ = os.Chmod(unreadableDACFile, 0644)
	})

	broadOracleGlob := filepath.Join(tmpDir, "u01", "app", "oracle", "admin", "*", "adump", "*")
	cotsVarAdmGlob := filepath.Join(tmpDir, "var", "adm", "*.log")

	// =========================================================================
	// STEP 1: Negative Control (exclude: nil)
	// Prove non-tautologically that WITHOUT Guardrail 3's synthesized **/*.ext
	// exclude list, a broad glob (<tmpdir>/u01/app/oracle/admin/*/adump/*)
	// ingests server.key and ora_old.aud.gz!
	// =========================================================================
	negSink := new(consumertest.LogsSink)
	flFactory := filelogreceiver.NewFactory()
	negCfg := flFactory.CreateDefaultConfig()
	negMap := confmap.NewFromStringMap(map[string]any{
		"include":           []any{broadOracleGlob},
		"include_file_path": true,
		"start_at":          "beginning",
		"poll_interval":     "50ms",
	})
	require.NoError(t, negMap.Unmarshal(negCfg))

	negReceiver, err := flFactory.CreateLogs(ctx, receivertest.NewNopSettings(flFactory.Type()), negCfg, negSink)
	require.NoError(t, err)
	require.NoError(t, negReceiver.Start(ctx, componenttest.NewNopHost()))
	require.Eventually(t, func() bool {
		return negSink.LogRecordCount() >= 3
	}, 5*time.Second, 50*time.Millisecond)
	require.NoError(t, negReceiver.Shutdown(ctx))

	var negBodies []string
	for _, ld := range negSink.AllLogs() {
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			sl := ld.ResourceLogs().At(i).ScopeLogs()
			for j := 0; j < sl.Len(); j++ {
				lrs := sl.At(j).LogRecords()
				for k := 0; k < lrs.Len(); k++ {
					negBodies = append(negBodies, lrs.At(k).Body().AsString())
				}
			}
		}
	}
	assert.Contains(t, negBodies, "-----BEGIN PRIVATE KEY----- SECRET_KEY_MATERIAL_DO_NOT_INGEST",
		"Negative control (exclude: nil) must ingest server.key, proving Guardrail 3's exclude test is non-tautological")
	assert.Contains(t, negBodies, "COMPRESSED_ARCHIVE_PAYLOAD_DO_NOT_INGEST",
		"Negative control (exclude: nil) must ingest ora_old.aud.gz")
	t.Logf("[Baseline Negative Control (exclude: nil)] Ingested %d records including server.key and ora_old.aud.gz: %v", len(negBodies), negBodies)

	// =========================================================================
	// STEP 2: Proposed Synthesis via googlecontrolplaneprovider + Live Export
	// Evaluate two FileLogSourcePolicy instances + GCPDestinationPolicy
	// ("dashpole-dev"), start real filelogreceiver -> googlepolicyprocessor ->
	// otlpexporter (telemetry.googleapis.com:443), and verify:
	//   1. ora_9876.aud and messages.log are tailed with log.file.path populated
	//   2. server.key and ora_old.aud.gz are excluded by Guardrail 3's **/*.ext list
	//   3. root_only.aud (0000 mode) is skipped by OS POSIX DAC (Guardrail 4)
	//   4. Live export to telemetry.googleapis.com:443 (dashpole-dev) succeeds!
	// =========================================================================
	t.Setenv("FLEET_ID", "fleet-tab6-e2e")
	ps := &googlepolicy.PolicySet{
		PolicySetID: "projects/dashpole-dev/locations/us-central1/policySets/tab6-e2e-policyset",
		RevisionID:  "rev-tab6-e2e-1",
		Policies: map[string]*googlepolicy.PolicySetEntry{
			"dest_dashpole_dev": {
				PolicyObj: &gcpdestination.GCPDestinationPolicy{
					Name:      "dest_dashpole_dev",
					ProjectID: "dashpole-dev",
				},
			},
			"oracle_ofa_policy": {
				PolicyObj: &filelog.FileLogSourcePolicy{
					ID:      "oracle_ofa_policy",
					Include: []string{broadOracleGlob},
				},
			},
			"cots_var_adm_policy": {
				PolicyObj: &filelog.FileLogSourcePolicy{
					ID:      "cots_var_adm_policy",
					Include: []string{cotsVarAdmGlob},
				},
			},
		},
	}
	googlepolicy.SetActivePolicySet(ps)
	t.Cleanup(func() {
		for googlepolicy.ActivePolicySet() != nil {
			googlepolicy.RollbackActivePolicySet()
		}
	})

	prov := googlecontrolplaneprovider.NewFactory().Create(confmaptest.NewNopProviderSettings())
	t.Cleanup(func() {
		_ = prov.Shutdown(ctx)
	})
	ret, err := prov.Retrieve(ctx, "googlecontrolplane:component:tab6-e2e", nil)
	require.NoError(t, err)
	mergedConf, err := ret.AsConf()
	require.NoError(t, err)

	// Extract synthesized receivers::filelog config and unmarshal into real filelogreceiver.Config:
	synthesizedFileLogMap, ok := mergedConf.Get("receivers::filelog").(map[string]any)
	require.True(t, ok, "receivers::filelog must be synthesized as map[string]any")
	synthesizedFileLogMap["start_at"] = "beginning"
	synthesizedFileLogMap["poll_interval"] = "50ms"

	propFlCfg := flFactory.CreateDefaultConfig()
	require.NoError(t, confmap.NewFromStringMap(synthesizedFileLogMap).Unmarshal(propFlCfg))

	// Instantiate real googleclientauthextension + otlpexporter targeting telemetry.googleapis.com:443 in dashpole-dev:
	authFactory := googleclientauthextension.NewFactory()
	authCfg := authFactory.CreateDefaultConfig().(*googleclientauthextension.Config)
	authCfg.Config.Project = "dashpole-dev"
	authID := component.MustNewIDWithName("googleclientauth", "dest_dashpole_dev")
	authExt, err := authFactory.Create(ctx, extensiontest.NewNopSettings(authFactory.Type()), authCfg)
	require.NoError(t, err)
	host := &authHost{
		Host: componenttest.NewNopHost(),
		exts: map[component.ID]component.Component{authID: authExt},
	}
	require.NoError(t, authExt.Start(ctx, host))
	t.Cleanup(func() { _ = authExt.Shutdown(ctx) })

	otlpExpFactory := otlpexporter.NewFactory()
	otlpExpCfg := otlpExpFactory.CreateDefaultConfig().(*otlpexporter.Config)
	otlpExpCfg.ClientConfig.Endpoint = "telemetry.googleapis.com:443"
	otlpExpCfg.ClientConfig.Auth = configoptional.Some(configauth.Config{AuthenticatorID: authID})
	liveExporter, err := otlpExpFactory.CreateLogs(ctx, exportertest.NewNopSettings(otlpExpFactory.Type()), otlpExpCfg)
	require.NoError(t, err)
	require.NoError(t, liveExporter.Start(ctx, host))
	t.Cleanup(func() { _ = liveExporter.Shutdown(ctx) })

	gpFactory := googlepolicyprocessor.NewFactory()
	gpProc, err := gpFactory.CreateLogs(ctx, processortest.NewNopSettings(gpFactory.Type()), gpFactory.CreateDefaultConfig(), liveExporter)
	require.NoError(t, err)
	require.NoError(t, gpProc.Start(ctx, host))
	t.Cleanup(func() { _ = gpProc.Shutdown(ctx) })

	propSink := new(consumertest.LogsSink)
	teeConsumer := &exportingLogConsumer{
		sink:      propSink,
		next:      gpProc,
		projectID: "dashpole-dev",
	}

	propReceiver, err := flFactory.CreateLogs(ctx, receivertest.NewNopSettings(flFactory.Type()), propFlCfg, teeConsumer)
	require.NoError(t, err)
	require.NoError(t, propReceiver.Start(ctx, host))
	require.Eventually(t, func() bool {
		return propSink.LogRecordCount() >= 2 && teeConsumer.exportedCnt.Load() >= 2
	}, 10*time.Second, 50*time.Millisecond)
	require.NoError(t, propReceiver.Shutdown(ctx))

	require.Nil(t, teeConsumer.exportErr.Load(), "live OTLP export to telemetry.googleapis.com:443 (dashpole-dev) must succeed with 0 errors")
	assert.Equal(t, 2, propSink.LogRecordCount(), "only the 2 valid enterprise log records (ora_9876.aud and messages.log) should be ingested")

	ingestedByPath := make(map[string]string)
	for _, ld := range propSink.AllLogs() {
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			sl := ld.ResourceLogs().At(i).ScopeLogs()
			for j := 0; j < sl.Len(); j++ {
				lrs := sl.At(j).LogRecords()
				for k := 0; k < lrs.Len(); k++ {
					lr := lrs.At(k)
					fpVal, exists := lr.Attributes().Get("log.file.path")
					require.True(t, exists, "log.file.path attribute must be populated by include_file_path: true")
					ingestedByPath[fpVal.Str()] = lr.Body().AsString()
				}
			}
		}
	}

	assert.Equal(t, oracleAudLine, ingestedByPath[oracleAudFile], "Oracle .aud log record must be tailed with exact log.file.path")
	assert.Equal(t, cotsLogLine, ingestedByPath[cotsLogFile], "COTS /var/adm/messages.log record must be tailed with exact log.file.path")
	assert.NotContains(t, ingestedByPath, privateKeyFile, "server.key must be excluded by Guardrail 3")
	assert.NotContains(t, ingestedByPath, compressedArchiveFile, "ora_old.aud.gz must be excluded by Guardrail 3")
	assert.NotContains(t, ingestedByPath, unreadableDACFile, "0000-mode root_only.aud must be skipped by OS DAC (Guardrail 4)")
	t.Logf("[Proposed Synthesis + Live Export] Successfully tailed & exported %d records to telemetry.googleapis.com:443 (dashpole-dev): paths=%v",
		teeConsumer.exportedCnt.Load(), []string{oracleAudFile, cotsLogFile})
}

func TestE2E_SymlinkEscapeAttack_BaselineLexicalVsProposedPostSymlinkResolution(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	adumpDir := filepath.Join(tmpDir, "u01", "app", "oracle", "admin", "ORCL", "adump")
	require.NoError(t, os.MkdirAll(adumpDir, 0755))

	// Create a real symlink inside the Oracle adump directory pointing to /etc/passwd:
	shadowSymlink := filepath.Join(adumpDir, "shadow.log")
	require.NoError(t, os.Symlink("/etc/passwd", shadowSymlink))

	// =========================================================================
	// STEP 1: Baseline (Lexical-Only Validation, DisableSymlinkResolution: true)
	// Prove non-tautologically that without post-symlink resolution, the policy
	// passes lexical validation and filelogreceiver actually ingests /etc/passwd!
	// =========================================================================
	baselinePolicy := &filelog.FileLogSourcePolicy{
		ID:                       "baseline_lexical_only",
		Include:                  []string{shadowSymlink},
		DisableSymlinkResolution: true,
	}
	baselineConf, err := baselinePolicy.Evaluate(ctx)
	require.NoError(t, err, "Lexical-only baseline validation allows symlink .../adump/shadow.log -> /etc/passwd")

	flFactory := filelogreceiver.NewFactory()
	baselineFlMap, ok := baselineConf.Get("receivers::filelog").(map[string]any)
	require.True(t, ok)
	baselineFlMap["start_at"] = "beginning"
	baselineFlMap["poll_interval"] = "50ms"

	baselineFlCfg := flFactory.CreateDefaultConfig()
	require.NoError(t, confmap.NewFromStringMap(baselineFlMap).Unmarshal(baselineFlCfg))

	leakSink := new(consumertest.LogsSink)
	leakReceiver, err := flFactory.CreateLogs(ctx, receivertest.NewNopSettings(flFactory.Type()), baselineFlCfg, leakSink)
	require.NoError(t, err)
	require.NoError(t, leakReceiver.Start(ctx, componenttest.NewNopHost()))
	require.Eventually(t, func() bool {
		return leakSink.LogRecordCount() > 0
	}, 5*time.Second, 50*time.Millisecond)
	require.NoError(t, leakReceiver.Shutdown(ctx))

	foundRootPasswdLine := false
	for _, ld := range leakSink.AllLogs() {
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			sl := ld.ResourceLogs().At(i).ScopeLogs()
			for j := 0; j < sl.Len(); j++ {
				lrs := sl.At(j).LogRecords()
				for k := 0; k < lrs.Len(); k++ {
					if strings.HasPrefix(lrs.At(k).Body().AsString(), "root:") {
						foundRootPasswdLine = true
					}
				}
			}
		}
	}
	require.True(t, foundRootPasswdLine, "Without symlink resolution, filelogreceiver reads /etc/passwd ('root:...') via shadow.log symlink")
	t.Logf("[Baseline (Symlink Resolution Disabled)] filelogreceiver leaked %d lines from /etc/passwd via %s!", leakSink.LogRecordCount(), shadowSymlink)

	// =========================================================================
	// STEP 2: Proposed (Guardrail 2 Post-Symlink Resolution Enabled)
	// Prove that both direct path (.../adump/shadow.log) and wildcard glob
	// (.../adump/*.log) resolve the symlink to /etc/passwd via filepath.EvalSymlinks
	// and reject the policy with ErrSensitivePathDenied before filelogreceiver starts!
	// =========================================================================
	proposedDirectPolicy := &filelog.FileLogSourcePolicy{
		ID:      "proposed_direct_symlink_check",
		Include: []string{shadowSymlink},
	}
	_, err = proposedDirectPolicy.Evaluate(ctx)
	require.ErrorIs(t, err, filelog.ErrSensitivePathDenied, "Guardrail 2 post-symlink resolution must block direct symlink to /etc/passwd")
	assert.Contains(t, err.Error(), "/etc/**")
	t.Logf("[Proposed (Post-Symlink Resolution Enabled - Direct)] Blocked policy with error: %v", err)

	proposedWildcardPolicy := &filelog.FileLogSourcePolicy{
		ID:      "proposed_wildcard_symlink_check",
		Include: []string{filepath.Join(adumpDir, "*.log")},
	}
	_, err = proposedWildcardPolicy.Evaluate(ctx)
	require.ErrorIs(t, err, filelog.ErrSensitivePathDenied, "Guardrail 2 post-symlink resolution must block wildcard glob containing symlink to /etc/passwd")
	assert.Contains(t, err.Error(), "/etc/**")
	t.Logf("[Proposed (Post-Symlink Resolution Enabled - Wildcard Glob)] Blocked policy with error: %v", err)
}
