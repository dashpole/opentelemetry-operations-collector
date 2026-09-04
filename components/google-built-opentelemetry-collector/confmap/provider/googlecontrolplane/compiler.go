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

package googlecontrolplane

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/confmap/provider/googlecontrolplane/driver"
	v3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

// ConfigCompiler synthesizes validated policies into a runnable collector configuration (confmap.Conf).
type ConfigCompiler struct {
	registry    *driver.PolicyDriverRegistry
	logger      *zap.Logger
	fleetID     string
	collectorID     string
	baseConfig      map[string]any
	fileReader      func(string) ([]byte, error)
	transport       string
	projectID       string
	endpoint        string
	path            string
	insecure        bool
	serverAuthority string
	caCertPath      string
	clientCertPath  string
	clientKeyPath   string
}

// CompilerOption configures a ConfigCompiler.
type CompilerOption func(*ConfigCompiler)

// WithCompilerTransport sets the policy transport scheme (e.g. "xds" or "file").
func WithCompilerTransport(transport string) CompilerOption {
	return func(c *ConfigCompiler) {
		c.transport = transport
	}
}

// WithCompilerEndpoint sets the endpoint for xDS transport.
func WithCompilerEndpoint(endpoint string) CompilerOption {
	return func(c *ConfigCompiler) {
		c.endpoint = endpoint
	}
}

// WithCompilerPath sets the file path for file transport.
func WithCompilerPath(path string) CompilerOption {
	return func(c *ConfigCompiler) {
		c.path = path
	}
}

// WithCompilerProjectID sets the GCP project ID.
func WithCompilerProjectID(projectID string) CompilerOption {
	return func(c *ConfigCompiler) {
		c.projectID = projectID
	}
}

// WithCompilerInsecure sets whether the xDS connection is insecure.
func WithCompilerInsecure(insecure bool) CompilerOption {
	return func(c *ConfigCompiler) {
		c.insecure = insecure
	}
}

// WithCompilerServerAuthority sets the server authority header for xDS.
func WithCompilerServerAuthority(serverAuthority string) CompilerOption {
	return func(c *ConfigCompiler) {
		c.serverAuthority = serverAuthority
	}
}

// WithCompilerCACertPath sets the CA certificate path for xDS.
func WithCompilerCACertPath(caCertPath string) CompilerOption {
	return func(c *ConfigCompiler) {
		c.caCertPath = caCertPath
	}
}

// WithCompilerClientCertPath sets the client certificate path for xDS.
func WithCompilerClientCertPath(clientCertPath string) CompilerOption {
	return func(c *ConfigCompiler) {
		c.clientCertPath = clientCertPath
	}
}

// WithCompilerClientKeyPath sets the client private key path for xDS.
func WithCompilerClientKeyPath(clientKeyPath string) CompilerOption {
	return func(c *ConfigCompiler) {
		c.clientKeyPath = clientKeyPath
	}
}

// WithCompilerFleetID sets the fleet ID in the compiler context.
func WithCompilerFleetID(fleetID string) CompilerOption {
	return func(c *ConfigCompiler) {
		c.fleetID = fleetID
	}
}

// WithCompilerCollectorID sets the collector instance ID in the compiler context.
func WithCompilerCollectorID(collectorID string) CompilerOption {
	return func(c *ConfigCompiler) {
		c.collectorID = collectorID
	}
}

// WithCompilerBaseConfig sets a base configuration to merge against.
func WithCompilerBaseConfig(baseConfig map[string]any) CompilerOption {
	return func(c *ConfigCompiler) {
		c.baseConfig = baseConfig
	}
}

// WithCompilerFileReader sets a custom file reader for loading base configuration.
func WithCompilerFileReader(reader func(string) ([]byte, error)) CompilerOption {
	return func(c *ConfigCompiler) {
		if reader != nil {
			c.fileReader = reader
		}
	}
}

// NewConfigCompiler creates a new ConfigCompiler.
func NewConfigCompiler(registry *driver.PolicyDriverRegistry, logger *zap.Logger, opts ...CompilerOption) *ConfigCompiler {
	if logger == nil {
		logger = zap.NewNop()
	}
	c := &ConfigCompiler{
		registry:   registry,
		logger:     logger,
		fileReader: os.ReadFile,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// SetBaseConfig updates the base configuration for merging.
func (c *ConfigCompiler) SetBaseConfig(base map[string]any) {
	c.baseConfig = base
}

// Compile executes 5-stage configuration synthesis for a validated set of policies:
// Stage 1: Universal base config (service.telemetry & processors::policy/global)
// Stage 2: Active destination synthesis (exporters, auth extensions, batch processors)
// Stage 3: Transformation rules aggregation into policy/global
// Stage 4: Source policy pipeline wiring (receiver -> post_processors -> policy/global -> batch -> exporter)
// Stage 5: Base config merge (injecting policy/global before batching, or fallback before exporters)
func (c *ConfigCompiler) Compile(policies []*v3.TypedExtensionConfig) (map[string]any, map[string]string, error) {
	resolvedTokens := make(map[string]string)
	conf := make(map[string]any)

	receivers := make(map[string]any)
	processors := make(map[string]any)
	exporters := make(map[string]any)
	extensions := make(map[string]any)
	serviceExts := make([]string, 0)

	// Stage 1: Universal Base Configuration
	// Register policy/global token and initialize empty policy array
	resolvedTokens["global_policy_processor"] = "policy/global"
	globalPolicyRules := make([]any, 0)

	var informerExtName string
	if c.transport == "file" {
		informerExtName = "filepolicy"
		fileCfg := map[string]any{
			"path": c.path,
		}
		extensions[informerExtName] = fileCfg
		serviceExts = appendSliceUnique(serviceExts, informerExtName)
	} else {
		informerExtName = "googlexdspolicy"
		endpoint := c.endpoint
		if endpoint == "" {
			endpoint = "telemetrydirector.googleapis.com:443"
		}
		xdsCfg := map[string]any{
			"endpoint": endpoint,
		}
		if c.fleetID != "" {
			xdsCfg["fleet_id"] = c.fleetID
		}
		if c.projectID != "" {
			xdsCfg["project_id"] = c.projectID
		}
		if c.collectorID != "" {
			xdsCfg["collector_id"] = c.collectorID
		}
		if c.serverAuthority != "" {
			xdsCfg["server_authority"] = c.serverAuthority
		}
		if c.caCertPath != "" {
			xdsCfg["ca_cert_path"] = c.caCertPath
		}
		if c.clientCertPath != "" {
			xdsCfg["client_cert_path"] = c.clientCertPath
		}
		if c.clientKeyPath != "" {
			xdsCfg["client_key_path"] = c.clientKeyPath
		}
		if c.insecure {
			xdsCfg["insecure"] = true
		}
		extensions[informerExtName] = xdsCfg
		serviceExts = appendSliceUnique(serviceExts, informerExtName)
	}

	// Telemetry resource attributes (declarative format for otelconftelemetry)
	telemetryAttrs := make([]any, 0)
	if c.collectorID != "" {
		telemetryAttrs = append(telemetryAttrs, map[string]any{
			"name":  "service.instance.id",
			"value": c.collectorID,
		})
	}
	if c.fleetID != "" {
		telemetryAttrs = append(telemetryAttrs, map[string]any{
			"name":  "gcp.fleet_id",
			"value": c.fleetID,
		})
	}

	serviceTelemetry := map[string]any{}
	if len(telemetryAttrs) > 0 {
		serviceTelemetry["resource"] = map[string]any{
			"attributes": telemetryAttrs,
		}
	}

	service := map[string]any{
		"telemetry": serviceTelemetry,
		"pipelines": make(map[string]any),
	}

	// Partition policies by class
	var destPolicy *v3.TypedExtensionConfig
	sourcePolicies := make([]*v3.TypedExtensionConfig, 0)
	transformPolicies := make([]*v3.TypedExtensionConfig, 0)

	for _, p := range policies {
		if p.TypedConfig == nil {
			continue
		}
		drv := c.registry.GetDriver(p.TypedConfig.TypeUrl)
		if drv == nil {
			continue
		}
		switch drv.Class() {
		case driver.PolicyClassDestination:
			if destPolicy == nil {
				destPolicy = p
			}
		case driver.PolicyClassSource:
			sourcePolicies = append(sourcePolicies, p)
		case driver.PolicyClassTransformation:
			transformPolicies = append(transformPolicies, p)
		}
	}

	// Stage 2: Active Destination Synthesis
	var activeDestExporter string
	signalBatchProcessors := map[string]string{
		"metrics": "batch/gcp_destination_metrics",
		"logs":    "batch/gcp_destination_logs",
		"traces":  "batch/gcp_destination_traces",
	}

	var destDriver driver.PolicyDriver
	if destPolicy != nil {
		destDriver = c.registry.GetDriver(destPolicy.TypedConfig.TypeUrl)
	} else if c.baseConfig == nil && len(policies) == 0 {
		destDriver = c.registry.GetDriver(driver.TypeURLGcpDestination)
	}

	if destDriver != nil {
		destCtx := &driver.CompilationContext{
			PolicyID:       destPolicy.GetName(),
			FleetID:        c.fleetID,
			CollectorID:    c.collectorID,
			ResolvedTokens: resolvedTokens,
		}
		frag, err := destDriver.GenerateConfig(destPolicy.GetTypedConfig(), destCtx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to generate destination config: %w", err)
		}
		mergeMap(exporters, frag.Exporters)
		mergeMap(extensions, frag.Extensions)
		mergeMap(processors, frag.Processors)
		serviceExts = appendSliceUnique(serviceExts, frag.ServiceExts...)

		activeDestExporter = "otlp/gcp_destination"
		if tokenExp, ok := resolvedTokens["active_destination_exporter"]; ok && tokenExp != "" {
			activeDestExporter = tokenExp
		}
		if tokenMetricBatch, ok := resolvedTokens["active_destination_metric_preprocess"]; ok && tokenMetricBatch != "" {
			signalBatchProcessors["metrics"] = tokenMetricBatch
		}
	}

	// Stage 3: Transformation Rules Aggregation
	for _, tp := range transformPolicies {
		drv := c.registry.GetDriver(tp.TypedConfig.TypeUrl)
		if drv == nil {
			continue
		}
		tCtx := &driver.CompilationContext{
			PolicyID:       tp.GetName(),
			FleetID:        c.fleetID,
			CollectorID:    c.collectorID,
			ResolvedTokens: resolvedTokens,
		}
		frag, err := drv.GenerateConfig(tp.GetTypedConfig(), tCtx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to generate transform config for %s: %w", tp.GetName(), err)
		}
		if gpp, ok := frag.Processors["policy/global"].(map[string]any); ok {
			if pols, ok := gpp["policies"].([]any); ok {
				globalPolicyRules = append(globalPolicyRules, pols...)
			}
		}
	}

	policyGlobalMap := map[string]any{
		"informer_extensions": []any{informerExtName},
	}
	if len(globalPolicyRules) > 0 {
		policyGlobalMap["policies"] = globalPolicyRules
	}
	processors["policy/global"] = policyGlobalMap

	// Stage 4: Source Policy Pipeline Wiring
	signalReceivers := make(map[string][]string)

	if len(sourcePolicies) == 0 {
		if c.baseConfig == nil && len(policies) == 0 {
			otlpDrv := c.registry.GetDriver(driver.TypeURLOtlpSource)
			if otlpDrv != nil {
				frag, err := otlpDrv.GenerateConfig(nil, &driver.CompilationContext{
					FleetID:        c.fleetID,
					CollectorID:    c.collectorID,
					ResolvedTokens: resolvedTokens,
				})
				if err == nil {
					mergeMap(receivers, frag.Receivers)
					for signal, pipe := range frag.Pipelines {
						for _, r := range pipe.Receivers {
							signalReceivers[signal] = appendSliceUnique(signalReceivers[signal], r)
						}
					}
				}
			}
		}
	} else {
		for _, sp := range sourcePolicies {
			drv := c.registry.GetDriver(sp.TypedConfig.TypeUrl)
			if drv == nil {
				continue
			}
			sCtx := &driver.CompilationContext{
				PolicyID:       sp.GetName(),
				FleetID:        c.fleetID,
				CollectorID:    c.collectorID,
				ResolvedTokens: resolvedTokens,
			}
			frag, err := drv.GenerateConfig(sp.GetTypedConfig(), sCtx)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to generate source config for %s: %w", sp.GetName(), err)
			}
			mergeMap(receivers, frag.Receivers)

			for signal, pipe := range frag.Pipelines {
				for _, r := range pipe.Receivers {
					signalReceivers[signal] = appendSliceUnique(signalReceivers[signal], r)
				}
			}
		}
	}

	// Zero Empty Pipeline Synthesis:
	// ONLY synthesize a pipeline if BOTH source receivers AND an active destination exporter exist for that signal.
	if activeDestExporter != "" {
		for signal, recvs := range signalReceivers {
			if len(recvs) == 0 {
				continue
			}
			batchProc := signalBatchProcessors[signal]
			pipeProcs := []any{"policy/global"}
			if batchProc != "" && processors[batchProc] != nil {
				pipeProcs = append(pipeProcs, batchProc)
			}

			recvAny := make([]any, len(recvs))
			for i, r := range recvs {
				recvAny[i] = r
			}

			pipelineKey := signal + "/googlecontrolplane"
			service["pipelines"].(map[string]any)[pipelineKey] = map[string]any{
				"receivers":  recvAny,
				"processors": pipeProcs,
				"exporters":  []any{activeDestExporter},
			}
		}
	}

	if len(serviceExts) > 0 {
		extAny := make([]any, len(serviceExts))
		for i, e := range serviceExts {
			extAny[i] = e
		}
		service["extensions"] = extAny
	}

	conf["receivers"] = receivers
	conf["processors"] = processors
	conf["exporters"] = exporters
	conf["extensions"] = extensions
	conf["service"] = service

	// Stage 5: Base Config Merge
	if c.baseConfig != nil {
		conf = c.mergeWithBaseConfig(c.baseConfig, conf)
	}

	return conf, resolvedTokens, nil
}

// mergeWithBaseConfig merges synthesized configuration with a user-provided base configuration.
// Merge semantics:
//  1. Synthesized pipelines are namespaced under service.pipelines.<signal>/googlecontrolplane,
//     preserving user pipelines untouched without implicit injection.
//  2. service.telemetry attributes in base config are preserved; service.instance.id & gcp.fleet_id are added.
//  3. Components (receivers, processors, exporters, extensions) are deeply merged without duplicate processor entries.
func (c *ConfigCompiler) mergeWithBaseConfig(base map[string]any, synthesized map[string]any) map[string]any {
	result := deepCopyMap(base)

	// Merge top-level components
	for _, section := range []string{"receivers", "processors", "exporters", "extensions"} {
		resSec, _ := result[section].(map[string]any)
		if resSec == nil {
			resSec = make(map[string]any)
			result[section] = resSec
		}
		synSec, _ := synthesized[section].(map[string]any)
		if synSec != nil {
			deepMergeMap(resSec, synSec)
		}
	}

	// Always ensure policy/global processor definition is present
	synProcs, _ := synthesized["processors"].(map[string]any)
	if synProcs != nil && synProcs["policy/global"] != nil {
		resProcs, _ := result["processors"].(map[string]any)
		if resProcs == nil {
			resProcs = make(map[string]any)
			result["processors"] = resProcs
		}
		resProcs["policy/global"] = synProcs["policy/global"]
	}

	// Merge service section
	baseService, _ := result["service"].(map[string]any)
	if baseService == nil {
		baseService = make(map[string]any)
		result["service"] = baseService
	}

	synService, _ := synthesized["service"].(map[string]any)

	// Merge service.extensions
	baseExts := toStringSlice(baseService["extensions"])
	synExts := toStringSlice(synService["extensions"])
	mergedExts := appendSliceUnique(baseExts, synExts...)
	if len(mergedExts) > 0 {
		extAny := make([]any, len(mergedExts))
		for i, e := range mergedExts {
			extAny[i] = e
		}
		baseService["extensions"] = extAny
	}

	// Merge service.telemetry attributes
	baseTelemetry, _ := baseService["telemetry"].(map[string]any)
	if baseTelemetry == nil {
		baseTelemetry = make(map[string]any)
		baseService["telemetry"] = baseTelemetry
	}
	baseResource, _ := baseTelemetry["resource"].(map[string]any)
	if baseResource == nil {
		baseResource = make(map[string]any)
		baseTelemetry["resource"] = baseResource
	}

	var existingAttrs []any
	existingNonEmptyKeys := make(map[string]struct{})

	if rawAttrs, ok := baseResource["attributes"]; ok {
		switch typedAttrs := rawAttrs.(type) {
		case []any:
			existingAttrs = typedAttrs
			for _, item := range typedAttrs {
				if m, ok := item.(map[string]any); ok {
					if name, ok := m["name"].(string); ok {
						val := m["value"]
						if valStr, isStr := val.(string); isStr && strings.TrimSpace(valStr) != "" {
							existingNonEmptyKeys[name] = struct{}{}
						} else if val != nil && val != "" {
							existingNonEmptyKeys[name] = struct{}{}
						}
					}
				}
			}
		case map[string]any:
			for k, v := range typedAttrs {
				existingAttrs = append(existingAttrs, map[string]any{
					"name":  k,
					"value": v,
				})
				if valStr, isStr := v.(string); isStr && strings.TrimSpace(valStr) != "" {
					existingNonEmptyKeys[k] = struct{}{}
				} else if v != nil && v != "" {
					existingNonEmptyKeys[k] = struct{}{}
				}
			}
		}
	}

	if c.collectorID != "" {
		if _, exists := existingNonEmptyKeys["service.instance.id"]; !exists {
			existingAttrs = append(existingAttrs, map[string]any{
				"name":  "service.instance.id",
				"value": c.collectorID,
			})
			existingNonEmptyKeys["service.instance.id"] = struct{}{}
		}
	}
	if c.fleetID != "" {
		if _, exists := existingNonEmptyKeys["gcp.fleet_id"]; !exists {
			existingAttrs = append(existingAttrs, map[string]any{
				"name":  "gcp.fleet_id",
				"value": c.fleetID,
			})
			existingNonEmptyKeys["gcp.fleet_id"] = struct{}{}
		}
	}

	if len(existingAttrs) > 0 {
		baseResource["attributes"] = existingAttrs
	}

	// Merge service.pipelines:
	// Base configuration pipelines are preserved without implicit modification.
	// Policies must be explicitly merged into configuration-based pipelines via anchors.
	// Synthesized policy pipelines are unioned alongside base configuration pipelines.
	basePipelines, _ := baseService["pipelines"].(map[string]any)
	if basePipelines == nil {
		basePipelines = make(map[string]any)
		baseService["pipelines"] = basePipelines
	}

	if synService != nil {
		if synPipes, ok := synService["pipelines"].(map[string]any); ok {
			for k, v := range synPipes {
				if _, exists := basePipelines[k]; !exists {
					if pMap, ok := v.(map[string]any); ok {
						basePipelines[k] = deepCopyMap(pMap)
					} else {
						basePipelines[k] = v
					}
				}
			}
		}
	}

	return result
}

// LoadBaseConfig loads a base collector configuration from a URI or file path.
// If uri is empty, a minimal default GCP base configuration is generated.
func (c *ConfigCompiler) LoadBaseConfig(uri string) (map[string]any, error) {
	if strings.TrimSpace(uri) == "" {
		return c.generateDefaultBaseConfig()
	}

	filePath := uri
	if strings.HasPrefix(uri, "file://") {
		u, err := url.Parse(uri)
		if err != nil {
			return nil, fmt.Errorf("invalid base_config URI: %w", err)
		}
		filePath = u.Path
	}

	data, err := c.fileReader(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read base configuration file %s: %w", filePath, err)
	}

	var conf map[string]any
	if err := yaml.Unmarshal(data, &conf); err != nil {
		return nil, fmt.Errorf("failed to parse base configuration YAML: %w", err)
	}

	if conf == nil {
		conf = make(map[string]any)
	}
	return conf, nil
}

func (c *ConfigCompiler) generateDefaultBaseConfig() (map[string]any, error) {
	conf, _, err := c.Compile(nil)
	return conf, err
}

func mergeMap(dest, src map[string]any) {
	deepMergeMap(dest, src)
}

func deepMergeMap(dest, src map[string]any) {
	for k, v := range src {
		existingVal, exists := dest[k]
		if !exists {
			switch typedV := v.(type) {
			case map[string]any:
				dest[k] = deepCopyMap(typedV)
			case []any:
				sliceCopy := make([]any, len(typedV))
				for i, item := range typedV {
					if m, ok := item.(map[string]any); ok {
						sliceCopy[i] = deepCopyMap(m)
					} else {
						sliceCopy[i] = item
					}
				}
				dest[k] = sliceCopy
			default:
				dest[k] = typedV
			}
			continue
		}

		destMap, destIsMap := existingVal.(map[string]any)
		srcMap, srcIsMap := v.(map[string]any)
		if destIsMap && srcIsMap {
			deepMergeMap(destMap, srcMap)
		}
		// If dest already has a value, preserve it.
	}
}

func deepCopyMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	dest := make(map[string]any, len(src))
	for k, v := range src {
		switch typedV := v.(type) {
		case map[string]any:
			dest[k] = deepCopyMap(typedV)
		case []any:
			sliceCopy := make([]any, len(typedV))
			for i, item := range typedV {
				if m, ok := item.(map[string]any); ok {
					sliceCopy[i] = deepCopyMap(m)
				} else {
					sliceCopy[i] = item
				}
			}
			dest[k] = sliceCopy
		default:
			dest[k] = typedV
		}
	}
	return dest
}

func appendSliceUnique(slice []string, items ...string) []string {
	seen := make(map[string]bool, len(slice)+len(items))
	res := make([]string, 0, len(slice)+len(items))
	for _, s := range slice {
		if !seen[s] && s != "" {
			seen[s] = true
			res = append(res, s)
		}
	}
	for _, item := range items {
		if !seen[item] && item != "" {
			seen[item] = true
			res = append(res, item)
		}
	}
	return res
}

func containsString(slice []string, target string) bool {
	for _, s := range slice {
		if s == target {
			return true
		}
	}
	return false
}
