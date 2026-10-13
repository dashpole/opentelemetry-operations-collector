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

package filelog

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/pkg/googlepolicy"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/collector/service"
	"go.opentelemetry.io/collector/service/pipelines"
)

func init() {
	_ = googlepolicy.RegisterPolicyDriver(PolicyType, &googlepolicy.GenericDriver[*FileLogSourcePolicy]{})
}

const (
	// PolicyType is the registered type identifier for FileLogSourcePolicy.
	PolicyType = "file_log"

	// SharedPipelineName is the name of the shared logs pipeline synthesized for filelog policies.
	SharedPipelineName = "filelog"
)

var (
	ErrMissingPolicyName           = errors.New("file_log policy must have a name or id")
	ErrEmptyInclude                = errors.New("file_log policy must specify at least one include glob")
	ErrEmptyGlobPattern            = errors.New("include glob pattern cannot be empty")
	ErrPathTraversal               = errors.New("include glob must not contain '..' or '.' traversal segments")
	ErrRelativeOrInvalidRoot       = errors.New("include glob must be an absolute Linux path (/...) or Windows drive/env path (C:\\..., %ProgramData%\\..., %SystemDrive%\\..., %SystemRoot%\\...)")
	ErrInsufficientLeadingSegments = errors.New("include glob requires >= 2 concrete non-wildcard leading directory segments before any wildcard or filename")
	ErrSensitivePathDenied         = errors.New("include glob targets a sensitive OS, credential, or container directory on the denylist")
	ErrSensitiveExtensionDenied    = errors.New("include glob targets a sensitive key, database datafile, binary, or archive extension on the denylist")
	ErrNotInBaselineAllowlist      = errors.New("include glob is outside the hardcoded 5-root allowlist (/var/log/**, /opt/**/logs/**, /srv/**/logs/**, %ProgramData%\\**\\logs\\**, %SystemDrive%\\inetpub\\logs\\**)")
)

// LinuxSensitiveDirectoryDenylist enumerates the 19 sensitive Linux directory globs from Tab 6 (Guardrail 2).
var LinuxSensitiveDirectoryDenylist = []string{
	"/etc/**",
	"/root/**",
	"/proc/**",
	"/sys/**",
	"/dev/**",
	"/boot/**",
	"/usr/**",
	"/bin/**",
	"/sbin/**",
	"/lib/**",
	"/lib64/**",
	"/var/run/secrets/**",
	"/run/secrets/**",
	"/var/lib/docker/**",
	"/var/lib/containerd/**",
	"/var/lib/kubelet/**",
	"/**/.ssh/**",
	"/**/.aws/**",
	"/**/.config/gcloud/**",
}

// WindowsSensitiveDirectoryDenylist enumerates the 8 sensitive Windows directory globs from Tab 6 (Guardrail 2).
var WindowsSensitiveDirectoryDenylist = []string{
	`%SystemRoot%\System32\config\**`,
	`%SystemRoot%\NTDS\**`,
	`%SystemRoot%\System32\winevt\**`,
	`%SystemRoot%\repair\**`,
	`**\Users\**\.ssh\**`,
	`**\Microsoft\Crypto\**`,
	`**\Microsoft\Protect\**`,
	`**\Microsoft\Credentials\**`,
}

// SensitiveExtensionDenylist enumerates the 23 sensitive key, database, binary, and archive extensions from Tab 6 (Guardrail 3).
var SensitiveExtensionDenylist = []string{
	"*.key",
	"*.pem",
	"*.pfx",
	"*.p12",
	"*.kdb",
	"*.jks",
	"*.keystore",
	"*.env",
	"*.dbf",
	"*.mdf",
	"*.ldf",
	"*.ndf",
	"*.sqlite*",
	"*.db",
	"*.exe",
	"*.dll",
	"*.so",
	"*.gz",
	"*.bz2",
	"*.xz",
	"*.zst",
	"*.zip",
	"*.tar",
}

// DefaultExcludeGlobs are automatically populated on the synthesized filelog receiver
// so broad directory globs (e.g. /u01/app/oracle/admin/*/adump/* or /var/log/**) never
// ingest compressed archives, private keys, or database datafiles.
// Each pattern is prefixed with **/ because stanza/fileconsumer matches full paths via doublestar.PathMatch.
var DefaultExcludeGlobs = []string{
	"**/*.gz",
	"**/*.bz2",
	"**/*.xz",
	"**/*.zst",
	"**/*.zip",
	"**/*.tar",
	"**/*.key",
	"**/*.pem",
	"**/*.pfx",
	"**/*.p12",
	"**/*.kdb",
	"**/*.jks",
	"**/*.keystore",
	"**/.env",
	"**/*.env",
	"**/*.dbf",
	"**/*.mdf",
	"**/*.ldf",
	"**/*.ndf",
	"**/*.sqlite*",
	"**/*.db",
	"**/*.exe",
	"**/*.dll",
	"**/*.so",
	"**/README",
}

// FileLogSourcePolicy implements googlepolicy.SourcePolicy for file_log collection.
type FileLogSourcePolicy struct {
	ID      string   `mapstructure:"id"`
	Name    string   `mapstructure:"name"`
	Include []string `mapstructure:"include"`

	// DisableSymlinkResolution is used only in baseline-vs-change tests to demonstrate
	// what happens when Guardrail 2's post-symlink resolution is disabled.
	DisableSymlinkResolution bool `mapstructure:"-"`
}

var _ googlepolicy.SourcePolicy = (*FileLogSourcePolicy)(nil)

func (p *FileLogSourcePolicy) PolicyName() string {
	if p.Name != "" {
		return p.Name
	}
	return p.ID
}

func (p *FileLogSourcePolicy) PolicyType() string {
	return PolicyType
}

func (p *FileLogSourcePolicy) PolicyClass() googlepolicy.PolicyClass {
	return googlepolicy.PolicyClassSource
}

// Validate validates the FileLogSourcePolicy using the 4-Guardrail Uniform Denylist & Structural Glob Validator.
func (p *FileLogSourcePolicy) Validate() error {
	if p.PolicyName() == "" {
		return ErrMissingPolicyName
	}
	return ValidateUniformDenylist(p.Include, !p.DisableSymlinkResolution)
}

// MergePolicies merges multiple valid FileLogSourcePolicy instances via a deterministic
// sorted set-union of their Include globs into a single synthesized FileLogSourcePolicy.
func MergePolicies(policies []*FileLogSourcePolicy) *FileLogSourcePolicy {
	seen := make(map[string]struct{})
	var mergedIncludes []string
	var names []string
	for _, pol := range policies {
		if pol == nil {
			continue
		}
		if name := pol.PolicyName(); name != "" {
			names = append(names, name)
		}
		for _, inc := range pol.Include {
			if _, ok := seen[inc]; !ok {
				seen[inc] = struct{}{}
				mergedIncludes = append(mergedIncludes, inc)
			}
		}
	}
	slices.Sort(mergedIncludes)
	mergedName := SharedPipelineName
	if len(names) == 1 {
		mergedName = names[0]
	}
	return &FileLogSourcePolicy{
		Name:    mergedName,
		Include: mergedIncludes,
	}
}

// Evaluate synthesizes the shared filelog receiver configuration with include_file_path: true
// and Guardrail 3's default exclude globs.
func (p *FileLogSourcePolicy) Evaluate(_ context.Context) (*confmap.Conf, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}

	seen := make(map[string]struct{}, len(p.Include))
	dedupedIncludes := make([]string, 0, len(p.Include))
	for _, inc := range p.Include {
		if _, ok := seen[inc]; !ok {
			seen[inc] = struct{}{}
			dedupedIncludes = append(dedupedIncludes, inc)
		}
	}

	filelogType, _ := component.NewType("filelog")
	filelogID := component.NewID(filelogType)

	receiverCfg := map[string]any{
		"include":           dedupedIncludes,
		"exclude":           slices.Clone(DefaultExcludeGlobs),
		"include_file_path": true,
	}

	conf := &otelcol.Config{
		Receivers: map[component.ID]component.Config{
			filelogID: component.Config(receiverCfg),
		},
	}

	cm := confmap.New()
	if err := cm.Marshal(conf); err != nil {
		return nil, fmt.Errorf("policy implementation failure for %s: marshaling config got error '%w'", p.PolicyName(), err)
	}
	return cm, nil
}

// LogsPipelines synthesizes the logs/filelog pipeline wiring the shared filelog receiver
// through preExportProcessors (including googlepolicy) to the destination exporters.
func (p *FileLogSourcePolicy) LogsPipelines(preExportProcessors []component.ID, exporters []component.ID, _ []component.ID) (*confmap.Conf, error) {
	filelogType, _ := component.NewType("filelog")
	filelogID := component.NewID(filelogType)

	pipeID := pipeline.NewIDWithName(pipeline.SignalLogs, SharedPipelineName)
	conf := &otelcol.Config{
		Service: service.Config{
			Pipelines: pipelines.Config{
				pipeID: &pipelines.PipelineConfig{
					Receivers:  []component.ID{filelogID},
					Processors: preExportProcessors,
					Exporters:  exporters,
				},
			},
		},
	}
	cm := confmap.New()
	if err := cm.Marshal(conf); err != nil {
		return nil, fmt.Errorf("policy implementation failure for %s: marshaling logs pipeline got error '%w'", p.PolicyName(), err)
	}
	return cm, nil
}

func (p *FileLogSourcePolicy) MetricsPipelines(_ []component.ID, _ []component.ID, _ []component.ID) (*confmap.Conf, error) {
	return nil, nil
}

func (p *FileLogSourcePolicy) TracesPipelines(_ []component.ID, _ []component.ID, _ []component.ID) (*confmap.Conf, error) {
	return nil, nil
}

var windowsDrivePrefixRe = regexp.MustCompile(`^[A-Za-z]:[\\/]`)

func isWindowsEnvRoot(seg string) bool {
	switch strings.ToLower(seg) {
	case "%programdata%", "%systemdrive%", "%systemroot%":
		return true
	default:
		return false
	}
}

func isWindowsDriveSegment(seg string) bool {
	return len(seg) == 2 && ((seg[0] >= 'A' && seg[0] <= 'Z') || (seg[0] >= 'a' && seg[0] <= 'z')) && seg[1] == ':'
}

func classifyOSPath(pattern string) (isWindows bool, isLinux bool) {
	if strings.HasPrefix(pattern, "/") && !strings.Contains(pattern, `\`) {
		return false, true
	}
	if windowsDrivePrefixRe.MatchString(pattern) {
		return true, false
	}
	normalized := strings.ReplaceAll(pattern, "/", `\`)
	firstSeg, _, found := strings.Cut(normalized, `\`)
	if found && isWindowsEnvRoot(firstSeg) {
		return true, false
	}
	return false, false
}

func hasWildcard(seg string) bool {
	return strings.ContainsAny(seg, "*?[]")
}

// ValidateBaselineAllowlist implements the original 5-root hardcoded allowlist from go/tango-preview-policies:
//   - Linux:   /var/log/**, /opt/**/logs/**, /srv/**/logs/**
//   - Windows: %ProgramData%\**\logs\**, %SystemDrive%\inetpub\logs\**
//
// It is used as the baseline comparator in our non-tautological Baseline-vs-Change tests.
func ValidateBaselineAllowlist(include []string) error {
	if len(include) == 0 {
		return ErrEmptyInclude
	}
	for _, pattern := range include {
		if strings.TrimSpace(pattern) == "" {
			return ErrEmptyGlobPattern
		}
		isWin, isLin := classifyOSPath(pattern)
		if !isWin && !isLin {
			return fmt.Errorf("%w: %q", ErrRelativeOrInvalidRoot, pattern)
		}
		if err := checkNoTraversal(pattern, isWin); err != nil {
			return err
		}
		if !matchesBaselineAllowlistLexical(pattern, isWin) {
			return fmt.Errorf("%w: %q", ErrNotInBaselineAllowlist, pattern)
		}
		if isLin {
			resolvedPaths, err := resolveExistingSymlinkTargets(pattern)
			if err != nil {
				return err
			}
			for _, rp := range resolvedPaths {
				if !matchesBaselineAllowlistLexical(rp, false) {
					return fmt.Errorf("%w (resolved symlink %q -> %q)", ErrNotInBaselineAllowlist, pattern, rp)
				}
			}
		}
	}
	return nil
}

func matchesBaselineAllowlistLexical(pattern string, isWin bool) bool {
	if !isWin {
		// Linux allowlist: /var/log/**, /opt/**/logs/**, /srv/**/logs/**
		if strings.HasPrefix(pattern, "/var/log/") && len(pattern) > len("/var/log/") {
			return true
		}
		if strings.HasPrefix(pattern, "/opt/") || strings.HasPrefix(pattern, "/srv/") {
			segs := strings.Split(strings.TrimPrefix(pattern, "/"), "/")
			// Must have at least root ("opt" or "srv"), a "logs" directory segment, and a leaf/glob after "logs".
			for i := 1; i < len(segs)-1; i++ {
				if segs[i] == "logs" {
					return true
				}
			}
		}
		return false
	}

	// Windows allowlist: %ProgramData%\**\logs\**, %SystemDrive%\inetpub\logs\**
	norm := strings.ToLower(strings.ReplaceAll(pattern, "/", `\`))
	if strings.HasPrefix(norm, `%systemdrive%\inetpub\logs\`) && len(norm) > len(`%systemdrive%\inetpub\logs\`) {
		return true
	}
	if strings.HasPrefix(norm, `c:\inetpub\logs\`) && len(norm) > len(`c:\inetpub\logs\`) {
		return true
	}
	if strings.HasPrefix(norm, `%programdata%\`) || strings.HasPrefix(norm, `c:\programdata\`) {
		segs := strings.Split(norm, `\`)
		for i := 1; i < len(segs)-1; i++ {
			if segs[i] == "logs" {
				return true
			}
		}
	}
	return false
}

// ValidateUniformDenylist enforces the 4 Guardrails from Tab 6:
//  1. Structural Glob Breadth Check (absolute path, no '..' traversal, >= 2 concrete leading segments before any wildcard)
//  2. Sensitive Directory Denylist (19 Linux patterns, 8 Windows patterns with case-folding & %SystemRoot% canonicalization, checked pre- and post-symlink resolution)
//  3. Sensitive Key, Database & Binary Extension Denylist (23 extension patterns)
//  4. OS Least Privilege & Deterministic Fleet-Uniform Validation
func ValidateUniformDenylist(include []string, checkSymlinks bool) error {
	if len(include) == 0 {
		return ErrEmptyInclude
	}
	for _, pattern := range include {
		if err := validateSinglePattern(pattern, checkSymlinks); err != nil {
			return err
		}
	}
	return nil
}

func validateSinglePattern(pattern string, checkSymlinks bool) error {
	if strings.TrimSpace(pattern) == "" {
		return ErrEmptyGlobPattern
	}

	isWin, isLin := classifyOSPath(pattern)
	if !isWin && !isLin {
		return fmt.Errorf("%w: %q", ErrRelativeOrInvalidRoot, pattern)
	}

	// Guardrail 1a: Reject '..' and '.' traversal segments before any path cleaning.
	if err := checkNoTraversal(pattern, isWin); err != nil {
		return err
	}

	// Guardrail 2 (Pre-Symlink Lexical Check): Sensitive Directory Denylist.
	if err := checkSensitiveDirectoryDenylist(pattern, isWin); err != nil {
		return err
	}

	// Guardrail 3 (Pre-Symlink Lexical Check): Sensitive Key, Database & Binary Extension Denylist.
	if err := checkSensitiveExtensionDenylist(pattern, isWin); err != nil {
		return err
	}

	// Guardrail 1b: Require >= 2 concrete non-wildcard leading segments and at least 1 leaf/glob segment (>= 3 total segments).
	if err := checkLeadingSegments(pattern, isWin); err != nil {
		return err
	}

	// Guardrail 2 & 3 (Post-Symlink Resolution Check on existing filesystem paths/globs).
	if checkSymlinks && isLin {
		resolvedPaths, err := resolveExistingSymlinkTargets(pattern)
		if err != nil {
			return err
		}
		for _, rp := range resolvedPaths {
			if err := checkSensitiveDirectoryDenylist(rp, false); err != nil {
				return fmt.Errorf("%w (via symlink %q -> %q)", err, pattern, rp)
			}
			if err := checkSensitiveExtensionDenylist(rp, false); err != nil {
				return fmt.Errorf("%w (via symlink %q -> %q)", err, pattern, rp)
			}
		}
	}

	return nil
}

func checkNoTraversal(pattern string, isWin bool) error {
	var segs []string
	if isWin {
		norm := strings.ReplaceAll(pattern, "/", `\`)
		segs = strings.Split(norm, `\`)
	} else {
		segs = strings.Split(strings.TrimPrefix(pattern, "/"), "/")
	}
	for _, seg := range segs {
		if seg == "" || seg == "." || seg == ".." {
			return fmt.Errorf("%w: %q", ErrPathTraversal, pattern)
		}
	}
	return nil
}

// checkLeadingSegments enforces Guardrail 1's structural breadth rule:
//   - On Linux: strip leading '/' and split on '/'. Require >= 3 total segments
//     ("/{seg1}/{seg2}/{leaf_or_glob}") where the first 2 directory segments ("seg1", "seg2")
//     contain zero wildcard characters ('*', '?', '[', ']').
//     This rejects "/*", "/**", "/u01/**", "/u01/*.log", "/data/**", while allowing
//     "/u01/app/**", "/var/adm/**", "/var/adm/messages", "/u01/app/oracle/admin/*/adump/*.aud".
//   - On Windows: normalize '/' to '\' and split on '\'. Segment 0 is the drive/env root
//     ("C:", "D:", "%ProgramData%", "%SystemDrive%", "%SystemRoot%"), Segment 1 must be a
//     concrete non-wildcard top-level directory ("AppLogs", "System32", "inetpub"), and there
//     must be >= 3 total segments ("{DriveOrEnvRoot}\{dir1}\{leaf_or_glob}").
//     This rejects "C:\*", "C:\**", "D:\**", "D:\**\*.log", "%ProgramData%\**\logs\**",
//     while allowing "D:\AppLogs\**", "D:\AppLogs\sep\agi.log", "%SystemRoot%\System32\LogFiles\**".
func checkLeadingSegments(pattern string, isWin bool) error {
	var segs []string
	if isWin {
		norm := strings.ReplaceAll(pattern, "/", `\`)
		segs = strings.Split(norm, `\`)
		if len(segs) < 3 {
			return fmt.Errorf("%w: %q (got %d segments, need drive/env root + concrete directory + file/glob)", ErrInsufficientLeadingSegments, pattern, len(segs))
		}
		if (!isWindowsDriveSegment(segs[0]) && !isWindowsEnvRoot(segs[0])) || hasWildcard(segs[0]) || hasWildcard(segs[1]) {
			return fmt.Errorf("%w: %q (first 2 segments %q\\%q must be concrete non-wildcard segments)", ErrInsufficientLeadingSegments, pattern, segs[0], segs[1])
		}
		return nil
	}

	segs = strings.Split(strings.TrimPrefix(pattern, "/"), "/")
	if len(segs) < 3 {
		return fmt.Errorf("%w: %q (got %d leading segments, need >= 2 concrete directory segments + file/glob)", ErrInsufficientLeadingSegments, pattern, len(segs)-1)
	}
	if hasWildcard(segs[0]) || hasWildcard(segs[1]) {
		return fmt.Errorf("%w: %q (first 2 directory segments /%s/%s must be concrete non-wildcard segments)", ErrInsufficientLeadingSegments, pattern, segs[0], segs[1])
	}
	return nil
}

func checkSensitiveDirectoryDenylist(pattern string, isWin bool) error {
	if !isWin {
		return checkLinuxSensitiveDirectoryDenylist(pattern)
	}
	return checkWindowsSensitiveDirectoryDenylist(pattern)
}

func checkLinuxSensitiveDirectoryDenylist(pattern string) error {
	segs := strings.Split(strings.TrimPrefix(pattern, "/"), "/")

	// 1. Check root-anchored sensitive directories:
	// /etc/**, /root/**, /proc/**, /sys/**, /dev/**, /boot/**, /usr/**, /bin/**, /sbin/**, /lib/**, /lib64/**
	rootDenied := map[string]string{
		"etc":   "/etc/**",
		"root":  "/root/**",
		"proc":  "/proc/**",
		"sys":   "/sys/**",
		"dev":   "/dev/**",
		"boot":  "/boot/**",
		"usr":   "/usr/**",
		"bin":   "/bin/**",
		"sbin":  "/sbin/**",
		"lib":   "/lib/**",
		"lib64": "/lib64/**",
	}
	if len(segs) >= 1 {
		if rule, bad := rootDenied[segs[0]]; bad {
			return fmt.Errorf("%w: %q matches %s", ErrSensitivePathDenied, pattern, rule)
		}
	}

	// 2. Check multi-segment anchored sensitive directories:
	// /run/secrets/**, /var/run/secrets/**, /var/lib/docker/**, /var/lib/containerd/**, /var/lib/kubelet/**
	// Also reject broad parent globs like /var/run/** or /var/lib/** whose wildcard at segment 2 directly
	// subsumes secrets/docker/containerd/kubelet.
	if len(segs) >= 2 && segs[0] == "run" && (segmentMatchesSensitiveDir(segs[1], "secrets") || hasWildcard(segs[1])) {
		return fmt.Errorf("%w: %q matches /run/secrets/**", ErrSensitivePathDenied, pattern)
	}
	if len(segs) >= 3 && segs[0] == "var" && segs[1] == "run" {
		if segmentMatchesSensitiveDir(segs[2], "secrets") || hasWildcard(segs[2]) {
			return fmt.Errorf("%w: %q matches /var/run/secrets/**", ErrSensitivePathDenied, pattern)
		}
	}
	if len(segs) >= 3 && segs[0] == "var" && segs[1] == "lib" {
		switch {
		case segmentMatchesSensitiveDir(segs[2], "docker"):
			return fmt.Errorf("%w: %q matches /var/lib/docker/**", ErrSensitivePathDenied, pattern)
		case segmentMatchesSensitiveDir(segs[2], "containerd"):
			return fmt.Errorf("%w: %q matches /var/lib/containerd/**", ErrSensitivePathDenied, pattern)
		case segmentMatchesSensitiveDir(segs[2], "kubelet"):
			return fmt.Errorf("%w: %q matches /var/lib/kubelet/**", ErrSensitivePathDenied, pattern)
		case hasWildcard(segs[2]):
			return fmt.Errorf("%w: %q subsumes /var/lib/{docker,containerd,kubelet}/**", ErrSensitivePathDenied, pattern)
		}
	}

	// 3. Check floating sensitive credential directories anywhere in the path:
	// /**/.ssh/**, /**/.aws/**, /**/.config/gcloud/**
	for i, seg := range segs {
		switch {
		case segmentMatchesSensitiveDir(seg, ".ssh"):
			return fmt.Errorf("%w: %q matches /**/.ssh/**", ErrSensitivePathDenied, pattern)
		case segmentMatchesSensitiveDir(seg, ".aws"):
			return fmt.Errorf("%w: %q matches /**/.aws/**", ErrSensitivePathDenied, pattern)
		case segmentMatchesSensitiveDir(seg, ".config"):
			if i+1 < len(segs) && (segmentMatchesSensitiveDir(segs[i+1], "gcloud") || hasWildcard(segs[i+1])) {
				return fmt.Errorf("%w: %q matches /**/.config/gcloud/**", ErrSensitivePathDenied, pattern)
			}
		}
	}

	return nil
}

// segmentMatchesSensitiveDir reports whether seg equals target or uses a partial wildcard
// (such as ".ss*", ".conf*", "micro*") that matches target.
func segmentMatchesSensitiveDir(seg, target string) bool {
	if seg == target {
		return true
	}
	if !hasWildcard(seg) || seg == "*" || seg == "**" {
		return false
	}
	matched, err := path.Match(seg, target)
	return err == nil && matched
}

// canonicalizeWindowsPath normalizes slashes to '\', lowercases the path for case-insensitive
// Windows matching, and canonicalizes '<Drive>:\windows\...' and '%systemdrive%\windows\...'
// to '%systemroot%\...' so Guardrail 2's %SystemRoot% rules cannot be bypassed via C:\Windows.
func canonicalizeWindowsPath(pattern string) []string {
	norm := strings.ToLower(strings.ReplaceAll(pattern, "/", `\`))
	segs := strings.Split(norm, `\`)
	if len(segs) >= 2 && (isWindowsDriveSegment(segs[0]) || segs[0] == "%systemdrive%") && segs[1] == "windows" {
		segs = append([]string{"%systemroot%"}, segs[2:]...)
	}
	return segs
}

func checkWindowsSensitiveDirectoryDenylist(pattern string) error {
	segs := canonicalizeWindowsPath(pattern)

	// 1. Check %SystemRoot% anchored sensitive directories:
	// %SystemRoot%\System32\config\**, %SystemRoot%\NTDS\**, %SystemRoot%\System32\winevt\**, %SystemRoot%\repair\**
	if len(segs) >= 2 && segs[0] == "%systemroot%" {
		switch {
		case segmentMatchesSensitiveDir(segs[1], "ntds"):
			return fmt.Errorf("%w: %q matches %%SystemRoot%%\\NTDS\\**", ErrSensitivePathDenied, pattern)
		case segmentMatchesSensitiveDir(segs[1], "repair"):
			return fmt.Errorf("%w: %q matches %%SystemRoot%%\\repair\\**", ErrSensitivePathDenied, pattern)
		case segmentMatchesSensitiveDir(segs[1], "system32"):
			if len(segs) >= 3 {
				switch {
				case segmentMatchesSensitiveDir(segs[2], "config"):
					return fmt.Errorf("%w: %q matches %%SystemRoot%%\\System32\\config\\**", ErrSensitivePathDenied, pattern)
				case segmentMatchesSensitiveDir(segs[2], "winevt"):
					return fmt.Errorf("%w: %q matches %%SystemRoot%%\\System32\\winevt\\**", ErrSensitivePathDenied, pattern)
				case hasWildcard(segs[2]):
					return fmt.Errorf("%w: %q subsumes %%SystemRoot%%\\System32\\{config,winevt}\\**", ErrSensitivePathDenied, pattern)
				}
			}
		default:
			if len(segs) >= 3 && hasWildcard(segs[1]) {
				return fmt.Errorf("%w: %q subsumes %%SystemRoot%%\\{NTDS,repair,System32}\\**", ErrSensitivePathDenied, pattern)
			}
		}
	}

	// 2. Check floating Windows credential & key directories:
	// **\Users\**\.ssh\**, **\Microsoft\Crypto\**, **\Microsoft\Protect\**, **\Microsoft\Credentials\**
	seenUsers := false
	for i, seg := range segs {
		if segmentMatchesSensitiveDir(seg, "users") {
			seenUsers = true
		}
		if segmentMatchesSensitiveDir(seg, ".ssh") {
			if seenUsers {
				return fmt.Errorf("%w: %q matches **\\Users\\**\\.ssh\\**", ErrSensitivePathDenied, pattern)
			}
			return fmt.Errorf("%w: %q matches **\\.ssh\\**", ErrSensitivePathDenied, pattern)
		}
		if segmentMatchesSensitiveDir(seg, "microsoft") && i+1 < len(segs) {
			switch {
			case segmentMatchesSensitiveDir(segs[i+1], "crypto"):
				return fmt.Errorf("%w: %q matches **\\Microsoft\\Crypto\\**", ErrSensitivePathDenied, pattern)
			case segmentMatchesSensitiveDir(segs[i+1], "protect"):
				return fmt.Errorf("%w: %q matches **\\Microsoft\\Protect\\**", ErrSensitivePathDenied, pattern)
			case segmentMatchesSensitiveDir(segs[i+1], "credentials"):
				return fmt.Errorf("%w: %q matches **\\Microsoft\\Credentials\\**", ErrSensitivePathDenied, pattern)
			case hasWildcard(segs[i+1]):
				return fmt.Errorf("%w: %q subsumes **\\Microsoft\\{Crypto,Protect,Credentials}\\**", ErrSensitivePathDenied, pattern)
			}
		}
	}

	return nil
}

// checkSensitiveExtensionDenylist enforces Guardrail 3 against all 23 sensitive extensions.
func checkSensitiveExtensionDenylist(pattern string, isWin bool) error {
	norm := pattern
	if isWin {
		norm = strings.ReplaceAll(pattern, `\`, "/")
	}
	base := strings.ToLower(path.Base(norm))

	exactSuffixRules := []struct {
		suffix string
		rule   string
	}{
		{".key", "*.key"},
		{".pem", "*.pem"},
		{".pfx", "*.pfx"},
		{".p12", "*.p12"},
		{".kdb", "*.kdb"},
		{".jks", "*.jks"},
		{".keystore", "*.keystore"},
		{".env", "*.env"},
		{".dbf", "*.dbf"},
		{".mdf", "*.mdf"},
		{".ldf", "*.ldf"},
		{".ndf", "*.ndf"},
		{".db", "*.db"},
		{".exe", "*.exe"},
		{".dll", "*.dll"},
		{".so", "*.so"},
		{".gz", "*.gz"},
		{".bz2", "*.bz2"},
		{".xz", "*.xz"},
		{".zst", "*.zst"},
		{".zip", "*.zip"},
		{".tar", "*.tar"},
	}

	ext := path.Ext(base)
	for _, r := range exactSuffixRules {
		if base == r.suffix || strings.HasSuffix(base, r.suffix) {
			return fmt.Errorf("%w: %q matches %s", ErrSensitiveExtensionDenied, pattern, r.rule)
		}
		if ext != "" && ext != ".*" && hasWildcard(ext) {
			if matched, err := path.Match(ext, r.suffix); err == nil && matched {
				return fmt.Errorf("%w: %q wildcard extension %q matches %s", ErrSensitiveExtensionDenied, pattern, ext, r.rule)
			}
		}
	}

	// Check *.sqlite* (e.g., *.sqlite, *.sqlite3, app.sqlite-wal, or partial wildcard *.sqli*)
	if strings.Contains(base, ".sqlite") {
		return fmt.Errorf("%w: %q matches *.sqlite*", ErrSensitiveExtensionDenied, pattern)
	}
	if ext != "" && ext != ".*" && hasWildcard(ext) {
		if matched, err := path.Match(ext, ".sqlite"); err == nil && matched {
			return fmt.Errorf("%w: %q wildcard extension %q matches *.sqlite*", ErrSensitiveExtensionDenied, pattern, ext)
		}
	}

	return nil
}

// resolveExistingSymlinkTargets resolves symlinks for existing paths, ancestor directories,
// filepath.Glob matches, and symlink entries under the longest existing concrete prefix directory.
func resolveExistingSymlinkTargets(pattern string) ([]string, error) {
	seen := make(map[string]struct{})
	var resolved []string
	addResolved := func(p string) {
		if p == "" || p == pattern {
			return
		}
		if _, ok := seen[p]; !ok {
			seen[p] = struct{}{}
			resolved = append(resolved, p)
		}
	}

	// 1. Find the concrete prefix before any wildcard segment.
	segs := strings.Split(strings.TrimPrefix(pattern, "/"), "/")
	concreteCount := 0
	for _, seg := range segs {
		if hasWildcard(seg) {
			break
		}
		concreteCount++
	}
	concretePrefix := "/" + strings.Join(segs[:concreteCount], "/")
	remainingSuffix := ""
	if concreteCount < len(segs) {
		remainingSuffix = strings.Join(segs[concreteCount:], "/")
	}

	// 2. Walk up from concretePrefix to find the longest existing filesystem ancestor,
	// evaluate symlinks on that ancestor, and re-attach the remaining suffix.
	curr := concretePrefix
	var tailParts []string
	for curr != "/" && curr != "." && curr != "" {
		if _, err := os.Lstat(curr); err == nil {
			evalAncestor, evalErr := filepath.EvalSymlinks(curr)
			if evalErr == nil {
				reconstructed := evalAncestor
				for i := len(tailParts) - 1; i >= 0; i-- {
					reconstructed = filepath.Join(reconstructed, tailParts[i])
				}
				if remainingSuffix != "" {
					reconstructed = reconstructed + "/" + remainingSuffix
				}
				addResolved(reconstructed)
			}
			break
		}
		tailParts = append(tailParts, filepath.Base(curr))
		curr = filepath.Dir(curr)
	}

	// 3. Expand filepath.Glob matches if pattern has wildcards (or exists directly).
	if matches, err := filepath.Glob(pattern); err == nil {
		for _, m := range matches {
			if remainingSuffix != "" && isExcludedByDefault(m) {
				continue
			}
			if evalMatch, evalErr := filepath.EvalSymlinks(m); evalErr == nil {
				addResolved(evalMatch)
			}
		}
	}

	// 4. If wildcards are present and concretePrefix exists as a directory on disk,
	// walk concretePrefix via filepath.WalkDir to inspect any symlink entries that match pattern.
	if remainingSuffix != "" {
		if info, err := os.Stat(concretePrefix); err == nil && info.IsDir() {
			_ = filepath.WalkDir(concretePrefix, func(entryPath string, d fs.DirEntry, walkErr error) error {
				if walkErr != nil {
					return nil
				}
				if d.Type()&os.ModeSymlink != 0 {
					evalTarget, evalErr := filepath.EvalSymlinks(entryPath)
					if evalErr != nil {
						return nil
					}
					if targetInfo, statErr := os.Stat(evalTarget); statErr == nil && targetInfo.IsDir() {
						if globCanTraverseDir(pattern, entryPath) {
							addResolved(filepath.Join(evalTarget, path.Base(pattern)))
						}
						return nil
					}
					if isExcludedByDefault(entryPath) {
						return nil
					}
					if globMatchesPath(pattern, entryPath) {
						addResolved(evalTarget)
					}
				}
				return nil
			})
		}
	}

	return resolved, nil
}

// globMatchesPath checks whether a path under concretePrefix matches a glob pattern (supporting both * and **).
func globMatchesPath(pattern, candidate string) bool {
	if matched, err := path.Match(pattern, candidate); err == nil && matched {
		return true
	}
	patSegs := strings.Split(strings.TrimPrefix(pattern, "/"), "/")
	candSegs := strings.Split(strings.TrimPrefix(candidate, "/"), "/")
	return matchSegments(patSegs, candSegs)
}

func matchSegments(patSegs, candSegs []string) bool {
	if len(patSegs) == 0 {
		return len(candSegs) == 0
	}
	if patSegs[0] == "**" {
		if matchSegments(patSegs[1:], candSegs) {
			return true
		}
		return len(candSegs) > 0 && matchSegments(patSegs, candSegs[1:])
	}
	if len(candSegs) == 0 {
		return false
	}
	matched, err := path.Match(patSegs[0], candSegs[0])
	if err != nil || !matched {
		return false
	}
	return matchSegments(patSegs[1:], candSegs[1:])
}

func isExcludedByDefault(candidate string) bool {
	if checkSensitiveExtensionDenylist(candidate, false) != nil {
		return true
	}
	return path.Base(candidate) == "README"
}

// globCanTraverseDir reports whether dirPath can be traversed as a directory prefix by pattern.
func globCanTraverseDir(pattern, dirPath string) bool {
	patSegs := strings.Split(strings.TrimPrefix(pattern, "/"), "/")
	dirSegs := strings.Split(strings.TrimPrefix(dirPath, "/"), "/")
	return dirPrefixMatchesGlob(patSegs, dirSegs)
}

func dirPrefixMatchesGlob(patSegs, dirSegs []string) bool {
	if len(dirSegs) == 0 {
		return len(patSegs) > 0
	}
	if len(patSegs) == 0 {
		return false
	}
	if patSegs[0] == "**" {
		if dirPrefixMatchesGlob(patSegs[1:], dirSegs) {
			return true
		}
		return dirPrefixMatchesGlob(patSegs, dirSegs[1:])
	}
	matched, err := path.Match(patSegs[0], dirSegs[0])
	if err != nil || !matched {
		return false
	}
	return dirPrefixMatchesGlob(patSegs[1:], dirSegs[1:])
}
