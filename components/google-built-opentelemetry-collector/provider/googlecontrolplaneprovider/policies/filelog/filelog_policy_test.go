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
	"os"
	"path/filepath"
	"testing"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/pkg/googlepolicy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
)

func TestBaselineAllowlistVsProposedDenylist(t *testing.T) {
	// 1. The 4 primary Allianz enterprise paths from Tab 6 (#33, #32, #76):
	// Baseline 5-root allowlist MUST reject all 4; Proposed Uniform Denylist MUST accept all 4.
	enterprisePaths := []struct {
		name    string
		finding string
		glob    string
	}{
		{
			name:    "Oracle_OFA_Audit_Trail_Log",
			finding: "#33",
			glob:    "/u01/app/oracle/admin/ORCL/adump/ora_1234.aud",
		},
		{
			name:    "Linux_COTS_Var_Adm_Messages",
			finding: "#32",
			glob:    "/var/adm/messages",
		},
		{
			name:    "Windows_Secondary_Drive_SEP_Log",
			finding: "#32/#76",
			glob:    `D:\AppLogs\sep\agi.log`,
		},
		{
			name:    "Windows_System32_LogFiles_Firewall",
			finding: "#76",
			glob:    `%SystemRoot%\System32\LogFiles\Firewall\pfirewall.log`,
		},
	}

	for _, tc := range enterprisePaths {
		t.Run("Enterprise_"+tc.name, func(t *testing.T) {
			baselineErr := ValidateBaselineAllowlist([]string{tc.glob})
			require.ErrorIs(t, baselineErr, ErrNotInBaselineAllowlist,
				"Baseline 5-root allowlist must reject %s (%s)", tc.glob, tc.finding)

			proposedErr := ValidateUniformDenylist([]string{tc.glob}, true)
			require.NoError(t, proposedErr,
				"Proposed 4-Guardrail Uniform Denylist must accept %s (%s)", tc.glob, tc.finding)
		})
	}

	// 2. Standard 2-segment OS log roots allowed by both Baseline and Proposed:
	sharedValidPaths := []string{
		"/var/log/syslog",
		"/var/log/**",
		"/opt/myapp/logs/app.log",
		"/srv/http/logs/access.log",
		`%SystemDrive%\inetpub\logs\LogFiles\W3SVC1\u_ex.log`,
		`%ProgramData%\Symantec\logs\agi.log`,
	}
	for _, p := range sharedValidPaths {
		t.Run("SharedValid_"+p, func(t *testing.T) {
			require.NoError(t, ValidateBaselineAllowlist([]string{p}))
			require.NoError(t, ValidateUniformDenylist([]string{p}, true))
		})
	}

	// 3. Sensitive OS & traversal paths rejected by both Baseline and Proposed:
	sharedRejectedPaths := []string{
		"/etc/shadow",
		`C:\Windows\System32\config\SAM`,
		"/var/log/../../etc/passwd",
		"relative/path/app.log",
	}
	for _, p := range sharedRejectedPaths {
		t.Run("SharedRejected_"+p, func(t *testing.T) {
			require.Error(t, ValidateBaselineAllowlist([]string{p}))
			require.Error(t, ValidateUniformDenylist([]string{p}, true))
		})
	}
}

func TestGuardrail1_StructuralGlobBreadthAndOnePagerBoundary(t *testing.T) {
	// Tests the 1-segment vs. 2-segment boundary audited in tango_onepager_6_filelog_enterprise_paths.md:
	// - Linux requires >= 2 concrete directory segments (/{seg1}/{seg2}/{leaf_or_glob}) before any wildcard.
	// - Windows requires drive/env root as segment 1 + >= 1 concrete top-level directory as segment 2
	//   ({DriveOrEnvRoot}\{dir1}\{leaf_or_glob}) before any wildcard.
	rejectedInsufficientSegments := []string{
		"/*",
		"/**",
		"/u01/**",
		"/u01/*",
		"/u01/*.log",
		"/data/**",
		"/apps/**",
		"/root_file.log",
		"/u01/single_dir_file.log",
		`C:\*`,
		`C:\**`,
		`D:\**`,
		`D:\**\*.log`,
		`E:\**\*.txt`,
		`%ProgramData%\**\logs\**`,
		`%SystemRoot%\*`,
		`D:\root_file.log`,
	}
	for _, pattern := range rejectedInsufficientSegments {
		t.Run("Reject_1Segment_"+pattern, func(t *testing.T) {
			err := ValidateUniformDenylist([]string{pattern}, false)
			require.ErrorIs(t, err, ErrInsufficientLeadingSegments, "expected %q to fail Guardrail 1 (>= 2 concrete leading segments)", pattern)
		})
	}

	acceptedTwoSegmentEnterprisePaths := []string{
		"/u01/app/**",
		"/u01/app/oracle/admin/*/adump/*.aud",
		"/u02/oradata/audit/*.aud",
		"/var/adm/**",
		"/var/adm/messages",
		"/data/logs/**",
		"/apps/sep/**",
		"/apps/trellix/agent/*.log",
		`D:\AppLogs\**`,
		`D:\AppLogs\**\*.log`,
		`E:\AppLogs\**\*.txt`,
		`%ProgramData%\Vendor\logs\**`,
		`%SystemRoot%\System32\LogFiles\**`,
	}
	for _, pattern := range acceptedTwoSegmentEnterprisePaths {
		t.Run("Accept_2Segment_"+pattern, func(t *testing.T) {
			err := ValidateUniformDenylist([]string{pattern}, false)
			require.NoError(t, err, "expected %q to pass Guardrail 1", pattern)
		})
	}

	traversalAndRelativePatterns := []struct {
		pattern     string
		expectedErr error
	}{
		{"", ErrEmptyGlobPattern},
		{"   ", ErrEmptyGlobPattern},
		{"var/log/syslog", ErrRelativeOrInvalidRoot},
		{"./var/log/syslog", ErrRelativeOrInvalidRoot},
		{"../etc/passwd", ErrRelativeOrInvalidRoot},
		{`%UNKNOWN_ROOT%\AppLogs\app.log`, ErrRelativeOrInvalidRoot},
		{"/var/log/../../etc/passwd", ErrPathTraversal},
		{"/u01/app/./oracle.aud", ErrPathTraversal},
		{"/u01//app/oracle.aud", ErrPathTraversal},
		{`D:\AppLogs\..\Windows\System32\config\SAM`, ErrPathTraversal},
	}
	for _, tc := range traversalAndRelativePatterns {
		t.Run("Reject_TraversalOrRelative_"+tc.pattern, func(t *testing.T) {
			err := ValidateUniformDenylist([]string{tc.pattern}, false)
			require.ErrorIs(t, err, tc.expectedErr)
		})
	}
}

func TestGuardrail2_SensitiveDirectoryDenylist_All19LinuxAnd8WindowsPatterns(t *testing.T) {
	linuxDeniedCases := []string{
		"/etc/passwd/extra",
		"/etc/nginx/access.log",
		"/root/secret/audit.log",
		"/proc/self/cmdline",
		"/sys/kernel/debug/tracing/trace",
		"/dev/shm/secret.log",
		"/boot/grub/grub.cfg",
		"/usr/local/logs/app.log",
		"/bin/sh/something",
		"/sbin/init/something",
		"/lib/systemd/something",
		"/lib64/tls/something",
		"/var/run/secrets/kubernetes.io/serviceaccount/token",
		"/var/run/*/kubernetes.io/token",
		"/run/secrets/db_password",
		"/run/*/db_password",
		"/var/lib/docker/containers/123/123-json.log",
		"/var/lib/containerd/io.containerd.grpc.v1.cri/containers/app.log",
		"/var/lib/kubelet/pods/123/volumes/secret.txt",
		"/var/lib/**/secret.txt",
		"/home/alice/.ssh/id_rsa_pub",
		"/u01/app/oracle/.ssh/authorized_keys",
		"/home/alice/.aws/credentials",
		"/home/alice/.config/gcloud/application_default_credentials.json",
		"/home/alice/.config/*/application_default_credentials.json",
		"/home/alice/.ss*/id_rsa",
		"/home/alice/.conf*/gcloud/adc.json",
		"/var/lib/dock*/containers/app.log",
	}
	for _, p := range linuxDeniedCases {
		t.Run("LinuxDenied_"+p, func(t *testing.T) {
			err := ValidateUniformDenylist([]string{p}, false)
			require.ErrorIs(t, err, ErrSensitivePathDenied, "expected Linux path %q to be rejected by Guardrail 2", p)
		})
	}

	windowsDeniedCases := []string{
		`%SystemRoot%\System32\config\SAM`,
		`C:\Windows\System32\config\SAM`,
		`c:\windows\system32\config\system`,
		`%SystemDrive%\Windows\System32\config\SECURITY`,
		`%SystemRoot%\NTDS\ntds.dit`,
		`C:\Windows\NTDS\ntds.dit`,
		`%SystemRoot%\System32\winevt\Logs\Security.evtx`,
		`C:\Windows\System32\winevt\Logs\System.evtx`,
		`%SystemRoot%\repair\sam`,
		`C:\Windows\repair\system`,
		`%SystemRoot%\System32\**`,
		`C:\Users\Administrator\.ssh\id_ed25519`,
		`D:\Users\svc_oracle\.ssh\authorized_keys`,
		`C:\ProgramData\Microsoft\Crypto\RSA\MachineKeys\key1`,
		`C:\Users\Admin\AppData\Roaming\Microsoft\Protect\S-1-5-21\cred`,
		`C:\Users\Admin\AppData\Local\Microsoft\Credentials\cred1`,
		`C:\ProgramData\Microsoft\*\MachineKeys\key1`,
		`C:\ProgramData\Micro*\Crypto\RSA\key1`,
		`%SystemRoot%\System32\conf*\SAM`,
	}
	for _, p := range windowsDeniedCases {
		t.Run("WindowsDenied_"+p, func(t *testing.T) {
			err := ValidateUniformDenylist([]string{p}, false)
			require.ErrorIs(t, err, ErrSensitivePathDenied, "expected Windows path %q to be rejected by Guardrail 2", p)
		})
	}
}

func TestGuardrail2_PostSymlinkResolutionAndAlt4Proof(t *testing.T) {
	tmpDir := t.TempDir()
	adumpDir := filepath.Join(tmpDir, "u01", "app", "oracle", "admin", "ORCL", "adump")
	require.NoError(t, os.MkdirAll(adumpDir, 0755))

	// 1. Direct file symlink: .../adump/shadow.log -> /etc/passwd
	shadowLink := filepath.Join(adumpDir, "shadow.log")
	require.NoError(t, os.Symlink("/etc/passwd", shadowLink))

	// Lexical check alone (checkSymlinks=false) passes, proving the check is non-tautological:
	require.NoError(t, ValidateUniformDenylist([]string{shadowLink}, false),
		"lexical-only check without symlink resolution does not detect shadow.log -> /etc/passwd")

	// Post-symlink check (checkSymlinks=true) catches /etc/passwd and rejects with ErrSensitivePathDenied:
	err := ValidateUniformDenylist([]string{shadowLink}, true)
	require.ErrorIs(t, err, ErrSensitivePathDenied, "post-symlink resolution must block shadow.log -> /etc/passwd")

	// 2. Wildcard glob over directory containing shadow.log -> /etc/passwd:
	wildcardGlob := filepath.Join(adumpDir, "*.log")
	err = ValidateUniformDenylist([]string{wildcardGlob}, true)
	require.ErrorIs(t, err, ErrSensitivePathDenied, "post-symlink resolution must block wildcard glob matching shadow.log -> /etc/passwd")

	// 3. Double-star glob over ancestor containing shadow.log -> /etc/passwd:
	doubleStarGlob := filepath.Join(tmpDir, "u01", "app", "**", "*.log")
	err = ValidateUniformDenylist([]string{doubleStarGlob}, true)
	require.ErrorIs(t, err, ErrSensitivePathDenied, "post-symlink resolution must block ** glob matching shadow.log -> /etc/passwd")

	// Remove shadowLink so we can test ancestor directory symlink and extension symlink independently:
	require.NoError(t, os.Remove(shadowLink))

	// 4. Ancestor directory symlink where leaf file does not exist yet:
	linkedEtc := filepath.Join(tmpDir, "u01", "app", "linked_etc")
	require.NoError(t, os.Symlink("/etc", linkedEtc))
	nonExistentLeafUnderSymlink := filepath.Join(linkedEtc, "nonexistent_future_audit.log")
	err = ValidateUniformDenylist([]string{nonExistentLeafUnderSymlink}, true)
	require.ErrorIs(t, err, ErrSensitivePathDenied, "post-symlink resolution on existing ancestor must block nonexistent leaf under symlinked /etc")
	require.NoError(t, os.Remove(linkedEtc))

	// 5. Symlinked subdirectory at depth >= 2 under a ** glob (e.g. <tmpdir>/u01/app/oracle/evil_dir -> /etc with <tmpdir>/u01/app/**/*.conf):
	evilDirLink := filepath.Join(tmpDir, "u01", "app", "oracle", "evil_dir")
	require.NoError(t, os.Symlink("/etc", evilDirLink))
	nestedDoubleStarGlob := filepath.Join(tmpDir, "u01", "app", "**", "*.conf")
	err = ValidateUniformDenylist([]string{nestedDoubleStarGlob}, true)
	require.ErrorIs(t, err, ErrSensitivePathDenied, "post-symlink resolution must block symlinked subdirectory traversed by ** glob")
	require.NoError(t, os.Remove(evilDirLink))

	// 6. Symlink to a file with a sensitive extension (*.key):
	secretKeyFile := filepath.Join(adumpDir, "private.key")
	require.NoError(t, os.WriteFile(secretKeyFile, []byte("SECRET"), 0600))
	disguisedKeyLink := filepath.Join(adumpDir, "disguised.aud")
	require.NoError(t, os.Symlink(secretKeyFile, disguisedKeyLink))
	err = ValidateUniformDenylist([]string{disguisedKeyLink}, true)
	require.ErrorIs(t, err, ErrSensitiveExtensionDenied, "post-symlink resolution must block symlink targeting *.key")
}

func TestGuardrail3_SensitiveExtensionDenylistAndSynthesizedExclude(t *testing.T) {
	deniedExtensions := []string{
		"/u01/app/server.key",
		"/u01/app/server.ke*",
		"/u01/app/server.k?y",
		"/u01/app/*.db*",
		"/u01/app/data.sqli*",
		"/u01/app/cert.pem",
		"/u01/app/keystore.pfx",
		"/u01/app/keystore.p12",
		"/u01/app/keystore.kdb",
		"/u01/app/keystore.jks",
		"/u01/app/app.keystore",
		"/u01/app/.env",
		"/u01/app/prod.env",
		"/u01/app/oracle/system01.dbf",
		`D:\SQLData\master.mdf`,
		`D:\SQLData\mastlog.ldf`,
		`D:\SQLData\secondary.ndf`,
		"/u01/app/data.sqlite",
		"/u01/app/data.sqlite3",
		"/u01/app/data.sqlite-wal",
		"/u01/app/state.db",
		`D:\AppLogs\agent.exe`,
		`D:\AppLogs\ library.dll`,
		"/u01/app/plugin.so",
		"/var/log/syslog.1.gz",
		"/var/log/syslog.1.bz2",
		"/var/log/syslog.1.xz",
		"/var/log/syslog.1.zst",
		"/var/log/archive.zip",
		"/var/log/archive.tar",
	}

	for _, p := range deniedExtensions {
		t.Run("DeniedExt_"+p, func(t *testing.T) {
			err := ValidateUniformDenylist([]string{p}, false)
			require.ErrorIs(t, err, ErrSensitiveExtensionDenied, "expected %q to be rejected by Guardrail 3", p)
		})
	}

	// Verify Evaluate() synthesizes include_file_path: true and DefaultExcludeGlobs with **/ prefixes:
	pol := &FileLogSourcePolicy{
		ID:      "oracle_audit",
		Include: []string{"/u01/app/oracle/admin/*/adump/*.aud"},
	}
	conf, err := pol.Evaluate(context.Background())
	require.NoError(t, err)

	assert.Equal(t, []any{"/u01/app/oracle/admin/*/adump/*.aud"}, conf.Get("receivers::filelog::include"))
	assert.Equal(t, true, conf.Get("receivers::filelog::include_file_path"))

	excludes, ok := conf.Get("receivers::filelog::exclude").([]any)
	require.True(t, ok)
	require.Len(t, excludes, len(DefaultExcludeGlobs))
	assert.Contains(t, excludes, "**/*.gz")
	assert.Contains(t, excludes, "**/*.key")
	assert.Contains(t, excludes, "**/*.pem")
	assert.Contains(t, excludes, "**/*.dbf")
	assert.Contains(t, excludes, "**/*.db")
}

func TestGuardrail4_MultiPolicyDeduplicationBaselineVsMerged(t *testing.T) {
	pol1 := &FileLogSourcePolicy{
		ID:      "policy_oracle",
		Include: []string{"/u01/app/oracle/admin/*/adump/*.aud", "/var/adm/messages"},
	}
	pol2 := &FileLogSourcePolicy{
		Name:    "policy_cots",
		Include: []string{"/var/adm/messages", `D:\AppLogs\sep\*.log`},
	}

	// 1. Baseline (without MergePolicies): naive confmap.Merge overwrites receivers::filelog::include
	// with only the second policy's include slice because confmap.enableMergeAppendOption only appends
	// service:: slices (extensions, receivers, exporters).
	c1, err := pol1.Evaluate(context.Background())
	require.NoError(t, err)
	c2, err := pol2.Evaluate(context.Background())
	require.NoError(t, err)

	naiveMerged := confmap.New()
	require.NoError(t, naiveMerged.Merge(c1))
	require.NoError(t, naiveMerged.Merge(c2))
	assert.Equal(t, []any{"/var/adm/messages", `D:\AppLogs\sep\*.log`}, naiveMerged.Get("receivers::filelog::include"),
		"Naive confmap.Merge drops /u01/app/oracle/admin/*/adump/*.aud from pol1")

	// 2. Proposed (with MergePolicies): preserves all unique include globs in deterministic sorted set-union order.
	mergedPolicy := MergePolicies([]*FileLogSourcePolicy{pol1, pol2})
	mergedConf, err := mergedPolicy.Evaluate(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []any{
		"/u01/app/oracle/admin/*/adump/*.aud",
		"/var/adm/messages",
		`D:\AppLogs\sep\*.log`,
	}, mergedConf.Get("receivers::filelog::include"))

	// Verify LogsPipelines wires the shared filelog receiver through googlepolicy processor.
	gpID := component.MustNewID("googlepolicy")
	expID := component.MustNewIDWithName("otlp_grpc", "default_gcp_destination")
	pipeConf, err := mergedPolicy.LogsPipelines([]component.ID{gpID}, []component.ID{expID}, nil)
	require.NoError(t, err)
	assert.Equal(t, []any{"filelog"}, pipeConf.Get("service::pipelines::logs/filelog::receivers"))
	assert.Equal(t, []any{"googlepolicy"}, pipeConf.Get("service::pipelines::logs/filelog::processors"))
	assert.Equal(t, []any{"otlp_grpc/default_gcp_destination"}, pipeConf.Get("service::pipelines::logs/filelog::exporters"))
}

func TestRegistryLoadsFileLogPolicyWithIDAndName(t *testing.T) {
	polByID, err := googlepolicy.LoadPolicy(PolicyType, map[string]any{
		"type":    PolicyType,
		"id":      "oracle-aud-policy",
		"include": []any{"/u01/app/oracle/admin/*/adump/*.aud"},
	})
	require.NoError(t, err)
	assert.Equal(t, "oracle-aud-policy", polByID.PolicyName())

	polByName, err := googlepolicy.LoadPolicy(PolicyType, map[string]any{
		"type":    PolicyType,
		"name":    "cots-log-policy",
		"include": []any{"/var/adm/messages"},
	})
	require.NoError(t, err)
	assert.Equal(t, "cots-log-policy", polByName.PolicyName())
}
