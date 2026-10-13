# Milestone 1 Evidence: `FileLogSourcePolicy`, 4-Guardrail Validator, and Multi-Policy Deduplication

**Milestone**: Milestone 1 of `PROTOTYPE_PLAN.md`
**Target Package**: `github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/filelog` & `googlecontrolplaneprovider`

---

## 1. Deliverables Implemented

1. **`policies/filelog/filelog_policy.go`**:
   - Registers `PolicyType = "file_log"` in `pkg/googlepolicy` (`GenericDriver[*FileLogSourcePolicy]`), supporting both `id` (`mapstructure:"id"`) and `name` (`mapstructure:"name"`).
   - Implements `ValidateBaselineAllowlist(include []string) error` (the 5-root hardcoded allowlist from `go/tango-preview-policies`: `/var/log/**`, `/opt/**/logs/**`, `/srv/**/logs/**`, `%ProgramData%\**\logs\**`, `%SystemDrive%\inetpub\logs\**`) for non-tautological Baseline-vs-Change evaluation.
   - Implements `ValidateUniformDenylist(include []string, checkSymlinks bool) error` enforcing all 4 guardrails from Tab 6:
     - **Guardrail 1 (Structural Glob Breadth Check)**: Requires an absolute Linux (`/`) or Windows drive/env root (`C:\`, `D:\`, `%ProgramData%\`, `%SystemDrive%\`, `%SystemRoot%\`), rejects any `.` or `..` traversal segment prior to path cleaning, and requires `>= 2` concrete non-wildcard leading segments before any wildcard or leaf filename (`>= 3` total segments: `/{seg1}/{seg2}/{leaf_or_glob}` on Linux; `{DriveOrEnvRoot}\{dir1}\{leaf_or_glob}` on Windows).
     - **Guardrail 2 (Sensitive Directory Denylist — Pre- & Post-Symlink Resolution)**: Enforces all 19 Linux patterns (`/etc/**`, `/root/**`, `/proc/**`, `/sys/**`, `/dev/**`, `/boot/**`, `/usr/**`, `/bin/**`, `/sbin/**`, `/lib/**`, `/lib64/**`, `/var/run/secrets/**`, `/run/secrets/**`, `/var/lib/docker/**`, `/var/lib/containerd/**`, `/var/lib/kubelet/**`, `/**/.ssh/**`, `/**/.aws/**`, `/**/.config/gcloud/**`) and all 8 Windows patterns (`%SystemRoot%\System32\config\**`, `%SystemRoot%\NTDS\**`, `%SystemRoot%\System32\winevt\**`, `%SystemRoot%\repair\**`, `**\Users\**\.ssh\**`, `**\Microsoft\Crypto\**`, `**\Microsoft\Protect\**`, `**\Microsoft\Credentials\**`, with case-insensitivity and `<Drive>:\Windows` / `%SystemDrive%\Windows` canonicalized to `%SystemRoot%`). Performs post-symlink resolution (`filepath.EvalSymlinks`) across the longest existing concrete ancestor, `filepath.Glob` matches, and `filepath.WalkDir` symlink entries under the longest existing concrete prefix, re-verifying resolved targets against both Guardrail 2 and Guardrail 3.
     - **Guardrail 3 (Sensitive Key, Database & Binary Extension Denylist)**: Rejects all 23 sensitive/binary/archive extensions (`*.key`, `*.pem`, `*.pfx`, `*.p12`, `*.kdb`, `*.jks`, `*.keystore`, `*.env`, `*.dbf`, `*.mdf`, `*.ldf`, `*.ndf`, `*.sqlite*`, `*.db`, `*.exe`, `*.dll`, `*.so`, `*.gz`, `*.bz2`, `*.xz`, `*.zst`, `*.zip`, `*.tar`) and synthesizes `DefaultExcludeGlobs` (`**/*.ext` plus `**/README`) on `receivers::filelog`.
     - **Guardrail 4 (Synthesis & Multi-Policy Deduplication)**: `MergePolicies` computes the deterministic sorted set-union of `include` globs across multiple `FileLogSourcePolicy` instances and synthesizes `receivers::filelog` (`include_file_path: true`) and `service::pipelines::logs/filelog`.
2. **`provider.go`**:
   - Registers `policies/filelog` and merges all valid `*filelog.FileLogSourcePolicy` instances in `evaluatePolicySet` via `filelog.MergePolicies` while recording `PolicyEvaluateErrorEvent` for any invalid `FileLogSourcePolicy` without failing the policy set.
3. **`policies/filelog/filelog_policy_test.go` & `provider_test.go`**:
   - Unit and provider tests covering all 4 guardrails, baseline-vs-change comparisons, and multi-policy deduplication.

---

## 2. Non-Tautological Baseline-vs-Change Proof

### A. 5-Root Hardcoded Allowlist (`ValidateBaselineAllowlist`) vs. 4-Guardrail Uniform Denylist (`ValidateUniformDenylist`)

| Test Case / Path | Allianz Finding | `ValidateBaselineAllowlist` (Baseline) | `ValidateUniformDenylist` (Proposed) |
| :--- | :--- | :--- | :--- |
| `/u01/app/oracle/admin/ORCL/adump/ora_1234.aud` | `#33` (Oracle OFA Audit Trail) | **REJECTED** (`ErrNotInBaselineAllowlist`) | **ACCEPTED** (`nil`) |
| `/var/adm/messages` | `#32` (COTS Linux Log Path) | **REJECTED** (`ErrNotInBaselineAllowlist`) | **ACCEPTED** (`nil`) |
| `D:\AppLogs\sep\agi.log` | `#32` / `#76` (Windows Secondary Drive COTS) | **REJECTED** (`ErrNotInBaselineAllowlist`) | **ACCEPTED** (`nil`) |
| `%SystemRoot%\System32\LogFiles\Firewall\pfirewall.log` | `#76` (Windows System32 LogFiles) | **REJECTED** (`ErrNotInBaselineAllowlist`) | **ACCEPTED** (`nil`) |
| `/etc/shadow` | Guardrail 2 (Linux Sensitive Dir) | **REJECTED** (`ErrNotInBaselineAllowlist`) | **REJECTED** (`ErrSensitivePathDenied`: `/etc/**`) |
| `C:\Windows\System32\config\SAM` | Guardrail 2 (Windows Sensitive Dir) | **REJECTED** (`ErrNotInBaselineAllowlist`) | **REJECTED** (`ErrSensitivePathDenied`: `%SystemRoot%\System32\config\**`) |
| `/var/log/../../etc/passwd` | Guardrail 1 (`..` Path Traversal) | **REJECTED** (`ErrPathTraversal`) | **REJECTED** (`ErrPathTraversal`) |
| `<tmpdir>/u01/app/oracle/admin/ORCL/adump/shadow.log -> /etc/passwd` | Guardrail 2 (Symlink Escape) | **ACCEPTED** when `checkSymlinks=false` (Lexical-Only Baseline) | **REJECTED** when `checkSymlinks=true` (`ErrSensitivePathDenied`: `/etc/**`) |

### B. Multi-Policy `confmap.Merge` Overwrite (Baseline) vs. `filelog.MergePolicies` Set-Union (Proposed)

- **Baseline (Naive `confmap.Merge` of two `FileLogSourcePolicy` confmaps)**:
  - Because `confmap.enableMergeAppendOption` in `provider.go` only appends `service::` slices (`service::extensions`, `service::**::receivers`, `service::**::exporters`), component-level slices like `receivers::filelog::include` use last-writer-wins semantics.
  - Merging `pol1` (`["/u01/app/oracle/admin/*/adump/*.aud", "/var/adm/messages"]`) and `pol2` (`["/var/adm/messages", "D:\\AppLogs\\sep\\*.log"]`) drops `/u01/app/oracle/admin/*/adump/*.aud`, leaving `["/var/adm/messages", "D:\\AppLogs\\sep\\*.log"]`.
- **Proposed (`filelog.MergePolicies` in `provider.go`)**:
  - Preserves all unique `include` globs in deterministic sorted set-union order:
    `["/u01/app/oracle/admin/*/adump/*.aud", "/var/adm/messages", "D:\\AppLogs\\sep\\*.log"]`.

---

## 3. Companion One-Pager (`tango_onepager_6_filelog_enterprise_paths.md`) Boundary Audit Findings

1. **1-Segment vs. 2-Segment Glob Boundary (`Guardrail 1` vs. Prose/Table Examples in Tab 6)**:
   - Under Guardrail 1 (`>= 2` concrete non-wildcard leading segments before any wildcard):
     - **Linux**: Strip leading `/` and split on `/`, requiring `/{seg1}/{seg2}/{leaf_or_glob}`.
       - **Rejected (1 concrete leading segment)**: `/*`, `/**`, `/u01/**`, `/u01/*`, `/u01/*.log`, `/data/**`, `/apps/**`.
       - **Accepted (`>= 2` concrete leading segments)**: `/u01/app/**`, `/u01/app/oracle/admin/*/adump/*.aud`, `/var/adm/**`, `/var/adm/messages`, `/data/logs/**`, `/apps/sep/**`.
     - **Windows**: Normalize `\` and split on `\`, counting the drive/env root (`C:`, `D:`, `%ProgramData%`, `%SystemDrive%`, `%SystemRoot%`) as segment 1 + a concrete top-level directory (`AppLogs`, `System32`, `Vendor`) as segment 2 (`{DriveOrEnvRoot}\{dir1}\{leaf_or_glob}`).
       - **Rejected (1 concrete leading segment)**: `C:\*`, `C:\**`, `D:\**`, `D:\**\*.log`, `E:\**\*.txt`, `%ProgramData%\**\logs\**`.
       - **Accepted (`>= 2` concrete leading segments)**: `D:\AppLogs\**`, `D:\AppLogs\**\*.log`, `E:\AppLogs\**\*.txt`, `%ProgramData%\Vendor\logs\**`, `%SystemRoot%\System32\LogFiles\**`.
   - **Recommended Spec Update for `tango_onepager_6_filelog_enterprise_paths.md`**:
     - In **Line 14**, change `such as /var/adm/**, /u01/**, /data/**, /apps/**` to `such as /var/adm/**, /u01/app/**, /data/logs/**, /apps/sep/**` (since `/u01/**`, `/data/**`, `/apps/**` have only 1 leading segment and are rejected by Guardrail 1 to prevent full-volume I/O walks on mounted data disks).
     - In **Line 15**, **Line 82**, and **Line 85**, change `D:\**\*.log, E:\**\*.txt` and `D:\**` to `D:\AppLogs\**\*.log, E:\ServiceLogs\**\*.txt` and `D:\AppLogs\**` (matching Line 21's `D:\AppLogs\**`, since `D:\**` has only 1 leading segment `D:` and is rejected by Guardrail 1 just like `C:\**`).
     - In **Line 38**, change `before any "**" wildcard` to `before any "*" or "**" wildcard` (since `/*` and `C:\*` use single `*`).
2. **Real-World Linux Host Discovery: `systemd` `/var/log/README -> /usr/share/doc/systemd/README.logs` Symlink**:
   - On standard systemd Linux distributions (Debian, Ubuntu, RHEL, gLinux), `/usr/lib/tmpfiles.d/legacy.conf` creates a factory symlink `/var/log/README -> ../../usr/share/doc/systemd/README.logs`.
   - Because `/usr/**` is in Guardrail 2's Linux Sensitive Directory Denylist, a wildcard walk of `/var/log/**` on a live systemd Linux host encounters `/var/log/README -> /usr/share/doc/systemd/README.logs` and would reject `include: ["/var/log/**"]` unless `**/README` is included in `DefaultExcludeGlobs` (and excluded candidate files are skipped during wildcard symlink scans). Adding `**/README` to `DefaultExcludeGlobs` ensures `filelogreceiver` ignores the static systemd `README` symlink and allows `/var/log/**` on real systemd hosts.

---

## 4. Exact Verification Command & Output

Command:
```bash
cd /tmp/prototypes/tab6_filelog_denylist/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider && go test -v ./policies/filelog/... .
```

Output summary:
```text
=== RUN   TestBaselineAllowlistVsProposedDenylist
--- PASS: TestBaselineAllowlistVsProposedDenylist (0.02s)
    --- PASS: TestBaselineAllowlistVsProposedDenylist/Enterprise_Oracle_OFA_Audit_Trail_Log (0.00s)
    --- PASS: TestBaselineAllowlistVsProposedDenylist/Enterprise_Linux_COTS_Var_Adm_Messages (0.00s)
    --- PASS: TestBaselineAllowlistVsProposedDenylist/Enterprise_Windows_Secondary_Drive_SEP_Log (0.00s)
    --- PASS: TestBaselineAllowlistVsProposedDenylist/Enterprise_Windows_System32_LogFiles_Firewall (0.00s)
    --- PASS: TestBaselineAllowlistVsProposedDenylist/SharedValid_/var/log/syslog (0.00s)
    --- PASS: TestBaselineAllowlistVsProposedDenylist/SharedValid_/var/log/** (0.01s)
    --- PASS: TestBaselineAllowlistVsProposedDenylist/SharedValid_/opt/myapp/logs/app.log (0.00s)
    --- PASS: TestBaselineAllowlistVsProposedDenylist/SharedValid_/srv/http/logs/access.log (0.00s)
    --- PASS: TestBaselineAllowlistVsProposedDenylist/SharedValid_%SystemDrive%\inetpub\logs\LogFiles\W3SVC1\u_ex.log (0.00s)
    --- PASS: TestBaselineAllowlistVsProposedDenylist/SharedValid_%ProgramData%\Symantec\logs\agi.log (0.00s)
    --- PASS: TestBaselineAllowlistVsProposedDenylist/SharedRejected_/etc/shadow (0.00s)
    --- PASS: TestBaselineAllowlistVsProposedDenylist/SharedRejected_C:\Windows\System32\config\SAM (0.00s)
    --- PASS: TestBaselineAllowlistVsProposedDenylist/SharedRejected_/var/log/../../etc/passwd (0.00s)
    --- PASS: TestBaselineAllowlistVsProposedDenylist/SharedRejected_relative/path/app.log (0.00s)
=== RUN   TestGuardrail1_StructuralGlobBreadthAndOnePagerBoundary
--- PASS: TestGuardrail1_StructuralGlobBreadthAndOnePagerBoundary (0.00s)
=== RUN   TestGuardrail2_SensitiveDirectoryDenylist_All19LinuxAnd8WindowsPatterns
--- PASS: TestGuardrail2_SensitiveDirectoryDenylist_All19LinuxAnd8WindowsPatterns (0.00s)
=== RUN   TestGuardrail2_PostSymlinkResolutionAndAlt4Proof
--- PASS: TestGuardrail2_PostSymlinkResolutionAndAlt4Proof (0.00s)
=== RUN   TestGuardrail3_SensitiveExtensionDenylistAndSynthesizedExclude
--- PASS: TestGuardrail3_SensitiveExtensionDenylistAndSynthesizedExclude (0.00s)
=== RUN   TestGuardrail4_MultiPolicyDeduplicationBaselineVsMerged
--- PASS: TestGuardrail4_MultiPolicyDeduplicationBaselineVsMerged (0.00s)
=== RUN   TestRegistryLoadsFileLogPolicyWithIDAndName
--- PASS: TestRegistryLoadsFileLogPolicyWithIDAndName (0.00s)
PASS
ok  	github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/filelog	0.156s
=== RUN   TestRetrieve_MultipleFileLogPoliciesMergedAndInvalidSkipped
--- PASS: TestRetrieve_MultipleFileLogPoliciesMergedAndInvalidSkipped (0.02s)
PASS
ok  	github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider	10.454s
```
