# Milestone 2 Evidence: Empirical End-to-End `filelogreceiver` + Symlink Attack Defense + Live `telemetry.googleapis.com:443` Export (`dashpole-dev`)

**Milestone**: Milestone 2 of `PROTOTYPE_PLAN.md`
**Target File**: `components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/filelog/e2e_filelog_enterprise_test.go`

---

## 1. Deliverables Implemented

1. **`go.mod` & `go.sum`**:
   - Added `github.com/open-telemetry/opentelemetry-collector-contrib/receiver/filelogreceiver v0.162.0` to `googlecontrolplaneprovider/go.mod` so the real upstream `filelogreceiver` can be instantiated directly from `googlecontrolplaneprovider`-synthesized configurations.
2. **`policies/filelog/e2e_filelog_enterprise_test.go`**:
   - **`TestE2E_FileLogEnterpriseLogs_ExcludeNegativeControl_OSDAC_AndLiveGCPExport`**:
     - Creates a temporary enterprise filesystem layout containing:
       - Simulated Oracle OFA audit trail file: `<tmpdir>/u01/app/oracle/admin/ORCL/adump/ora_9876.aud`
       - Linux COTS log file: `<tmpdir>/var/adm/messages.log`
       - Compressed archive file: `<tmpdir>/u01/app/oracle/admin/ORCL/adump/ora_old.aud.gz`
       - Private key file: `<tmpdir>/u01/app/oracle/admin/ORCL/adump/server.key`
       - Unreadable OS DAC file: `<tmpdir>/u01/app/oracle/admin/ORCL/adump/root_only.aud` (mode `0000`)
     - Runs a non-tautological **Negative Control (`exclude: nil`)** with `include: ["<tmpdir>/u01/app/oracle/admin/*/adump/*"]` on `filelogreceiver`, proving that without Guardrail 3's synthesized `**/*.ext` `exclude` list, `filelogreceiver` ingests `server.key` (`-----BEGIN PRIVATE KEY----- SECRET_KEY_MATERIAL_DO_NOT_INGEST`) and `ora_old.aud.gz` (`COMPRESSED_ARCHIVE_PAYLOAD_DO_NOT_INGEST`).
     - Evaluates two `FileLogSourcePolicy` instances (`oracle_ofa_policy` and `cots_var_adm_policy`) + a `GCPDestinationPolicy` (`ProjectID: "dashpole-dev"`) via `googlecontrolplaneprovider.Retrieve()`.
     - Unmarshals the provider-synthesized `receivers::filelog` map into `filelogreceiver.NewFactory()`, connects it through `googlepolicyprocessor` to a live `otlpexporter` authenticated via `googleclientauthextension` to `telemetry.googleapis.com:443` in `dashpole-dev`, and verifies:
       1. `ora_9876.aud` and `messages.log` are tailed with `log.file.path` populated (`include_file_path: true`).
       2. `server.key` and `ora_old.aud.gz` are excluded by Guardrail 3's synthesized `**/*.key` and `**/*.gz` `exclude` patterns.
       3. `root_only.aud` (mode `0000`) is blocked by OS kernel POSIX DAC permissions (Guardrail 4) without crashing `filelogreceiver`.
       4. Both valid enterprise log records are exported to live `telemetry.googleapis.com:443` in `dashpole-dev` with `0` export errors.
   - **`TestE2E_SymlinkEscapeAttack_BaselineLexicalVsProposedPostSymlinkResolution`**:
     - Creates a real symlink `<tmpdir>/u01/app/oracle/admin/ORCL/adump/shadow.log -> /etc/passwd`.
     - **Baseline (`DisableSymlinkResolution: true`)**: Shows that lexical-only validation accepts `shadow.log` and `filelogreceiver` leaks 61 lines from `/etc/passwd` (including `root:x:0:0:...`).
     - **Proposed (Guardrail 2 Post-Symlink Resolution Enabled)**: Shows that both direct path (`<tmpdir>/.../adump/shadow.log`) and wildcard glob (`<tmpdir>/.../adump/*.log`) resolve the symlink via `filepath.EvalSymlinks` to `/etc/passwd` and reject the policy with `ErrSensitivePathDenied` (`matches /etc/**`) before `filelogreceiver` can start.

---

## 2. Non-Tautological Baseline-vs-Change Summary

| Validation / Pipeline Scenario | Baseline Behavior (Guardrail Disabled) | Proposed Behavior (Guardrail Enabled) |
| :--- | :--- | :--- |
| **Broad Directory Glob `<tmpdir>/u01/app/oracle/admin/*/adump/*` Containing `server.key` & `ora_old.aud.gz`** | With `exclude: nil`, `filelogreceiver` ingests **3 records** including `server.key` (`-----BEGIN PRIVATE KEY----- ...`) and `ora_old.aud.gz` | With Guardrail 3's synthesized `**/*.ext` `exclude` list, `filelogreceiver` ingests **only `ora_9876.aud`** (plus `/var/adm/messages.log` from the merged COTS policy) and excludes `server.key` and `ora_old.aud.gz` |
| **Symlink Escape `<tmpdir>/u01/app/oracle/admin/ORCL/adump/shadow.log -> /etc/passwd`** | With `DisableSymlinkResolution: true` (lexical-only check), policy validation succeeds and `filelogreceiver` leaks **61 lines** from `/etc/passwd` (`root:...`) | With Guardrail 2 post-symlink resolution enabled (`filepath.EvalSymlinks`), `Evaluate()` rejects both direct path and `*.log` wildcard policies with `ErrSensitivePathDenied: "/etc/passwd" matches /etc/**` |
| **OS POSIX DAC `0000`-Mode File (`root_only.aud`)** | N/A | OS kernel denies read on `root_only.aud`; `filelogreceiver` logs permission warning, skips `root_only.aud`, and tails `ora_9876.aud` & `messages.log` cleanly |
| **Live OTLP/gRPC Export to `telemetry.googleapis.com:443` (`dashpole-dev`)** | N/A | `2` tailed enterprise log records (`ora_9876.aud` and `/var/adm/messages.log`) with `log.file.path` populated are exported via `googlepolicyprocessor` + `otlpexporter` (`googleclientauthextension`) to `telemetry.googleapis.com:443` with `0` errors |

---

## 3. Exact Verification Command & Output

Command:
```bash
cd /tmp/prototypes/tab6_filelog_denylist/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider && go test -v -run TestE2E ./policies/filelog/...
```

Output:
```text
=== RUN   TestE2E_FileLogEnterpriseLogs_ExcludeNegativeControl_OSDAC_AndLiveGCPExport
    e2e_filelog_enterprise_test.go:166: [Baseline Negative Control (exclude: nil)] Ingested 3 records including server.key and ora_old.aud.gz: [-----BEGIN PRIVATE KEY----- SECRET_KEY_MATERIAL_DO_NOT_INGEST Audit file /u01/app/oracle/admin/ORCL/adump/ora_9876.aud | ACTION : [100] 'LOGON' | DATABASE USER:[1] '/' | PRIVILEGE : [6] 'SYSDBA' | STATUS:[1] '0' COMPRESSED_ARCHIVE_PAYLOAD_DO_NOT_INGEST]
    e2e_filelog_enterprise_test.go:296: [Proposed Synthesis + Live Export] Successfully tailed & exported 2 records to telemetry.googleapis.com:443 (dashpole-dev): paths=[/tmp/TestE2E_FileLogEnterpriseLogs_ExcludeNegativeControl_OSDAC_AndLi762592566/001/u01/app/oracle/admin/ORCL/adump/ora_9876.aud /tmp/TestE2E_FileLogEnterpriseLogs_ExcludeNegativeControl_OSDAC_AndLi762592566/001/var/adm/messages.log]
--- PASS: TestE2E_FileLogEnterpriseLogs_ExcludeNegativeControl_OSDAC_AndLiveGCPExport (2.41s)
=== RUN   TestE2E_SymlinkEscapeAttack_BaselineLexicalVsProposedPostSymlinkResolution
    e2e_filelog_enterprise_test.go:357: [Baseline (Symlink Resolution Disabled)] filelogreceiver leaked 61 lines from /etc/passwd via /tmp/TestE2E_SymlinkEscapeAttack_BaselineLexicalVsProposedPostSymlink277639384/001/u01/app/oracle/admin/ORCL/adump/shadow.log!
    e2e_filelog_enterprise_test.go:372: [Proposed (Post-Symlink Resolution Enabled - Direct)] Blocked policy with error: include glob targets a sensitive OS, credential, or container directory on the denylist: "/etc/passwd" matches /etc/** (via symlink "/tmp/TestE2E_SymlinkEscapeAttack_BaselineLexicalVsProposedPostSymlink277639384/001/u01/app/oracle/admin/ORCL/adump/shadow.log" -> "/etc/passwd")
    e2e_filelog_enterprise_test.go:381: [Proposed (Post-Symlink Resolution Enabled - Wildcard Glob)] Blocked policy with error: include glob targets a sensitive OS, credential, or container directory on the denylist: "/etc/passwd" matches /etc/** (via symlink "/tmp/TestE2E_SymlinkEscapeAttack_BaselineLexicalVsProposedPostSymlink277639384/001/u01/app/oracle/admin/ORCL/adump/*.log" -> "/etc/passwd")
--- PASS: TestE2E_SymlinkEscapeAttack_BaselineLexicalVsProposedPostSymlinkResolution (0.15s)
PASS
ok  	github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/filelog	2.884s
```
