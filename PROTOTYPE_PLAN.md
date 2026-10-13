# Prototype Plan: Supporting Enterprise COTS and Database Log Paths in `FileLogSourcePolicy` (Tab 6)

**Author**: dashpole@google.com
**Companion Design Doc**: `tango_onepager_6_filelog_enterprise_paths.md` (Tab 6 — Project Tango Bindplane Findings `#33`, `#32`, `#76`)
**Target Repository**: `opentelemetry-operations-collector` (`components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider`)
**Branch**: `proto/tango-tab6-filelog-denylist` (from `v0.162.0`)

---

## 1. Core Question & Hypothesis

In `go/tango-preview-policies`, `FileLogSourcePolicy` restricts `include` globs to a hardcoded allowlist of 5 OS log roots:
- **Linux**: `/var/log/**`, `/opt/**/logs/**`, `/srv/**/logs/**`
- **Windows**: `%ProgramData%\**\logs\**`, `%SystemDrive%\inetpub\logs\**`

During the Allianz Google SecOps / Bindplane evaluation, three `High` findings (`#33` Oracle OFA audit trail `/u01/app/oracle/admin/*/adump/*.aud`, `#32` COTS application logs `/var/adm/**` and vendor mount points without a literal `/logs/` segment, and `#76` Windows secondary drives `D:\AppLogs\**` and `%SystemRoot%\System32\LogFiles\**`) demonstrated that static root allowlists block standard enterprise database and COTS log layouts.

**Hypothesis**: We can replace the 5-root allowlist in `googlecontrolplaneprovider` with a **fleet-uniform, deterministic 4-Guardrail Validator and `FileLogSourcePolicy` synthesizer** (`policies/filelog/filelog_policy.go`) that:
1. Accepts legitimate enterprise COTS and database paths across Linux and Windows without host-specific environment variables or schema expansion.
2. Rejects overly broad full-disk globs (`/*`, `/**`, `/u01/**`, `C:\*`, `C:\**`, `D:\**`) and path traversal (`..`) via a structural glob breadth check (`>= 2` concrete non-wildcard leading segments before any wildcard).
3. Blocks sensitive OS, credential, and container runtime directories on Linux and Windows both **pre-symlink** (lexical glob validation with Windows case-folding and `%SystemRoot%` canonicalization) and **post-symlink** (`filepath.EvalSymlinks` on existing filesystem ancestors and `filepath.WalkDir` entries).
4. Blocks sensitive key, database datafile, binary, and compressed archive extensions (`*.key`, `*.pem`, `*.dbf`, `*.db`, `*.gz`, etc., matched against both `*.ext` and `**/*.ext`) at validation time AND automatically populates `**/*.ext` `exclude` patterns on the synthesized `filelog` receiver (since `stanza/fileconsumer` evaluates `doublestar.PathMatch(exclude, fullPath)` where bare `*` does not cross path separators).
5. Enforces Guardrail 4 (OS-level POSIX DAC least-privilege resilience + `filelog` synthesis with `include_file_path: true`) and deduplicates multiple `FileLogSourcePolicy` instances via set-union of valid `include` globs into a single shared `filelog` receiver connected to the logs export pipeline.

---

## 2. Scope & Intentional Simplifications

### In Scope
- Implementation of `FileLogSourcePolicy` (`type: file_log`, supporting both `id` and `name` keys via `mapstructure`) in `components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/filelog/filelog_policy.go`.
- Implementation of both the **Baseline 5-Root Allowlist Validator** (`ValidateBaselineAllowlist`) and the **Proposed 4-Guardrail Uniform Denylist Validator** (`ValidateUniformDenylist`) for symmetric, non-tautological comparison.
- Multi-policy deduplication in `provider.go` (`evaluatePolicySet`) that merges multiple `FileLogSourcePolicy` instances via deterministic sorted set-union of `include` globs into a single `filelog` receiver (`include_file_path: true` + default `**/*.ext` `exclude` list) and `logs/filelog` pipeline (necessary because `confmap.enableMergeAppendOption` in `provider.go` only appends `service::` slices such as `service::extensions`, `service::**::receivers`, and `service::**::exporters`, leaving component-level slices like `receivers::filelog::include` as last-writer-wins).
- End-to-end empirical verification using the real upstream `filelogreceiver` (`github.com/open-telemetry/opentelemetry-collector-contrib/receiver/filelogreceiver v0.162.0`), `googlepolicyprocessor`, `googleclientauthextension`, and `otlpexporter` exporting tailed enterprise logs (with `log.file.path` populated) to live `telemetry.googleapis.com:443` in GCP project `dashpole-dev`, plus verifying Guardrail 3's synthesized `exclude` list (with a negative control where `exclude: nil` ingests `server.key`) and Guardrail 4's OS DAC behavior on an unreadable (`0000`-mode) file.
- End-to-end empirical symlink attack reproduction (`.../u01/app/oracle/admin/ORCL/adump/shadow.log -> /etc/passwd`) comparing naive lexical validation (which leaks `/etc/passwd` through `filelogreceiver`) against Guardrail 2 post-symlink resolution (which blocks the policy and prevents reading `/etc/passwd`).
- Auditing `tango_onepager_6_filelog_enterprise_paths.md` against the concrete implementation, specifically testing and documenting the 1-segment vs. 2-segment glob boundary (`/u01/**` and `D:\**` rejected vs. `/u01/app/**` and `D:\AppLogs\**` accepted).

### Out of Scope / Intentional PoC Shortcuts
- Server-side `telemetrymanagement.googleapis.com` API plumbing: the Go validation function in `policies/filelog` is pure and deterministic (for static checks) so it can be imported directly by the Control Plane API server.
- Continuous mid-stream symlink target filtering inside `opentelemetry-collector-contrib/pkg/stanza/fileconsumer` for symlinks created *after* policy validation when using a broad directory glob. In this prototype, `Validate()` resolves symlinks on: (a) the longest existing concrete ancestor directory of the pattern, (b) `filepath.Glob` matches, and (c) `filepath.WalkDir` entries under the longest existing concrete prefix when wildcards (`*` or `**`) are present, re-evaluating resolved paths against both Guardrail 2 and Guardrail 3. Continuous `fileconsumer` open-time `O_NOFOLLOW` / realpath re-verification is documented in the Productionization Gap Ledger.
- Windows MSI `NT SERVICE\otelcol-google` service account configuration (evaluated analytically; Linux runtime execution is used for live collector tests while both Linux and Windows path grammars are tested in unit/integration tests).

---

## 3. Candidate Approaches & Symmetric Evaluation Rubric

| Dimension | Baseline (Approach A: 5-Root Hardcoded Allowlist) | Proposed (Approach B: Uniform Denylist + 4 Structural Guardrails) |
| :--- | :--- | :--- |
| **Linux Allowed Roots** | Only `/var/log/**`, `/opt/**/logs/**`, `/srv/**/logs/**` | Any absolute path with `>= 2` concrete leading directory segments (`/{seg1}/{seg2}/...`) not matching the Linux Sensitive Directory or Extension Denylists |
| **Windows Allowed Roots** | Only `%ProgramData%\**\logs\**`, `%SystemDrive%\inetpub\logs\**` | Any absolute drive (`C:\...`, `D:\...`) or env root (`%ProgramData%\...`, `%SystemDrive%\...`, `%SystemRoot%\...`) with `>= 2` concrete leading segments (drive/env token as segment 1 + concrete top-level directory as segment 2, e.g., `D:\AppLogs\**`, `%SystemRoot%\System32\LogFiles\**`) not matching the Windows Sensitive Directory or Extension Denylists |
| **Glob Breadth Protection** | Implicitly bounded by the 5 root prefixes | Guardrail 1: Rejects relative paths, `..` traversal, and globs with `< 2` concrete leading segments before any wildcard (`/*`, `/**`, `/u01/**`, `C:\*`, `C:\**`, `D:\**`, `D:\**\*.log`, `%ProgramData%\**\logs\**`). Tokenization rule: on Linux, strip leading `/` and split on `/`, requiring `>= 2` concrete non-wildcard segments (`/{seg1}/{seg2}/...`); on Windows, normalize `\` and split on `\`, counting the drive/env root (`C:`, `D:`, `%SystemRoot%`, `%SystemDrive%`, `%ProgramData%`) as segment 1 and requiring at least 1 concrete top-level directory (`D:\{dir1}\...`) as segment 2 before any wildcard (`*`, `**`, `?`, `[`) |
| **Sensitive Directory Protection** | Implicitly excluded unless inside `/var/log` | Guardrail 2: Explicit Linux (19 patterns) & Windows (8 patterns, with case-insensitive matching and canonicalization of `<Drive>:\Windows\...` and `%SystemDrive%\Windows\...` to `%SystemRoot%\...`) denylists checked pre- and post-symlink resolution (`filepath.EvalSymlinks` on longest existing concrete ancestor + `filepath.WalkDir` under concrete prefix) |
| **Sensitive Extension Protection** | None (`*.key` or `*.gz` inside `/var/log/**` allowed) | Guardrail 3: Rejects 23 key, database, binary, and archive extensions (`*.ext` / `**/*.ext`) in `include` AND auto-populates `**/*.ext` `exclude` patterns on synthesized `filelog` receiver |
| **Guardrail 4: Least Privilege & Multi-Policy Synthesis** | N/A | Guardrail 4: Runs under unprivileged POSIX DAC (`User=otelcol-google`, gracefully skipping `0000`-mode files) and merges `include` globs via set-union across `FileLogSourcePolicy` instances into a shared `filelog` receiver (`include_file_path: true`) |

---

## 4. Incremental Milestones & Falsifiable Exit Gates

### Milestone 1: Implement `FileLogSourcePolicy`, 4-Guardrail Validator, and Multi-Policy Deduplication in `googlecontrolplaneprovider`

- **Deliverables**:
  1. `components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/filelog/filelog_policy.go`:
     - Implements `FileLogSourcePolicy` (`PolicyType = "file_log"`, supporting both `ID` via `mapstructure:"id"` and `Name` via `mapstructure:"name"`), `Validate()`, `ValidateBaselineAllowlist()`, `ValidateUniformDenylist()`, `Evaluate()`, `LogsPipelines()`, and `MergeFileLogPolicies()`.
     - Enforces:
       - **Guardrail 1**: Absolute path, no `..`, and `>= 2` concrete leading segments before any `*`, `**`, `?`, `[` wildcard (Linux: strip leading `/` and split on `/`, requiring `/{seg1}/{seg2}/...`; Windows: normalize `\` and split on `\`, counting drive/env root `C:`, `D:`, `%SystemRoot%`, `%SystemDrive%`, `%ProgramData%` as segment 1 + concrete top-level directory `D:\{dir1}\...` as segment 2).
       - **Guardrail 2**: Linux 19-pattern & Windows 8-pattern sensitive directory denylists (with Windows case-insensitivity and `<Drive>:\Windows\...` / `%SystemDrive%\Windows\...` canonicalized to `%SystemRoot%\...`), checked both lexically and post-symlink via `filepath.EvalSymlinks` on (a) longest existing concrete ancestor, (b) `filepath.Glob` matches, and (c) `filepath.WalkDir` entries under the longest existing concrete prefix when wildcards are present, re-checking resolved paths against both Guardrail 2 and Guardrail 3.
       - **Guardrail 3**: 23-pattern sensitive key/DB/binary/archive extension denylist (`*.ext` and `**/*.ext`) + default `**/*.ext` `exclude` synthesis (`**/*.gz`, `**/*.bz2`, `**/*.xz`, `**/*.zst`, `**/*.zip`, `**/*.tar`, `**/*.key`, `**/*.pem`, `**/*.pfx`, `**/*.p12`, `**/*.kdb`, `**/*.jks`, `**/*.keystore`, `**/.env`, `**/*.env`, `**/*.dbf`, `**/*.mdf`, `**/*.ldf`, `**/*.ndf`, `**/*.sqlite*`, `**/*.db`, `**/*.exe`, `**/*.dll`, `**/*.so`).
       - **Guardrail 4**: Synthesizes `filelog` receiver (`include_file_path: true`) and merges multiple `FileLogSourcePolicy` instances via deterministic sorted set-union of `include` globs.
  2. `components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/provider.go`:
     - Registers `filelog` policy package and deduplicates multiple `FileLogSourcePolicy` instances in `evaluatePolicySet` so multiple `file_log` policies merge their `include` globs into one shared `filelog` receiver rather than overwriting each other in `confmap.Merge` (since `confmap.enableMergeAppendOption` only appends `service::` slices and overwrites component config slices like `receivers::filelog::include`).
  3. `components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/filelog/filelog_policy_test.go` & `provider_test.go`:
     - Comprehensive unit and provider integration tests covering all 4 guardrails, Windows canonicalization, multi-policy merging, and the 1-segment vs. 2-segment subtlety (`/u01/**`, `D:\**`, and `%ProgramData%\**\logs\**` rejected under uniform denylist; `/u01/app/**`, `D:\AppLogs\**`, and `%ProgramData%\Vendor\logs\**` accepted).
- **Exact Verification Command**:
  ```bash
  cd /tmp/prototypes/tab6_filelog_denylist/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider && go test -v ./policies/filelog/... .
  ```
- **Baseline-vs-Change Proof (Non-Tautological Exit Gate)**:
  - `TestBaselineAllowlistVsProposedDenylist` evaluates the 4 Allianz enterprise log paths (`/u01/app/oracle/admin/ORCL/adump/ora_1234.aud`, `/var/adm/messages`, `D:\AppLogs\sep\agi.log`, `%SystemRoot%\System32\LogFiles\Firewall\pfirewall.log`) against `ValidateBaselineAllowlist` (asserting 4/4 fail with allowlist errors) and against `ValidateUniformDenylist` (asserting 4/4 succeed), while asserting that both validators reject sensitive paths (`/etc/shadow`, `C:\Windows\System32\config\SAM`) and traversal (`/var/log/../../etc/passwd`).
  - `TestMultiPolicyDeduplicationBaselineVsMerged` proves that without multi-policy set-union merging, `confmap.Merge` overwrites `receivers::filelog::include` with only the last policy's globs, whereas with `MergeFileLogPolicies` in `provider.go`, all valid `include` globs from multiple `FileLogSourcePolicy` instances are preserved in sorted set-union order.
  - Record full command output AND the companion one-pager (`tango_onepager_6_filelog_enterprise_paths.md`) 1-segment vs. 2-segment boundary audit findings (`D:\**`, `D:\**\*.log`, `/u01/**`, `/data/**`, `/apps/**` rejected as 1-segment; `D:\AppLogs\**`, `/u01/app/**`, `/data/logs/**`, `/apps/sep/**` accepted as 2-segment) in `milestone_1_evidence.md`.

### Milestone 2: Empirical End-to-End `filelogreceiver` + Symlink Attack Test + Live `telemetry.googleapis.com:443` Export (`dashpole-dev`)

- **Deliverables**:
  1. `components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/go.mod` & `go.sum`:
     - Adds `github.com/open-telemetry/opentelemetry-collector-contrib/receiver/filelogreceiver v0.162.0` so the real `filelogreceiver` can be instantiated in end-to-end tests.
  2. `components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/filelog/e2e_filelog_enterprise_test.go`:
     - **Part A (Real `filelogreceiver` + Guardrail 3 `exclude` Negative Control + Guardrail 4 OS DAC + Live GCP Export to `dashpole-dev`)**:
       - Creates a temporary enterprise directory hierarchy containing:
         1. Simulated Oracle audit trail file (`<tmpdir>/u01/app/oracle/admin/ORCL/adump/ora_9876.aud`)
         2. COTS log file (`<tmpdir>/var/adm/messages.log`)
         3. Compressed archive (`<tmpdir>/u01/app/oracle/admin/ORCL/adump/ora_old.aud.gz`)
         4. Private key file (`<tmpdir>/u01/app/oracle/admin/ORCL/adump/server.key`)
         5. Unreadable OS DAC file (`<tmpdir>/u01/app/oracle/admin/ORCL/adump/root_only.aud` with mode `0000`)
       - **Negative Control (`exclude: nil`)**: Runs `filelogreceiver` with `include: ["<tmpdir>/u01/app/oracle/admin/*/adump/*"]` and `exclude: nil`, proving that without Guardrail 3's synthesized `**/*.ext` `exclude` list, `filelogreceiver` ingests `server.key` (`-----BEGIN PRIVATE KEY-----`)!
       - **Proposed Synthesis (Guardrail 3 + Guardrail 4)**: Evaluates two `FileLogSourcePolicy` instances (`include: ["<tmpdir>/u01/app/oracle/admin/*/adump/*"]` and `include: ["<tmpdir>/var/adm/*.log"]`) through `googlecontrolplaneprovider.Retrieve()`, unmarshals the synthesized `receivers::filelog` configuration directly into `filelogreceiver.NewFactory()`, starts the receiver pipeline feeding `googlepolicyprocessor` and `otlpexporter` (authenticated via `googleclientauthextension` to `telemetry.googleapis.com:443` in `dashpole-dev`), verifies that `ora_9876.aud` and `messages.log` are tailed with `log.file.path` populated, verifies that `ora_old.aud.gz` and `server.key` are excluded by Guardrail 3's `**/*.ext` `exclude` list, verifies that `root_only.aud` (`0000`) is blocked by OS DAC without crashing the receiver (Guardrail 4), and confirms `0` export errors to live `telemetry.googleapis.com:443`.
     - **Part B (Empirical Symlink Escape Attack — Baseline Lexical Check vs. Guardrail 2 Post-Symlink Resolution)**:
       - Creates a real symlink `<tmpdir>/u01/app/oracle/admin/ORCL/adump/shadow.log -> /etc/passwd` (tested both as a direct path `<tmpdir>/u01/app/oracle/admin/ORCL/adump/shadow.log` and via a wildcard glob `<tmpdir>/u01/app/oracle/admin/ORCL/adump/*.log`).
       - **Baseline (Symlink Resolution Disabled)**: Shows that a lexical-only denylist accepts `<tmpdir>/u01/app/oracle/admin/ORCL/adump/shadow.log`, and starting `filelogreceiver` actually ingests lines from `/etc/passwd`!
       - **Change (Guardrail 2 Post-Symlink Resolution Enabled)**: Shows that `ValidateUniformDenylist` resolves `<tmpdir>/u01/app/oracle/admin/ORCL/adump/shadow.log` (and wildcard matches via `filepath.WalkDir` + `filepath.EvalSymlinks`) to `/etc/passwd`, rejects the policy with `ErrSensitivePathDenied` matching `/etc/**`, and prevents `filelogreceiver` from starting or reading `/etc/passwd`.
- **Exact Verification Command**:
  ```bash
  cd /tmp/prototypes/tab6_filelog_denylist/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider && go test -v -run TestE2E ./policies/filelog/...
  ```
- **Baseline-vs-Change Proof (Non-Tautological Exit Gate)**:
  - Captures side-by-side empirical execution showing:
    1. Without Guardrail 3's `**/*.ext` `exclude` list, `filelogreceiver` on `<tmpdir>/u01/app/oracle/admin/*/adump/*` ingests `server.key`, whereas with Guardrail 3's synthesized `exclude` list, `server.key` and `ora_old.aud.gz` are skipped.
    2. Lexical-only validation (symlink check disabled) allows the symlinked path and `filelogreceiver` reads `/etc/passwd` (`root:x:0:0:...`), whereas Guardrail 2 post-symlink resolution blocks the policy before `filelogreceiver` can read a single byte.
    3. Real `filelogreceiver` + `googlepolicyprocessor` + live `otlpexporter` export to `telemetry.googleapis.com:443` (`dashpole-dev`) succeeds with `log.file.path` populated on every tailed enterprise log record.
  - Record full command output in `milestone_2_evidence.md`.
