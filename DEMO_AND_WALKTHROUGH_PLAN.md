# Demo & Walkthrough Plan: Enterprise COTS & Database Log Paths in `FileLogSourcePolicy` (Tab 6)

**Author**: dashpole@google.com
**Companion Design Doc**: `tango_onepager_6_filelog_enterprise_paths.md`
**Target Workspace**: `/tmp/prototypes/tab6_filelog_denylist/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider`
**Branch**: `proto/tango-tab6-filelog-denylist`

---

## 1. 60-Second Stakeholder Executive Pitch

- **Why This Matters**: During the Allianz Google SecOps / Bindplane evaluation, three `High` findings (`#33` Oracle OFA audit logs under `/u01/app/oracle/admin/*/adump/*.aud`, `#32` COTS application logs under `/var/adm/**`, and `#76` Windows secondary drives `D:\AppLogs\**` and `%SystemRoot%\System32\LogFiles\**`) were blocked by `FileLogSourcePolicy`'s hardcoded 5-root allowlist in `go/tango-preview-policies`.
- **What We Built & Tested**: We implemented `FileLogSourcePolicy` (`policies/filelog/filelog_policy.go`) and multi-policy set-union synthesis in `googlecontrolplaneprovider/provider.go` enforcing a **4-Guardrail Uniform Denylist & Structural Glob Validator** (structural `>= 2`-segment breadth check, 19 Linux & 8 Windows sensitive directory denylists with pre- and post-symlink resolution, 23 sensitive key/DB/binary/archive extension denylists with auto-injected `**/*.ext` `exclude` globs, and OS POSIX DAC least privilege).
- **Decision / Next Step**: Adopt the 4-Guardrail Uniform Denylist in `go/tango-preview-policies` and update the 1-segment vs. 2-segment glob examples in Tab 6 (`D:\AppLogs\**` and `/u01/app/**` instead of `D:\**` and `/u01/**`).

---

## 2. Option A: Reproducible Live Terminal Walkthrough

### Step 1: Compare the 5-Root Hardcoded Allowlist vs. Proposed 4-Guardrail Uniform Denylist
Run the unit test suite comparing `ValidateBaselineAllowlist` (which rejects all 4 Allianz enterprise paths) against `ValidateUniformDenylist` (which accepts all 4 while blocking 1-segment full-disk globs, path traversal, all 19 Linux and 8 Windows sensitive directories, and all 23 sensitive extensions):

```bash
cd /tmp/prototypes/tab6_filelog_denylist/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider
go test -v -run "TestBaselineAllowlistVsProposedDenylist|TestGuardrail" ./policies/filelog/...
```

**Expected Output Highlights**:
- `TestBaselineAllowlistVsProposedDenylist`: All 4 Allianz enterprise paths (`/u01/app/oracle/admin/ORCL/adump/ora_1234.aud`, `/var/adm/messages`, `D:\AppLogs\sep\agi.log`, `%SystemRoot%\System32\LogFiles\Firewall\pfirewall.log`) fail `ValidateBaselineAllowlist` (`ErrNotInBaselineAllowlist`) and pass `ValidateUniformDenylist`.
- `TestGuardrail1_StructuralGlobBreadthAndOnePagerBoundary`: Rejects 1-segment globs (`/*`, `/**`, `/u01/**`, `C:\*`, `C:\**`, `D:\**`, `D:\**\*.log`, `%ProgramData%\**\logs\**`) with `ErrInsufficientLeadingSegments` and accepts 2-segment globs (`/u01/app/**`, `/var/adm/**`, `D:\AppLogs\**`, `%ProgramData%\Vendor\logs\**`, `%SystemRoot%\System32\LogFiles\**`).
- `TestGuardrail2_SensitiveDirectoryDenylist_All19LinuxAnd8WindowsPatterns` & `TestGuardrail3_SensitiveExtensionDenylistAndSynthesizedExclude`: Rejects all 19 Linux sensitive directories, all 8 Windows sensitive directories (including `C:\Windows\System32\config\SAM` via `%SystemRoot%` canonicalization and partial wildcards like `.ss*`, `Micro*`), and all 23 sensitive extensions (including partial wildcards like `*.ke*`, `*.db*`).

### Step 2: Verify Multi-Policy Set-Union Deduplication in `googlecontrolplaneprovider`
Run the provider integration test showing multiple `FileLogSourcePolicy` instances merged into a shared `receivers::filelog` (`include_file_path: true`) and `service::pipelines::logs/filelog` while an invalid `/etc/shadow` policy is rejected and logged via `PolicyEvaluateErrorEvent`:

```bash
cd /tmp/prototypes/tab6_filelog_denylist/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider
go test -v -run TestRetrieve_MultipleFileLogPoliciesMergedAndInvalidSkipped .
```

### Step 3: Run the End-to-End `filelogreceiver` + Live `telemetry.googleapis.com:443` Export (`dashpole-dev`) & Symlink Escape Attack Test
Run the empirical end-to-end test suite:

```bash
cd /tmp/prototypes/tab6_filelog_denylist/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider
go test -v -run TestE2E ./policies/filelog/...
```

**Expected Output Highlights**:
1. `[Baseline Negative Control (exclude: nil)]`: Ingests `3` records including `server.key` (`-----BEGIN PRIVATE KEY----- SECRET_KEY_MATERIAL_DO_NOT_INGEST`) and `ora_old.aud.gz`.
2. `[Proposed Synthesis + Live Export]`: Synthesizes `receivers::filelog` from `oracle_ofa_policy` + `cots_var_adm_policy`, excludes `server.key` and `ora_old.aud.gz` via Guardrail 3 (`**/*.key`, `**/*.gz`), skips `0000`-mode `root_only.aud` via OS POSIX DAC (Guardrail 4), populates `log.file.path`, and exports the `2` valid enterprise log records (`ora_9876.aud` and `/var/adm/messages.log`) to live `telemetry.googleapis.com:443` in `dashpole-dev` with `0` errors.
3. `[Baseline (Symlink Resolution Disabled)]` vs. `[Proposed (Post-Symlink Resolution Enabled)]`: Shows that without symlink resolution, `filelogreceiver` leaks `61` lines from `/etc/passwd` via `<tmpdir>/u01/app/oracle/admin/ORCL/adump/shadow.log -> /etc/passwd`, whereas Guardrail 2 post-symlink resolution blocks both direct path and `*.log` wildcard policies with `ErrSensitivePathDenied: "/etc/passwd" matches /etc/**`.

---

## 3. Option B: AI-Narrated Walkthrough Video Storyboard & Script

*(Note: All spoken narration text below contains zero raw symbols, backticks, or URLs so it can be fed directly to TTS generation.)*

| Scene | Visual / Terminal Content | Spoken Narration Script |
| :--- | :--- | :--- |
| **Scene 1: The Enterprise Log Path Problem (0:00–0:20)** | Side-by-side view of `tango_onepager_6_filelog_enterprise_paths.md` and `TestBaselineAllowlistVsProposedDenylist` showing the 5 hardcoded OS log roots rejecting Oracle OFA audit logs, Linux var adm messages, and Windows secondary drive logs. | During the Allianz Bindplane evaluation, three high priority findings showed that enterprise databases and commercial off the shelf applications write logs outside standard operating system log folders. Under the initial five root allowlist, Oracle audit trail logs under u zero one, Linux system logs under var adm, and Windows logs on secondary drives or System32 LogFiles were all rejected. |
| **Scene 2: The Four-Guardrail Uniform Denylist Architecture (0:20–0:50)** | Code walkthrough of `policies/filelog/filelog_policy.go` highlighting Guardrail 1 (two concrete leading segments), Guardrail 2 (nineteen Linux and eight Windows sensitive directory rules plus symlink resolution), Guardrail 3 (twenty-three denied extensions and double-star exclude synthesis), and Guardrail 4 (set-union deduplication in `provider.go`). | Instead of a brittle allowlist or host specific environment variables, our prototype implements a fleet uniform four guardrail validator in the Google Control Plane Provider. Guardrail one requires at least two concrete leading directory segments before any wildcard, blocking full disk walks like slash u zero one double star or D drive double star, while allowing slash u zero one slash app double star and D drive AppLogs double star. Guardrail two blocks nineteen Linux and eight Windows sensitive directories both before and after resolving symlinks. Guardrail three blocks twenty-three sensitive key, database, binary, and archive extensions and automatically injects double star exclude patterns into the filelog receiver. Guardrail four merges multiple file log policies via set union into a shared filelog pipeline. |
| **Scene 3: Live End-to-End Filelog Receiver & Google Cloud Telemetry Export (0:50–1:20)** | Terminal execution of `TestE2E_FileLogEnterpriseLogs_ExcludeNegativeControl_OSDAC_AndLiveGCPExport` showing the negative control ingesting a private key file when excludes are omitted, followed by the synthesized policy excluding the key and archive files, respecting operating system file permissions, stamping log file path, and exporting two records to Google Cloud Telemetry in dashpole dev. | In our first end to end test, we first run a negative control without Guardrail three's synthesized exclude list, proving that a wildcard over an Oracle audit directory would otherwise ingest a private key file and a compressed archive. Next, we evaluate two enterprise file log policies through the Google Control Plane Provider. The synthesized filelog receiver tails the Oracle audit log and the var adm log with log file path populated, excludes the private key and compressed archive, skips an unreadable zero permission file via operating system access controls, and exports both enterprise log records live to Google Cloud Telemetry with zero errors. |
| **Scene 4: Empirical Symlink Escape Attack & Key Learnings (1:20–1:50)** | Terminal execution of `TestE2E_SymlinkEscapeAttack_BaselineLexicalVsProposedPostSymlinkResolution` showing 61 lines of `/etc/passwd` leaked when symlink resolution is disabled versus immediate rejection with `ErrSensitivePathDenied` when Guardrail 2 post-symlink resolution is enabled, followed by the summary table from `PROTOTYPE_LEARNINGS.md`. | Finally, we test a real symlink escape where shadow dot log inside the Oracle audit directory points to etc passwd. With symlink resolution disabled, lexical validation passes and the filelog receiver leaks sixty-one lines from etc passwd. With Guardrail two's post symlink resolution enabled, the validator resolves the symlink target to etc passwd and blocks both direct path and wildcard policies before the receiver can start. Our prototype also uncovered five concrete improvements for the design doc, including aligning the prose examples with the two segment breadth rule and excluding systemd's var log README symlink. |
