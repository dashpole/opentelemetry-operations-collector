# Post-Prototype Learnings & Design-Discrepancy Report: Enterprise COTS & Database Log Paths in `FileLogSourcePolicy` (Tab 6)

**Author**: dashpole@google.com
**Companion Design Doc**: `tango_onepager_6_filelog_enterprise_paths.md` (Tab 6 — Project Tango Bindplane Findings `#33`, `#32`, `#76`)
**Target Repository**: `opentelemetry-operations-collector` (`components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider`)
**Branch**: `proto/tango-tab6-filelog-denylist`

---

## 1. Executive Summary: What We Proved

This prototype replaced `FileLogSourcePolicy`'s hardcoded 5-root allowlist (`/var/log/**`, `/opt/**/logs/**`, `/srv/**/logs/**`, `%ProgramData%\**\logs\**`, `%SystemDrive%\inetpub\logs\**`) with a deterministic **4-Guardrail Uniform Denylist & Structural Glob Validator** (`policies/filelog/filelog_policy.go`) in `googlecontrolplaneprovider`:

1. **Unblocks All 4 Allianz Enterprise COTS & Database Log Archetypes (`#33`, `#32`, `#76`)**:
   - Whereas `ValidateBaselineAllowlist` rejected 4/4 enterprise paths (`/u01/app/oracle/admin/ORCL/adump/ora_1234.aud`, `/var/adm/messages`, `D:\AppLogs\sep\agi.log`, `%SystemRoot%\System32\LogFiles\Firewall\pfirewall.log`), `ValidateUniformDenylist` accepted 4/4 while preserving fleet-wide uniform policy validity across both the Control Plane API and the agent.
2. **Real `filelogreceiver` + Live OTLP/gRPC Export to `telemetry.googleapis.com:443` (`dashpole-dev`)**:
   - Synthesized a shared `receivers::filelog` (`include_file_path: true` + `DefaultExcludeGlobs`) and `service::pipelines::logs/filelog` across multiple `FileLogSourcePolicy` instances, tailed a simulated Oracle `.aud` audit log and `/var/adm/messages.log` with `log.file.path` populated, excluded `server.key` and `ora_old.aud.gz` (verified against a negative control where `exclude: nil` ingested `server.key`), skipped a `0000`-mode file via OS POSIX DAC without crashing, and exported the `2` valid enterprise log records to live `telemetry.googleapis.com:443` in `dashpole-dev` with `0` export errors.
3. **Empirical Symlink Escape Defense (`.../adump/shadow.log -> /etc/passwd`)**:
   - Proved that lexical-only validation (`DisableSymlinkResolution: true`) allows `<tmpdir>/u01/app/oracle/admin/ORCL/adump/shadow.log -> /etc/passwd` and causes `filelogreceiver` to ingest `61` lines of `/etc/passwd` (`root:...`), whereas Guardrail 2's post-symlink resolution (`filepath.EvalSymlinks` across concrete ancestors, `filepath.Glob` matches, and `filepath.WalkDir` symlink entries/subdirectories) blocks both direct path and wildcard glob (`*.log`, `**/*.conf`) policies with `ErrSensitivePathDenied` (`matches /etc/**`).

---

## 2. What Surprised Us & Design-Doc Discrepancies Discovered

### Discrepancy 1: The 1-Segment vs. 2-Segment Glob Boundary in `tango_onepager_6_filelog_enterprise_paths.md`
- **What Happened**:
  - Guardrail 1 requires `>= 2` concrete non-wildcard leading segments before any wildcard (`/{seg1}/{seg2}/{leaf_or_glob}` on Linux; `{DriveOrEnvRoot}\{dir1}\{leaf_or_glob}` on Windows) to prevent full-disk I/O walks.
  - However, several prose examples in `tango_onepager_6_filelog_enterprise_paths.md` inadvertently cited 1-segment globs:
    - **Line 14**: Listed `/u01/**, /data/**, /apps/**` alongside `/var/adm/**`. While `/var/adm/**` has 2 concrete leading directory segments (`var`, `adm`), `/u01/**`, `/data/**`, and `/apps/**` have only **1** concrete leading directory segment before `**` and are rejected by Guardrail 1 (whereas `/u01/app/**`, `/data/logs/**`, and `/apps/sep/**` have 2 concrete leading segments and pass).
    - **Line 15, Line 82, and Line 85**: Listed `D:\**\*.log`, `E:\**\*.txt`, and `D:\**` (which have only **1** concrete leading segment, the drive root `D:` or `E:`, before `**`, and are rejected by Guardrail 1 just like `C:\**`), whereas **Line 21** properly wrote `D:\AppLogs\**` (2 concrete leading segments: drive `D:` + directory `AppLogs`).
    - **Line 38**: Stated `Require >= 2 concrete leading directory segments before any "**" wildcard (rejects "/*", "/**", "C:\*", "C:\**"...)`. To reject single-star `/*`, `/u01/*`, and `C:\*`, the rule must require `>= 2` concrete leading segments before **any wildcard** (`*`, `**`, `?`, `[`), not just `**`.
    - **Baseline Allowlist Interaction (`%ProgramData%\**\logs\**`)**: The baseline allowlist entry `%ProgramData%\**\logs\**` has `**` immediately after `%ProgramData%` (only 1 concrete segment before `**`). Under Guardrail 1, Windows `ProgramData` policies must specify at least the vendor/application top-level directory (`%ProgramData%\Symantec\logs\**` or `%ProgramData%\Vendor\**`).

### Discrepancy 2: `doublestar.PathMatch` Requires `**/*.ext` (Not Bare `*.ext`) in Synthesized `exclude` Globs
- **What Happened**:
  - In `opentelemetry-collector-contrib/pkg/stanza/fileconsumer/matcher/internal/finder`, `pathExcluded(excludes, path)` checks each full absolute path returned by `doublestar.FilepathGlob` using `doublestar.PathMatch(exclude, fullPath)`.
  - In `doublestar/v4`, a single `*` does not cross path separators (`/` or `\`), so bare `exclude: ["*.gz", "*.key"]` returns `false` on `/u01/app/oracle/admin/ORCL/adump/ora_old.aud.gz` and `/u01/app/oracle/admin/ORCL/adump/server.key`.
  - **Fix Applied & Recommended Spec Update**: Synthesized `exclude` patterns on `receivers::filelog` must be prefixed with `**/` (`**/*.gz`, `**/*.key`, `**/*.pem`, `**/*.dbf`, `**/*.db`, etc.). Update Guardrail 3 in `tango_onepager_6_filelog_enterprise_paths.md` (Lines 54–56) to note that `Validate()` blocks `*.ext` / `**/*.ext` (including partial extension wildcards like `*.ke*`, `*.k?y`, `*.db*`, `*.sqli*`) and `Evaluate()` auto-injects `exclude: ["**/*.key", "**/*.pem", ..., "**/*.gz"]` on the synthesized `filelog` receiver.

### Discrepancy 3: Windows `<Drive>:\Windows` Canonicalization & Case-Insensitivity
- **What Happened**:
  - Guardrail 2's Windows denylist in `tango_onepager_6_filelog_enterprise_paths.md` (Lines 47–50) writes `%SystemRoot%\System32\config\**`, `%SystemRoot%\NTDS\**`, `%SystemRoot%\System32\winevt\**`, and `%SystemRoot%\repair\**`.
  - A customer or attacker writing `C:\Windows\System32\config\SAM`, `c:\windows\ntds\ntds.dit`, or `%SystemDrive%\Windows\repair\sam` would bypass a naive prefix check against `%SystemRoot%`.
  - **Fix Applied & Recommended Spec Update**: Canonicalize `<Drive>:\Windows\...` and `%SystemDrive%\Windows\...` to `%SystemRoot%\...` and lowercase all segments prior to Windows denylist evaluation (and match partial segment wildcards like `%SystemRoot%\System32\conf*\SAM` or `C:\ProgramData\Micro*\Crypto\**`).

### Discrepancy 4: Real-World `systemd` `/var/log/README -> /usr/share/doc/systemd/README.logs` Symlink on Linux Hosts
- **What Happened**:
  - When running post-symlink validation of `include: ["/var/log/**"]` on a real gLinux/Debian systemd host, `filepath.WalkDir("/var/log")` encountered `/var/log/README -> ../../usr/share/doc/systemd/README.logs`, a factory symlink created by systemd's `/usr/lib/tmpfiles.d/legacy.conf`.
  - Because `/usr/**` is in Guardrail 2's Linux Sensitive Directory Denylist, `/var/log/README` resolved to `/usr/share/doc/systemd/README.logs` and caused `/var/log/**` to fail validation!
  - **Fix Applied**: Added `**/README` to `DefaultExcludeGlobs` and skipped files already excluded by `DefaultExcludeGlobs` during wildcard symlink directory scans.

### Discrepancy 5: Multi-Policy `confmap.Merge` Overwrites `receivers::filelog::include` Without Explicit Set-Union Merging
- **What Happened**:
  - `googlecontrolplaneprovider/provider.go` enables `confmap.enableMergeAppendOption`, which only appends and deduplicates `service::extensions`, `service::**::receivers`, and `service::**::exporters` slices. Component-level config slices like `receivers::filelog::include` still use last-writer-wins semantics during `confmap.Merge`.
  - **Fix Applied**: `provider.go` collects all valid `*filelog.FileLogSourcePolicy` instances in `evaluatePolicySet` and merges them via `filelog.MergePolicies` (deterministic sorted set-union of `Include` globs) before synthesizing `receivers::filelog` and `service::pipelines::logs/filelog`.

---

## 3. Recommended Surgical Updates to `tango_onepager_6_filelog_enterprise_paths.md`

| Section / Line in `tango_onepager_6_filelog_enterprise_paths.md` | Current Text | Recommended Replacement |
| :--- | :--- | :--- |
| **Context and Motivation — Finding `#32` (L14)** | `(such as /var/adm/**, /u01/**, /data/**, /apps/**, or vendor paths that do not contain a literal /logs/ segment)` | `(such as /var/adm/**, /u01/app/**, /data/logs/**, /apps/sep/**, or vendor paths that do not contain a literal /logs/ segment)` |
| **Context and Motivation — Finding `#76` (L15)** | `dedicated secondary drives (D:\**\*.log, E:\**\*.txt) or in %SystemRoot%\System32\LogFiles\**` | `dedicated secondary drives (D:\AppLogs\**\*.log, E:\ServiceLogs\**\*.txt) or in %SystemRoot%\System32\LogFiles\**` |
| **Proposed Design — Guardrail 1 (L38–L39)** | `- Require >= 2 concrete leading directory segments before any "**" wildcard (rejects "/*", "/**", "C:\*", "C:\**" to prevent full-disk I/O walks)` | `- Require >= 2 concrete non-wildcard leading segments before any "*" or "**" wildcard (/{dir1}/{dir2}/... on Linux; {DriveOrEnvRoot}\{dir1}\... on Windows; rejects "/*", "/**", "/u01/**", "C:\*", "C:\**", "D:\**" to prevent full-disk I/O walks)` |
| **Proposed Design — Guardrail 2 (L47–L50)** | `- Windows: %SystemRoot%\System32\config\**, ...` | `- Windows (case-insensitive; <Drive>:\Windows and %SystemDrive%\Windows canonicalized to %SystemRoot%): %SystemRoot%\System32\config\**, ...` |
| **Proposed Design — Guardrail 3 (L53–L56)** | `- Rejects: *.key, *.pem, ... *.tar` | `- Rejects include globs targeting: *.key, *.pem, ... *.tar (including partial extension wildcards like *.ke*, *.db*) and auto-populates exclude: ["**/*.key", "**/*.pem", ..., "**/*.gz", "**/README"] on the synthesized filelog receiver` |
| **Proposed Design — Guardrail 4 (L61)** | `- Synthesize filelog Receiver (include_file_path: true)` | `- Merge multiple FileLogSourcePolicy instances via sorted set-union of include globs into a shared filelog receiver (include_file_path: true)` |
| **Alternatives Considered Table (L82, L85)** | `Supports /u01/app/oracle/.../adump/*.aud (#33) & D:\** (#76)` and `secondary Windows drives (D:\**, #76)` | `Supports /u01/app/oracle/.../adump/*.aud (#33) & D:\AppLogs\** (#76)` and `secondary Windows drives (D:\AppLogs\**, #76)` |

---

## 4. Productionization Gap Ledger

| ID | Gap / PoC Shortcut | Production Hardening Requirement |
| :--- | :--- | :--- |
| **G1** | **Continuous Post-Startup Symlink Creation (TOCTOU inside `stanza/fileconsumer`)** | `ValidateUniformDenylist` resolves symlinks on existing paths, ancestors, and directory entries when a policy is loaded/evaluated. If a symlink `shadow.aud -> /etc/shadow` is created inside `/u01/app/oracle/admin/ORCL/adump/` *after* policy evaluation while a wildcard `*.aud` is active, upstream `stanza/fileconsumer` (`finder.go`) should be hardened with a post-`EvalSymlinks` callback filter (or `O_NOFOLLOW` on leaf open) so newly created symlinks into denylisted directories are rejected on every poll cycle. |
| **G2** | **Control Plane API Shared Validator Package** | Export `ValidateUniformDenylist` (with `checkSymlinks=false` for pure static validation at the Control Plane API and `checkSymlinks=true` on the GBOC agent) from `pkg/googlepolicy/filelog` so `telemetrymanagement.googleapis.com` and `googlecontrolplaneprovider` execute the exact same Go validation code. |
| **G3** | **Windows MSI Service Account Hardening (`NT SERVICE\otelcol-google`)** | As noted in Tab 6's Upfront Trade-Offs, Windows collectors running as `LocalSystem` bypass standard NTFS ACLs; pair the Windows denylist with running the Windows collector service under `NT SERVICE\otelcol-google`. |
