# Filesystem Programming Challenge — "Atomic Ingestor"

## Scenario

Your team receives large CSV files dropped into a Linux
directory by various upstream jobs (scp, rsync, cron scripts).
Files can arrive slowly (partial copies), be renamed atomically once done,
or be re-sent/duplicated.

Build a robust, crash-safe ingestor that detects when files are complete,
writes a manifest, and moves them into a date-partitioned warehouse—without
double-processing.

## Requirements

### Watch & Discover

- Monitor a configurable input directory (e.g., /staging).
- Detect complete files and ignore partial writes.
- Support two completion modes:
  - Stability window: size+mtime unchanged for N seconds.
  - Sidecar: presence of `<filename>.ok`.
- Exclude hidden files and temp suffixes like .tmp, .part, .swp.

### Ingest & Move (Exactly-once)

- For each complete file, compute SHA-256 and gather (device, inode, size, mtime).
- Append a JSON Lines record to a manifest under `/manifests/YYYY/MM/DD/HH/manifest.jsonl`:

```json
{
  "path": "...",
  "sha256": "...",
  "device": 2049,
  "inode": 1234567,
  "size": 987654,
  "mtime": 1730546400
}
```

- Move the file to `/warehouse/ingest_date=YYYY-MM-DD/`.
- Use atomic rename when same filesystem; otherwise copy + fsync + rename. Preserve permissions/ownership if possible.
- Provide idempotence and exactly-once semantics:
  - Never re-ingest the same content (even if the filename changes).
  - A durable state (e.g., SQLite or a small kv store) keyed by {device,inode,sha256} is acceptable.

### Reliability

- Crash-safe: on restart, continue without duplicates or lost files.
- Handle log rotation patterns (rename-after-write) without ingesting mid-write.
- Must not hold directory-wide locks that block upstream writers.

### Performance

- Handle directories with up to 1,000,000 files.
- Use inotify/fanotify if available; otherwise degrade to periodic scans without O(n) re-hashing every tick.
- Bounded memory; avoid loading entire directory listings into memory all at once.

### CLI & Ops

Provide a single binary/script with options:

- `--input`, `--warehouse`, `--manifests`
- `--mode [stability|sidecar]`, `--stability-seconds N`
- `--concurrency N`, `--dry-run`, `--state-path`
- `--log-level`

Emit structured logs and basic metrics (files/sec, bytes/sec, queue depth).

## Constraints

- Target OS: Linux. Language: your choice (Go/Rust/Python preferred).
- No external daemons; standard libraries + common OSS libs are fine.
- Assume you cannot use lsof.

## Deliverables (within 90–120 minutes)

1. Source code.
2. Short README explaining:
   - Completion detection approach
   - Exactly-once strategy & crash recovery
   - Same-FS vs cross-FS move behavior
3. A tiny test script that simulates:
   - Slow copy (e.g., write in chunks, sleep)
   - Rename from .tmp → .csv
   - Cross-filesystem move (simulate with a bind mount)
   - Duplicate resend (same content, different name)
