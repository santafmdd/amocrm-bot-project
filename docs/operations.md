# Operations: Long-Running Jobs and Cache Cleanup

## Progress Artifacts

For long-running pipelines we now write a unified progress set in the run directory:

- `progress.json` — latest structured progress snapshot.
- `progress.log` — human-readable timeline.
- `heartbeat.json` — periodic heartbeat with current stage.

Common fields:

- `step_name`
- `current` / `total`
- `percent`
- `elapsed_seconds`
- `eta_seconds`
- `current_item_summary`

Current item summary may include:

- manager / recipient
- date / plan_date
- deal_id / call_id
- model
- stage

## Where It Is Used

- `call_review` (`analyze-period`)
- `daily_control` build
- `week_plan` build
- `weekly_manager_summary` build
- `week_summary` build
- `training_materials` build + write
- `weekly_shared.pipeline_cli` build-cycle
- presentation discovery/transcription stages inside `analyze-period`

## Cache Manager

Command:

```powershell
python -m src.deal_analyzer.cache_manager status --config config/deal_analyzer.call_review.deepseek.realwrite.json
```

Dry-run cleanup (default-safe behavior):

```powershell
python -m src.deal_analyzer.cache_manager cleanup --config config/deal_analyzer.call_review.deepseek.realwrite.json --older-than-days 14 --max-size-gb 20 --dry-run
```

Apply deletion:

```powershell
python -m src.deal_analyzer.cache_manager cleanup --config config/deal_analyzer.call_review.deepseek.realwrite.json --older-than-days 14 --max-size-gb 20 --delete
```

## Safety Rules

- Cleanup is dry-run by default.
- Only cache/media directories are cleanup candidates.
- Run artifacts (`workspace/*/<run_id>`) are not deletion targets.
- Sheets payloads, summaries, reports, and Google Docs links are never touched by cache cleanup.

## Config Fields

Add to deal analyzer config when needed:

```json
{
  "cache_cleanup_enabled": true,
  "cache_retention_days": 14,
  "cache_max_size_gb": 20,
  "progress_heartbeat_seconds": 30
}
```

