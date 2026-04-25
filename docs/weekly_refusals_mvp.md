# Weekly Refusals MVP

## Scope
- Separate flow from `analytics_sales`: uses amoCRM **Events list** (`events_list` page type).
- No AI at this stage.
- Goal: deterministic capture + structured dataset + aggregated refusal tables.

## Runtime Flow
1. Open `events/list` page.
2. Open filter panel.
3. Apply filters:
   - date mode / period (or custom from-to)
   - managers empty
   - entity kind
   - event type
   - pipeline
   - status before
   - status after (`Закрыто и не реализовано`)
4. Read event table rows.
5. Parse and aggregate:
   - counts by `status_before`
   - counts by `status_after`
   - preserve `deal_id` / `deal_url`
6. Save compiled artifact in `exports/compiled/weekly_refusals_<report_id>_<ts>.json`.
7. Write block through API writer (`src/writers/weekly_refusals_block_writer.py`) or dry-run plan only.

## Profiles
MVP includes 4 independent report profiles:
- `weekly_refusals_weekly_2m`
- `weekly_refusals_weekly_long`
- `weekly_refusals_cumulative_2m`
- `weekly_refusals_cumulative_long`

`cumulative` is recalculated from source range each run (no additive merge with old sheet values).

## Dry-run
Use `--writer-layout-api-dry-run` or `--writer-layout-dry-run` with these profiles to prevent sheet updates while keeping capture + artifacts.

## Current Limitations
- UI selectors for events list are deterministic MVP-level and may require selector tuning per amoCRM UI changes.
- No AI deal analysis yet.
- No deep card crawling yet.

## Report IDs (Current)

Weekly refusals profiles available now:
- `weekly_refusals_weekly_2m`
- `weekly_refusals_weekly_long`
- `weekly_refusals_cumulative_2m`
- `weekly_refusals_cumulative_long`
- `weekly_refusals_example` (alias for smoke validation; same routing as weekly_2m)

### Smoke Dry-run Command
```bash
python -m src.run_profile_analytics --report-id weekly_refusals_example --writer-layout-api-dry-run --browser-backend openclaw_cdp --tag-selection-mode script
```

## Update (2026-04-15): Weekly Period Modes

Weekly refusals runtime period can now be controlled by config or CLI without code edits.

Config-level strategy (`filters.period_strategy`):
- `current_week`
- `previous_week`
- `auto_weekly`
- `monday_current_else_previous`
- `manual_range`

CLI overrides (current run only):
- `--weekly-period-strategy`
- `--weekly-period-mode`
- `--weekly-date-from`
- `--weekly-date-to`


## Update (2026-04-16): Cumulative Write Semantics (Explicit)

Cumulative profiles use **recompute from source** semantics:
- parser aggregates rows from current source capture range,
- writer overwrites target counts with recomputed totals,
- writer does not read existing sheet numbers to add on top.

So cumulative run is idempotent for the same source range (no double growth on repeated execution).

Validation markers:
- compiled artifact contains `mode` and `writer_mode_semantics=recompute_from_source`,
- write summary contains `writer_mode` and `writer_mode_semantics`.


## Update (2026-04-16): Cumulative Strategy Matrix

Supported modes now:
- `weekly`: overwrite target counts by current source slice.
- `cumulative + recompute_from_source`: overwrite with recomputed source totals.
- `cumulative + add_existing_values`: add incoming weekly counts to existing sheet counts by canonical key.

For `add_existing_values` mode:
- required deterministic `period_key`;
- duplicate live apply for same period is blocked (idempotency guard);
- repeated dry-run is safe and does not write guard state.
- `period_key` now includes absolute date range (`YYYY-MM-DD..YYYY-MM-DD`) even for labels like `За эту неделю`, so different weeks do not collide by text label.
- before any reapply, run dry-run preview first; `cumulative_force_reapply=true` is unsafe unless idempotent delta logic is explicitly enabled.

Writer summary fields to inspect:
- `writer_mode`
- `writer_mode_semantics`
- `cumulative_write_strategy`
- `period_key`
- `duplicate_period_guard`
