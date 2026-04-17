# Deal Analyzer MVP (rules + Ollama)

## Scope
Deal analyzer reads collector output and builds local analytical result.

Current constraints:
- no writes to Google Sheets;
- no UI;
- no weekly/refusals/analytics writer changes;
- exports only to `workspace/deal_analyzer`.

## Backends
`analyzer_backend` in config:
- `rules` - deterministic rule-based analyzer (default).
- `ollama` - local LLM analysis through Ollama chat API.

Rules backend remains available as stable fallback.

## Period Modes
Supported `period_mode` values:
- `smart_manager_default`
- `current_week_to_date`
- `previous_calendar_week`
- `previous_workweek`
- `custom_range`

Semantics:
- `current_week_to_date`: Monday of current week .. run date.
- `previous_calendar_week`: previous week Monday..Sunday.
- `previous_workweek`: previous week Monday..Friday.
- `custom_range`: explicit `date_from` + `date_to`.
- `smart_manager_default`:
  - Saturday/Sunday run -> `current_week_to_date`
  - Monday-Friday run -> `previous_workweek`

Recommended operation mode:
- if bot usually runs on weekend, keep `smart_manager_default`.
- if operator needs fixed reporting, pass explicit `--period-mode`.

## Public Export Visibility
Config controls public metadata:
- `hide_executed_at_from_public_exports`
- `executed_at_visibility` (`internal_only` | `public`)
- `period_label_mode` (`period_only` | `period_and_as_of`)

Public exports (json/md/csv) always include:
- `period_start`
- `period_end`
- `public_period_label`
- `as_of_date`

`executed_at` is hidden from public exports by default.

## Future Enrich Contract (v2)
Prepared config flags (not active at runtime yet):
- `client_list_enrich_enabled`
- `appointment_list_enrich_enabled`
- `client_list_source_name`
- `appointment_list_source_name`

No Google Drive/Sheets enrich is performed in current MVP.

## Config
`config/deal_analyzer.local.json` example:

```json
{
  "output_dir": "workspace/deal_analyzer",
  "analyzer_backend": "rules",
  "ollama_base_url": "http://127.0.0.1:11434",
  "ollama_model": "gemma4:e4b",
  "ollama_timeout_seconds": 60,
  "style_profile_name": "manager_ru_v1",
  "period_mode": "smart_manager_default",
  "custom_date_from": "",
  "custom_date_to": "",
  "period_label_mode": "period_only",
  "hide_executed_at_from_public_exports": true,
  "executed_at_visibility": "internal_only",
  "client_list_enrich_enabled": false,
  "appointment_list_enrich_enabled": false,
  "client_list_source_name": "client_list_v1",
  "appointment_list_source_name": "appointments_v1"
}
```

## CLI
Analyze one deal:

```powershell
python -m src.deal_analyzer.cli --config config/deal_analyzer.local.json analyze-deal --input workspace/amocrm_collector/deal_31913530_latest.json
```

Analyze period with default smart mode:

```powershell
python -m src.deal_analyzer.cli --config config/deal_analyzer.local.json analyze-period --input workspace/amocrm_collector/collect_period_2026-04-01_2026-04-07_latest.json
```

Analyze period with explicit mode:

```powershell
python -m src.deal_analyzer.cli --config config/deal_analyzer.local.json analyze-period --period-mode previous_workweek --input workspace/amocrm_collector/collect_period_2026-04-01_2026-04-07_latest.json
```

Analyze period with custom range:

```powershell
python -m src.deal_analyzer.cli --config config/deal_analyzer.local.json analyze-period --period-mode custom_range --date-from 2026-04-01 --date-to 2026-04-07 --input workspace/amocrm_collector/collect_period_2026-04-01_2026-04-07_latest.json
```

## Ollama notes
- Start local Ollama service before running analyzer.
- If Ollama is unavailable, CLI returns clear error with connection details.
- Prompt is structured and explicitly forbids inventing missing facts.
