# Deal Analyzer MVP (rules + hybrid + Ollama)

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
- `hybrid` - rules-first analysis + optional short LLM layer.
- `ollama` - local LLM analysis through Ollama chat API.

Rules backend remains available as stable fallback.

## Hybrid backend (safe overlay)
`hybrid` never replaces base rules scoring.

Flow:
1. Build standard rules analysis (mandatory).
2. If Ollama is available, request only a small structured JSON overlay:
   - `loss_reason_short`
   - `manager_insight_short`
   - `coaching_hint_short`
3. On timeout / invalid JSON / model error:
   - run does not crash;
   - base rules result is kept;
   - artifact marks fallback/error with `llm_fallback=true` and `llm_error=true`.

This keeps period runs stable while still adding optional short insights when LLM response is valid.

## Ollama Reliability: Preflight + Repair + Fallback
For `analyze-period` with `analyzer_backend=ollama`:
- CLI runs a short preflight probe (`/api/chat`, small JSON payload).
- If preflight fails, run does not crash: whole period is processed through rules fallback.
- For each deal, parser tries strict JSON first, then safe repair:
  - trims extra text before/after JSON,
  - handles ```json ... ``` fences,
  - extracts balanced JSON object if model adds noise.
- If repair succeeds, deal is counted as `llm_success_repaired`.
- If retry still fails, only that deal falls back to rules (`rules_fallback`), batch continues.

## External Enrichment Sources (read-only)
Analyzer can enrich deals from two external Google Sheets sources:
- client list
- appointment list

Config switches:
- `client_list_enrich_enabled`
- `appointment_list_enrich_enabled`
- `client_list_source_url`
- `appointment_list_source_url`
- `client_list_sheet_name`
- `appointment_list_sheet_name`
- `matching_strategy`
- `fields_mapping`
- `operator_outputs_enabled`

### Matching priority
For each source, matching is applied in this priority:
1. `deal_id` / explicit external id
2. phone
3. email
4. company + contact name
5. company only

Per-deal enrichment fields in export:
- `enrichment_match_status`
- `enrichment_match_source`
- `enrichment_confidence`
- `matched_client_list_row_id`
- `matched_appointment_row_id`

Additional pulled fields:
- from client list: test started/completed/status/comments
- from appointment list: appointment date, assigned by, conducted by, meeting status, transfer/cancel flag

## Operator Outputs
When `operator_outputs_enabled=true`, each deal includes:
- `manager_summary`
- `employee_coaching`
- `employee_fix_tasks` (3-7 tasks)

Tone requirements in generated text:
- business/human internal style
- no mentions of AI/LLM
- short actionable wording

## Period metadata/output artifacts
Period metadata includes:
- `llm_success`
- `llm_success_repaired`
- `llm_fallback`
- `llm_error`
- `backend_requested`
- `backend_effective_summary`

Per-deal backend fields:
- `analysis_backend_requested`
- `analysis_backend_used`
- `llm_repair_applied`
- `llm_error`
- `llm_fallback`
- `loss_reason_short`
- `manager_insight_short`
- `coaching_hint_short`

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

## CLI
Analyze one deal:

```powershell
python -m src.deal_analyzer.cli --config config/deal_analyzer.local.json analyze-deal --input workspace/amocrm_collector/deal_31913530_latest.json
```

Analyze period:

```powershell
python -m src.deal_analyzer.cli --config config/deal_analyzer.local.json analyze-period --period-mode previous_workweek --input workspace/amocrm_collector/collect_period_2026-04-01_2026-04-07_latest.json
```

Опционально можно ограничить размер батча:

```powershell
python -m src.deal_analyzer.cli --config config/deal_analyzer.local.json analyze-period --input workspace/amocrm_collector/collect_period_2026-04-01_2026-04-07_latest.json --limit 20
```

### Period artifacts
`analyze-period` теперь дополнительно создает batch run-папку:
- `workspace/deal_analyzer/period_runs/<run_timestamp>/deals/deal_<id>.json` — per-deal snapshot+analysis artifacts;
- `workspace/deal_analyzer/period_runs/<run_timestamp>/summary.json` — агрегированный итог запуска.
- `workspace/deal_analyzer/period_runs/<run_timestamp>/summary.md` — человекочитаемый итог run.
- `workspace/deal_analyzer/period_runs/<run_timestamp>/top_risks.json` — быстрый риск-лист по сделкам.
- `workspace/deal_analyzer/period_runs/<run_timestamp>/manager_brief.md` — короткий управленческий бриф для быстрого чтения.

`summary.json` включает:
- `run_timestamp`
- `backend_requested`
- `analysis_backend_used` (+ counts по backend used)
- `total_deals_seen`
- `total_deals_analyzed`
- `deals_failed`
- `artifact_paths`
- `score_aggregates` (`min/max/avg`)
- `risk_flags_counts`

`summary.md` — это операторский markdown-срез с ключевыми секциями:
- run info;
- score aggregates;
- top risk flags;
- top 10 risky deals;
- top 10 highest score deals;
- пометка `[warnings]` для сделок со snapshot warnings.

`top_risks.json` — массив по сделкам для быстрого разбора риска/приоритезации.

`manager_brief.md` — управленческий компактный отчет:
- объем (просмотрено/проанализировано/упало),
- 5 основных риск-паттернов,
- отдельный блок `Qualified loss / market mismatch`,
- 5 сделок внимания,
- 5 сделок с лучшим потенциалом,
- короткий блок "что делать дальше".

### Rules quality slice (latest)
- Rules scoring теперь учитывает не только demo/brief, но и контекст из CRM: notes/tasks/tags, контактные и company-данные, long-call сигналы.
- Сценарии `reasoned loss` (осознанный отказ/market mismatch) помечаются отдельно как `qualified_loss:*`, а не смешиваются с пустыми hygiene-кейсами.
- В period markdown-отчетах `qualified_loss` выводится отдельной секцией.

### Policy-aware recommendations
- `qualified_loss` (осознанный отказ/market mismatch): рекомендации смещаются в фиксацию причины, сегментный вывод и снятие лишнего follow-up давления.
- `evidence_context` gap: приоритет — заполнение CRM-контекста (notes, pain, business task, evidence).
- `process_hygiene` gap (без qualified_loss): сохраняются классические follow-up/next-step/probability рекомендации.
- `closed_lost + evidence_context gap`: рекомендации про восстановление причины потери и корректную классификацию closeout, без дефолтного прогрева/демо.
- `won`: безопасный минимум по post-sale/handoff (без агрессивных pipeline pressure рекомендаций).
- Для all-loss batch manager artifacts рендерятся безопасно: блок потенциала не вводит в заблуждение и показывает fallback по закрытым кейсам.

Это технический batch slice для analyzer и snapshot pipeline, не weekly layer и не writer layer.

## Vertical Slice: Snapshot -> Analysis -> JSON
Минимальный сценарий для одной сделки/снапшота:

1. Получить prepared snapshot (например, из `build-call-snapshot`) **или** взять collector input и указать `--deal-id`.
2. Запустить:

```powershell
python -m src.deal_analyzer.cli --config config/deal_analyzer.local.json analyze-snapshot --input workspace/deal_analyzer/call_snapshot_deal_latest.json
```

или

```powershell
python -m src.deal_analyzer.cli --config config/deal_analyzer.local.json analyze-snapshot --input workspace/amocrm_collector/collect_period_2026-04-01_2026-04-07_latest.json --deal-id 31913530
```

Результат:
- JSON artifact в `workspace/deal_analyzer/analyze_snapshot_<timestamp>.json`;
- latest copy: `workspace/deal_analyzer/analyze_snapshot_latest.json` (если не задан `--no-latest`);
- в логе фиксируется `backend_requested` и фактический `analysis_backend_used`.

Покрыто сейчас:
- стабильный snapshot (включая partial warnings);
- запуск текущего backend (`rules`/`ollama`) поверх `snapshot.crm`;
- сохранение итогового JSON.

Пока не покрыто:
- запись результата в Google Sheets;
- UI/оркестрация внешних запусков.

## Notes
- Enrichment is read-only (no write-back to Google Sheets).
- Weekly/refusals/analytics writer flows are untouched.

## Data Quality / Owner Ambiguity Guardrails
- Analyzer now marks interpretation reliability with:
  - `data_quality_flags`
  - `owner_ambiguity_flag`
  - `crm_hygiene_confidence` (`high|medium|low`)
  - `analysis_confidence` (`high|medium|low`)
- Conservative owner ambiguity heuristic is used when CRM owner differs from enriched meeting actors (`enriched_conducted_by` / `enriched_assigned_by`) or attribution is limited.
- For low-confidence deals, manager/employee outputs switch to caution-first wording:
  - no accusatory assumptions about manager inactivity;
  - focus on fact restoration, owner attribution check, and CRM cleanup.
- Artifacts expose these markers in per-deal JSON and period markdown/csv outputs.

