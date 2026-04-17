# Deal Analyzer Enrichment MVP (Read-only)

## Scope
Этот документ фиксирует фактически реализованный read-only enrich/data pipeline для `deal_analyzer`.

Ключевое ограничение шага:
- только чтение внешних источников и локальный export;
- без write-back в Google Sheets;
- без изменений в working flows analytics / weekly_refusals / existing writers.

## Enrichment Sources

### 1) Client list (Google Sheets)
Используется как enrich источник клиентского прогресса.

Минимально подтягиваемые поля:
- `test_started`
- `test_finished`
- `test_status`
- `test_comments`
- `matched_client_row_ref`

### 2) Appointment table (Google Sheets)
Используется как enrich источник встреч и статусов назначения.

Минимально подтягиваемые поля:
- `meeting_assigned` / `appointment_date`
- `confirmation`
- `held_not_held` / `meeting_status`
- `transfer_cancel_flag`
- `matched_appointment_row_ref`

### 3) ROKS workbook (Google Sheets)
Используется как KPI/conversion context extractor (read-only).

Минимальный output:
- employee/month context
- team/month context
- weekly context (если маркеры доступны)
- conversion snapshot
- forecast/residual snapshot
- sanitized values (без `#DIV/0!`, `NaN`, `inf`)

## Matching Priority (deterministic)
Для client/appointment matching используется одинаковый приоритет:
1. `deal_id`
2. `phone`
3. `email`
4. `company + contact`
5. `company only`

## Per-deal Enrichment Output
По каждой сделке сохраняются:
- `enrichment_match_status` (`full`, `partial`, `none`, `disabled`, `error`)
- `enrichment_match_source` (`both`, `client_list`, `appointment_list`, `none`)
- `enrichment_confidence` (0..1)
- `matched_client_row_ref`
- `matched_appointment_row_ref`

Дополнительно в enriched deal:
- `enriched_test_started`
- `enriched_test_completed`
- `enriched_test_status`
- `enriched_test_comments`
- `enriched_appointment_date`
- `enriched_assigned_by`
- `enriched_conducted_by`
- `enriched_meeting_status`
- `enriched_transfer_cancel_flag`

## Unified Snapshot Contract
Snapshot builder формирует единый вход для analyzer reasoning:
- CRM normalized deal data
- enrichment context
- ROKS KPI/conversion context

Режимы:
- single deal snapshot
- period snapshot (массив per-deal items + team context)

## Operator Outputs (analysis-layer attachment)
В analysis export дополнительно формируются:
- `manager_summary`
- `employee_coaching`
- `employee_fix_tasks` (3..7 задач)

Стиль:
- деловой человеческий тон;
- без упоминаний AI/LLM;
- без канцелярской шаблонности.

## CLI
Новые команды:

```powershell
python -m src.deal_analyzer.cli --config config/deal_analyzer.local.json enrich-deal --input workspace/amocrm_collector/deal_31913530_latest.json
python -m src.deal_analyzer.cli --config config/deal_analyzer.local.json enrich-period --input workspace/amocrm_collector/collect_period_2026-04-01_2026-04-07_latest.json
python -m src.deal_analyzer.cli --config config/deal_analyzer.local.json roks-snapshot --manager "Илья"
python -m src.deal_analyzer.cli --config config/deal_analyzer.local.json roks-snapshot --team
```

## Artifacts
Output dir:
- `workspace/deal_analyzer/`

Артефакты:
- JSON
- Markdown
- CSV

## Not Implemented Yet
- write-back enriched outputs в Google Sheets;
- жестко зафиксированные бизнес-маппинги для всех вариантов ROKS листов;
- episode-aware aggregator поверх enriched snapshots (следующий этап).
