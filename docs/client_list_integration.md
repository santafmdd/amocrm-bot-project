# Client List Integration

## Назначение
`Клиентский список` используется как дополнительный источник планирования для роли `sales_manager`.

Цель:
- строить задачи Ильи от коммерческого потенциала конкретных клиентов,
- не заменять план абстрактными CRM-only задачами,
- не уводить sales_manager в массовый холодный верх воронки.

## Конфиг
Поддерживаемые поля:
- `client_list_enabled`
- `client_list_spreadsheet_id`
- `client_list_sheet_name`
- `client_list_link_columns`
- `client_list_status_columns`
- `client_list_comment_columns`
- `client_list_value_columns`
- `client_list_next_step_columns`

## Pipeline
Пакет: `src/deal_analyzer/client_list/`
- `reader.py`: чтение листа и discovery заголовков.
- `normalizer.py`: нормализация строк и извлечение amoCRM IDs из ссылок.
- `prioritizer.py`: приоритизация клиентского пула по стадиям.
- `artifacts.py`: debug/summary.
- `cli.py`: `discover`, `build-context`.

## Категории приоритета
- `invoice_to_payment`
- `test_to_invoice`
- `demo_to_test`
- `interest_to_demo`
- `renewal`
- `stalled_warm`
- `reactivation`
- `low_priority`
- `no_action`

## Интеграция в week_plan
`week_plan build` добавляет client context в planning scope (для sales_manager) и использует его для задач по:
- demo/test/invoice/payment
- renewals/reactivation
- follow-up по warm/stalled клиентам

## Артефакты
- `client_list_discovery.json`
- `client_list_rows_normalized.json`
- `client_list_priority_summary.json`
- `week_plan_client_context_debug.json`

## Smoke CLI
```powershell
python -m src.deal_analyzer.client_list.cli discover --config config/deal_analyzer.call_review.deepseek.realwrite.json
python -m src.deal_analyzer.client_list.cli build-context --config config/deal_analyzer.call_review.deepseek.realwrite.json --manager "Илья Бочков" --period-start 2026-04-13 --period-end 2026-04-17 --dry-run
```

## Ограничения
- Интеграция не делает structural changes в Google Sheets.
- Используется как planning/evidence context для weekly и training контуров.
