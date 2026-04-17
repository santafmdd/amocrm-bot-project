# amoCRM Collector MVP (Read-only)

## Что делает модуль
`src/amocrm_collector` собирает сделки через amoCRM API за период или по `deal_id`, нормализует их в единый формат и сохраняет локальные артефакты для следующего этапа аналитики.

Модуль **не пишет** в amoCRM.

## Команды
```powershell
python -m src.amocrm_collector.cli --config config/amocrm_collector.local.json collect-period --date-from 2026-04-01 --date-to 2026-04-07
python -m src.amocrm_collector.cli --config config/amocrm_collector.local.json collect-deal --deal-id 31913530
python -m src.amocrm_collector.cli --config config/amocrm_collector.local.json debug-deal-sections --deal-id 31913530
python -m src.amocrm_collector.cli --config config/amocrm_collector.local.json schema-check
```

## Источник авторизации
Collector использует уже существующий auth state:
- auth config: `config/amocrm_auth.local.json`
- state: `workspace/amocrm_auth/state.json`

## Что выгружается
- `collect-period`:
  - JSON payload с normalized списком сделок
  - JSONL с плоскими normalized rows
  - CSV с ключевыми полями
- `collect-deal`:
  - normalized сделка + raw bundle
- `debug-deal-sections`:
  - отдельный debug JSON по секциям `lead/notes/tasks/contacts/companies` со статусом ответа
- `schema-check`:
  - account/users/pipelines/custom fields snapshot

Все файлы: `workspace/amocrm_collector/`.

## Настройка исключений менеджеров
Исключения менеджеров задаются только через конфиг:
- `manager_ids_include`
- `manager_ids_exclude`
- `manager_names_exclude`

Хардкода имен в коде нет.

## Логика определения "презентация проведена"
`presentation_detected=true`, если выполнено любое условие из `presentation_detection.require_any_of`:
- `demo_result_present`
- `brief_present`
- `completed_meeting_task`
- `long_call`
- `comment_link_present` (ссылка обнаружена в комментарии компании/контакта)

`long_call` определяется по notes `call_in/call_out` с длительностью >= `min_call_duration_seconds`.

`presentation_detect_reason` содержит причины, например:
- `demo_result_present`
- `brief_present`
- `completed_meeting_task`
- `long_call_1850s`
- `company_comment_link_present`

## Где ищутся ссылки на материалы
Collector сканирует все источники (если включены в `presentation_link_search`):
- custom fields сделки (`scan_deal_custom_fields_url`)
- notes (`scan_notes_common_text`)
- комментарий компании (`scan_company_comment`)
- комментарий контакта (`scan_contact_comment`)

## Устойчивость collect-period и section warnings
Если отдельная секция по сделке (например `notes` или `tasks`) вернула не-JSON/ошибку, прогон не падает целиком:
- `lead core` сохраняется;
- проблемная секция подставляется как пустой список;
- в `bundle.warnings` пишется structured warning: `deal_id`, `section`, `endpoint_path`, `http_status`, `content_type`, `body_preview`.

В `collect-period` export добавляется сводка по секциям:
- `counts.deals_with_notes_warning`
- `counts.deals_with_tasks_warning`
- `section_warning_summary.notes.affected_deal_ids`
- `section_warning_summary.tasks.affected_deal_ids`

## Как читать debug-deal-sections
Команда `debug-deal-sections` нужна для диагностики без полного прогона периода.
Для каждой секции печатаются и сохраняются:
- `endpoint`
- `status`
- `content_type`
- `item_count`
- `body_preview` (если ответ не JSON)

Если `ok=false`, это сигнал проверить endpoint/параметры/прокси на стороне amoCRM.

## Ограничения MVP
- Набор полей настраивается через `*_field_id` в конфиге.
- Связанные сущности (контакты/компании/notes/tasks) читаются API-запросами per deal; для больших периодов это не оптимальный финальный вариант.
- Детектор презентации сейчас rule-based, без контентного анализа.
- `training_candidate_text` пока заготовка (пустая строка).

## Следующие шаги
1. Пакетная оптимизация API запросов и кэширование.
2. Расширение rule-проверок по notes/tasks.
3. Добавление data quality checks и профилей сбора.
4. Подготовка интерфейса запуска/настроек поверх config.
