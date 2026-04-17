# amoCRM Collector MVP (Read-only)

## Что делает модуль
`src/amocrm_collector` собирает сделки через amoCRM API за период или по `deal_id`, нормализует их в единый формат и сохраняет локальные артефакты для следующего этапа аналитики.

Модуль **не пишет** в amoCRM.

## Команды
```powershell
python -m src.amocrm_collector.cli --config config/amocrm_collector.local.json collect-period --date-from 2026-04-01 --date-to 2026-04-07
python -m src.amocrm_collector.cli --config config/amocrm_collector.local.json collect-deal --deal-id 31913530
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
- `schema-check`:
  - account/users/pipelines/custom fields snapshot

Все файлы: `workspace/amocrm_collector/`.

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
