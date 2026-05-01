# Architecture

## 1. Основная идея
Система состоит из изолированных контуров, которые связаны через артефакты и Google Sheets, но должны быть независимы по записи и безопасны по preflight.

Контуры:
- `call_review` (`Разбор звонков`)
- `daily_control` (`Дневной контроль`)
- `week_plan` (`План недели`)
- `training_materials` (документы обучения + ссылки в `План недели`)
- `weekly_manager_summary` (`Недельный свод менеджеров`)
- `week_summary` (`Свод недели`)
- shared-контуры: `weekly_shared`, `employee_profiles`, `client_list`, `employee_dashboard`, `progress`, `cache_manager`, `ui_foundation`.

## 2. Google Sheets слой
Рабочие табы файла `РОКС 2026`:
- `Разбор звонков`
- `Дневной контроль`
- `План недели`
- `Недельный свод менеджеров`
- `Свод недели`
- `РОКС ОАП-<месяц 2026>`

Важно:
- запись только values-only writers;
- структурные операции запрещены в обычном потоке;
- discovery/header mapping обязателен перед write.

## 3. Цепочка данных
Базовый полный цикл:
1. call_review формирует качественные кейсы;
2. daily_control строит день-менеджер пакеты;
3. weekly_manager_summary агрегирует manager-week и plan-fact;
4. week_summary агрегирует week-level отдела;
5. week_plan строит следующий план с учетом сводов и незакрытых задач;
6. training_materials создает материалы по строкам `План недели` с `activity_type=обучение`.

## 4. Weekly особые правила
- Первая неделя `2026-03-30..2026-04-03`: bootstrap допустим для `week_plan` при пустом signal pool.
- Текущая незакрытая неделя: планируем (`week_plan` + `training_materials`), но не закрываем в weekly своды.

## 5. Role policy
- `Илья Бочков` -> `sales_manager`.
  - коммерческий фокус: warm/current pipeline и downstream этапы.
  - запрет массового cold top-of-funnel как primary task.
- `Рустам Хомидов` -> `telemarketer`.
  - основной фокус: top-of-funnel, ЛПР, interest creation.

Role guard применяется в:
- week_plan
- weekly_manager_summary recommendations
- week_summary recommendations
- training_materials topics.

## 6. Week Plan v2 контракты
- Daily Task Triad (`развитие / коммерческий результат / контроль`) в каждой строке manager-day.
- SMART-валидация задач и post-training задач.
- duplicate guard (exact + semantic) внутри manager-week.
- coverage gate после preflight: частичная неделя менеджера блокирует write.

## 7. Demo methodology
Для sales_manager используется consultative demo стандарт:
- educational demo
- guided discovery
- client hands-on
- next-step commitment

Подробно: `docs/demo_methodology.md`.

## 8. Training materials architecture
- Build готовит payload и quality/quarantine.
- Write создает Google Docs/Drive ссылки (если API доступен) и пишет ссылки в `План недели`.
- External sources policy:
  - require external (strict)
  - allow no external (warning mode)
  - curated fallback file.

## 9. Diagnostics/artifacts
Каждый run-dir обязан иметь summary/debug и row-level причины фильтраций/quarantine.

Общие принципы:
- без silent drops;
- block_reason должен быть точным (`rows_empty`, `payload_missing`, `llm_generation_failed`, `external_sources_unavailable`, ...);
- row_flow_debug фиксирует этап и причину.

## 10. Long-running operations
Реализованы:
- `progress.json`
- `progress.log`
- `heartbeat.json`

Cache ops:
- `cache_manager status`
- `cache_manager cleanup --dry-run|--delete`

## 11. UI foundation
FastAPI слой запускает те же CLI-контуры как jobs.

Safety:
- `real_write=false` по умолчанию;
- real_write только по explicit confirmation token;
- structural operations policy: blocked.

## 12. Safety contract
Запрещено без явной команды:
- менять структуру листов;
- удалять вкладки;
- real-write без preflight/smoke;
- пушить токены/секреты.
