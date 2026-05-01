# Weekly Control and Base Analysis Spec

## 1. Назначение
Документ фиксирует бизнес-спецификацию weekly-контуров и связанных слоев:
- `Разбор звонков`
- `Дневной контроль`
- `План недели`
- `Недельный свод менеджеров`
- `Свод недели`
- base/client context
- coaching/training

## 2. Какие листы используются
Основной workbook: `РОКС 2026`.

Ключевые вкладки:
- `Разбор звонков`
- `Дневной контроль`
- `План недели`
- `Недельный свод менеджеров`
- `Свод недели`
- `РОКС ОАП-<месяц 2026>`

Дополнительные источники:
- `Клиентский список` (если `client_list_enabled=true`)
- training artifacts/docs links

## 3. Смысл каждого слоя
- `Разбор звонков`: качественный анализ кейса/звонка, контекст для coaching.
- `Дневной контроль`: manager-day контрольный срез и message сотруднику.
- `План недели`: план руководителя по датам и менеджерам (операции/контроль/обучение/развитие/стратегия).
- `Недельный свод менеджеров`: факт недели по менеджеру + plan-fact.
- `Свод недели`: итог недели отдела и вход в план следующей недели.

## 4. Правильная временная логика
Нормальный цикл:
1. `Дневной контроль` за неделю N
2. `Недельный свод менеджеров` за N
3. `Свод недели` за N
4. `План недели` на N+1

Bootstrap-режим:
- Для первой недели `2026-03-30..2026-04-03` в `week_plan` допустим bootstrap (при отсутствии signal history).

Текущая незакрытая неделя:
- План и обучение разрешены.
- Weekly summary/department summary до закрытия недели не записываются.

## 5. Role policy
- `Илья Бочков` = `sales_manager`:
  - warm/current pipeline и коммерческие этапы вниз по воронке.
  - запрещен массовый холодный верх воронки как основной фокус.
- `Рустам Хомидов` = `telemarketer`:
  - верх воронки и cold-контур допустим как primary focus.

## 6. Week plan quality
Обязательные правила:
- Daily Task Triad: `Развитие / Коммерческий результат / Контроль`.
- SMART-задачи (в т.ч. post-training).
- Duplicate guard внутри manager-week (exact + semantic).
- Coverage gate после validation/preflight (не по raw payload).
- CRM-only задачи не могут быть главной коммерческой задачей дня.

## 7. ROKS interpretation
ROKS по менеджерам интерпретируется role-based:
- для Бочкова `demo > interest` допустимо,
- для Хомидова downstream-этапы могут быть не применимы.

Не трактовать это как validation error.

## 8. Demo standard
Для sales_manager применяем consultative demo:
- educational demo
- guided discovery
- client hands-on
- фиксация next step

## 9. Training materials
- Источник строк: `План недели`, `activity_type=обучение`.
- External sources policy:
  - strict `--require-external-sources`
  - optional `--allow-no-external-sources`
  - curated fallback.

## 10. Base/client analysis
`Клиентский список` используется для коммерческого контекста задач sales_manager:
- invoice_to_payment
- test_to_invoice
- demo_to_test
- interest_to_demo
- renewal
- stalled_warm
- reactivation

## 11. Safety policy
- Только values-only writer path.
- Без structural operations.
- Без silent drops: каждая отфильтрованная строка должна быть в debug.
- Preflight обязателен перед write.

## 12. Артефакты
У каждого контура должны быть:
- `summary.json` / `summary.md`
- payload/quarantine/debug
- writer_plan + writer_status

Long-running контуры дополнительно:
- `progress.json`
- `progress.log`
- `heartbeat.json`
