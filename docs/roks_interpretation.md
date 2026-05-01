# ROKS Interpretation (Role-Based)

## Core principle
ROKS manager funnel is role-based, not always linearly attributable to one manager.

## Manager profiles

### Илья Бочков (`sales_manager`)
- Работает в основном по теплым/текущим этапам вниз по воронке.
- Может проводить демо, пришедшие из разных источников (свои и переданные).
- `demo > interest` допустимо.
- `ЛИД/СОСТ > 100%` может быть diagnostic-only, не blocker.

Правильная формулировка:
- `провел N демо`
- `часть демо могла прийти из встреч, назначенных Хомидовым/другими источниками`

Неправильная формулировка:
- `сам назначил N демо` (если это не подтверждено источником).

### Рустам Хомидов (`telemarketer`)
- Отвечает за верх воронки: `дозвоны -> ЛПР -> есть интерес`.
- Может передавать встречи дальше по воронке.
- `interest > 0` при `demo/test/invoice/payment = 0` допустимо.
- Downstream-метрики для него не должны быть обязательным KPI.

Правильная формулировка:
- `назначил N встреч / создал N есть интерес`
- `передал часть встреч на проведение`

## Validation/analytics contract
Нельзя считать ошибкой:
- `demo > interest` у Бочкова;
- `interest > 0` и downstream нули у Хомидова.

В weekly debug желательно иметь:
- `manager_role_profile`
- `source_generated_interest`
- `conducted_demo`
- `routed_meetings_possible`
- `downstream_metrics_applicable`

## Planning impact
- Week plan для Бочкова строится вокруг warm/current pipeline и коммерческих стадий.
- Week plan для Рустама допускает top-of-funnel задачи как основной контур.

## Demo methodology alignment
Для `sales_manager` демо-рекомендации должны быть consultative:
- educational demo
- guided discovery
- client hands-on
- next-step commitment
