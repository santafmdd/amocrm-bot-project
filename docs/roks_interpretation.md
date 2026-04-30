# ROKS Role-Based Interpretation

This project treats manager funnel metrics in ROKS as role-based, not strictly linear per one manager.

## Core rule
- A manager's `demo/test/invoice/payment` volume can include meetings routed from other managers or inbound streams.
- Therefore, per-manager comparisons like `demo <= interest` are diagnostic-only and must not be a validation blocker.

## May 2026 operational interpretation

### Ilya Bochkov
- `interest` = mainly self-generated interest/meetings he scheduled.
- `demo` = demos he actually conducted (own + routed).
- `demo > interest` is allowed and not an error.
- `ЛИД/СОСТ` above 100% can be diagnostic-only and not a hard validation error.

Recommended wording in summaries:
- Use `провел N демо`.
- If needed: `часть демо могла прийти из встреч, назначенных Хомидовым/другими источниками`.
- Do not claim `сам назначил N демо` unless source data explicitly confirms it.

### Rustam Khomidov
- Owns top-of-funnel: `дозвоны -> ЛПР -> есть интерес`.
- May pass meetings further for demo/test/invoice/payment.
- `interest > 0` with `demo/test/invoice/payment = 0` is allowed and not an error.
- Downstream stages are not mandatory for his personal KPI evaluation.

Recommended wording in summaries:
- Use `назначил N встреч / создал N есть интерес`.
- Mention routing: `часть встреч передана на проведение` when relevant.
- Do not evaluate him as if demo/test/payment are required personal outputs.

## Implementation contract
- No weekly parser/validator should block rows because of role-based non-linearity above.
- Weekly artifacts should include role interpretation debug fields:
  - `manager_role_profile`
  - `source_generated_interest`
  - `conducted_demo`
  - `routed_meetings_possible`
  - `downstream_metrics_applicable`

