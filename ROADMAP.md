# ROADMAP

## Active Stage

Переход от raw TSV writer к layout-aware writer для Google Sheets шаблона.

Что это значит на практике:
- основной путь записи: `google_sheets_layout_ui`;
- блоки определяются по жёлтым строкам с DSL и по структуре stage-таблицы под ними;
- запись только в числовые ячейки `Все/Активные/Закрытые`;
- без технических маркеров типа `::layout::` и без скрытых служебных строк.

## In Scope (Current Stage)

Поддерживаемые фильтры и поля DSL:
- `Теги` / `tags`
- `UTM Source` / `utm_source`
- `Воронка` / `pipeline`
- `Период` / `period`
- `Даты` / `dates_mode`

Поддерживаемые операторы:
- `=`
- `^=` (prefix match, с явным логированием ограничения, если UI не позволяет честно применить)

## Out of Scope (Current Stage)

Пока не реализуем:
- нижнюю таблицу отказов;
- менеджеров;
- продукт;
- источник сделки;
- сложные чекбокс-группы (кроме критичного минимума);
- произвольные блоки вне stage-таблиц;
- полную универсальную платформу под все поля amoCRM.

## Execution Priorities

1. Стабильный layout discovery на реальном листе без fixed coordinates.
2. Надёжный DSL parsing (`;`, `|`, `||`, `=`, `^=`).
3. Scenario scoring по согласованным правилам.
4. Точечная запись чисел в stage-блоки без full clear листа.
5. Отдельным этапом: полноценные per-scenario amoCRM re-runs для `||`.

## Update (2026-04-16): Weekly Refusals Writer Hardening

Done in current stage:
- canonical refusal status normalization added as a separate domain layer,
- near-duplicate refusal reasons collapse to canonical form before write planning,
- compact weekly block planning with safe row expansion via Google Sheets `insertDimension` rows,
- no dependence on accidental fixed gaps between sections.

Remaining:
- maintain alias dictionary as new real-world refusal variants appear,
- optional manual review tooling for canonical alias suggestions (future).

## Update (2026-04-16): Weekly Refusals Safe Insert + Canonical Merge

Operationally confirmed in current stage:
- block expansion uses real row insert,
- lower sections shift down physically,
- near-duplicate refusal reasons are canonicalized and merged before write.

Examples merged into one canonical reason row:
- `???????? ???????? ?? ???`
- `???????? ???????? ?? ????`
- `???????? ???????? ?? ?????`

Dry-run contract remains unchanged:
- planning/debug only, no live write/insert.

Next big stage (not implemented yet):
- analyzer for deals/episodes and explanation layer over collected weekly refusals dataset.



## Update (2026-04-16): Weekly/Cumulative Semantics Locked

Weekly refusals write contract is now fixed:
- weekly: source-slice overwrite,
- cumulative: source-range recompute overwrite.

Out of scope (explicit):
- additive merge from existing sheet values,
- formula patching with `+...` at write time.

This prevents uncontrolled growth on repeated runs for the same source period.


## Update (2026-04-16): Cumulative Strategies Split

Weekly refusals now supports two cumulative write strategies:
- `recompute_from_source`
- `add_existing_values`

For `add_existing_values`, duplicate period protection is mandatory:
- live re-apply of same `period_key` is blocked by guard state.

Near-term ops note:
- cumulative profiles in config currently use `add_existing_values`;
- manual reruns for same period require explicit force override path.


## Update (2026-04-16): Reporting Scope Closed, Analyzer Design Next

? ??????? scope ?????? ???? ??????? ? Google Sheets:
- analytics block,
- weekly refusals weekly (2m/long),
- cumulative refusals (2m/long).

??????????????? ?????? ??????:
- weekly: overwrite-from-source,
- cumulative: `add_existing_values`.

????????? ??????? ????:
- ?????????????? ??????????? ??????/????????.

????????? ??????-????? ???????????:
- `???????? ????????`,
- `?????? ??????`,
- `?????`,
- `?????????`.

?????????? ?? ??????????????:
- ??????????? ????? ? Google Sheets ???? ?? ????????,
- runtime/state ???????? ? ????????? ?????? ? ????????.

## Update (2026-04-18): Next Delivered Step (Analyzer Enrichment)

Сделано:
- реализован read-only enrich/data pipeline в `deal_analyzer`;
- подключены внешние источники (client list, appointment table, ROKS context);
- добавлены operator outputs (`manager_summary`, `employee_coaching`, `employee_fix_tasks`);
- добавлены новые operator CLI команды (`enrich-deal`, `enrich-period`, `roks-snapshot`).

Не входит в этот шаг:
- write-back enrich результатов в Google Sheets;
- изменения в analytics/weekly_refusals writer paths;
- direct LLM data fetching from source systems.

Следующий логичный шаг:
1. стабилизировать field mapping для конкретных боевых вкладок client/appointment/ROKS;
2. подключить controlled writer layer для enriched operator outputs (отдельным этапом, без ломки текущих отчетов).

## Update (2026-04-18): Deal Analyzer Calls MVP

Delivered:
1. Call evidence layer (API-first + fallback)
2. Transcript adapter + cache
3. Snapshot integration with call-derived context
4. Operator CLI for call collection/transcription

Next:
- plug first production STT backend into transcription adapter
- add episode-level call segmentation on top of cached transcripts
