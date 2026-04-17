# Архитектура проекта

## Слои системы

### 1. Browser/Session Layer

Назначение: безопасный запуск браузера и управление read-only сессией.

Компоненты:

- `src/browser/session.py`
- безопасные пути внутри проекта
- headful/headless режимы
- сохранение storage state

### 2. Filter Setup Flow

Назначение: автоматическая постановка фильтра на analytics-экране на основе report profile.

Компоненты:

- `src/browser/analytics_flow.py`
- открытие analytics экрана
- открытие панели фильтра
- установка `filter_source` (`tag` / `utm_source`)
- ввод `filter_values`
- click `Применить`
- диагностические screenshots и логи

### 3. Capture Flow

Назначение: последовательный сбор срезов `all / active / closed` после применения фильтра.

Компоненты:

- URL-based tab switching через `deals_type`
- `src/browser/amo_reader.py` (`read_all_tab_modes_by_url`)
- snapshot export JSON/CSV на каждую вкладку

### 4. Page Readers

Назначение: читать конкретный экран amoCRM в текущем состоянии.

Компоненты:

- `src/browser/amo_reader.py` (текущий MVP для `analytics_sales`)
- будущие readers для `deals_list`, `events_list`

### 5. Parsers

Назначение: преобразовывать видимый UI-текст в структурированные данные.

Компоненты:

- structured parser `visible_text`
- debug dump слой
- fallback parser

### 6. Report Jobs

Назначение: запуск профиля отчета end-to-end.

Компоненты:

- `src/run_profile_analytics.py`
- выбор report profile по `--report-id`
- orchestration: profile -> filter setup -> capture -> export

### 7. Config-Driven Profiles

Назначение: управлять отчетами и источниками через конфиг, без переписывания ядра.

Компоненты:

- `config/page_profiles.yaml`
- `config/report_profiles.yaml`
- `config/table_mappings.yaml`
- `src/config_loader.py`

Идея:

- добавить новый отчет = добавить/изменить профиль в YAML;
- добавить новый источник (analytics/deals/events) = добавить page profile + reader + flow.

## Контур 1: Табличная автоматизация (MVP)

Цель: сформировать "воронку отказов" в табличном виде.

## Контур 2: Weekly Summary по отказам

Цель: еженедельный краткий отчет по причинам и динамике отказов.

## Контур 3: Анализ сделок, звонков и презентаций

Цель: расширенная аналитика по качеству воронки и коммуникаций.

## Update (2026-04-16): Weekly Refusals Canonical Domain Layer

For weekly refusals path (`events_list -> parser -> writer`), a dedicated domain normalizer is now part of architecture:
- `src/domain/refusal_status_normalizer.py`.

Responsibilities:
- canonical text normalization for refusal statuses/reasons,
- explicit alias collapsing for known near-duplicates,
- stable grouped parsing for after-side reasons.

Writer integration:
- canonical dedupe is applied to both incoming parsed rows and existing sheet rows,
- compact block is planned first, then expanded safely with row insert if capacity is insufficient.

## Update (2026-04-16): Weekly Writer Expansion and Canonical Dedup Contract

Weekly refusals write path (`events_list -> parser -> weekly_refusals_block_writer`) now guarantees:
- canonical normalization before aggregation and before write planning,
- compact block layout planning without near-duplicate reason rows,
- safe expansion with real Sheets row insert when required,
- physical shift of subsequent section blocks downward.

Canonical duplicate merge example family:
- `???????? ???????? ?? ???`
- `???????? ???????? ?? ????`
- `???????? ???????? ?? ?????`

All variants collapse into canonical key `???????? ???????? ?? ?????`.

Status of next architecture contour:
- deal/episode analyzer (AI enrichment) is planned, but not implemented in current stage.



## Update (2026-04-16): Weekly Writer Cumulative Modes + Guard

`weekly_refusals_block_writer` now has explicit cumulative strategy layer:
- `recompute_from_source`
- `add_existing_values`

Add mode architecture details:
- reads existing sheet block counts by canonical key,
- merges with incoming parsed weekly counts,
- writes compact rewritten block,
- stores idempotency state in debug artifact (`weekly_refusals_cumulative_guard_state.json`).

Guard semantics:
- key = `target_id + period_key`,
- duplicate live apply blocked by default,
- dry-run does not mutate guard state.


## Update (2026-04-16): Transition to Deal/Episode Analyzer Design

???????? ?????? Google Sheets ? ??????? ????????????? ???? ????????? ???????????:
- analytics writer,
- weekly refusals writer (2m/long),
- cumulative refusals writer (2m/long).

???????? ?????? ??????:
- `weekly` -> overwrite-from-source,
- `cumulative` -> additive `add_existing_values` (??? ??????? period duplicate guard).

????? ??????? ?????? ???????????:
- ?????????? ??????/???????? (design/start phase).

Google Sheets (business layer) ??? ???????????:
- `???????? ????????`
- `?????? ??????`
- `?????`
- `?????????`

??????????? ????:
- ????????? ??????????? ????? ? Sheets ???? ?? ????????????,
- ??????????? ????????? ???????? ???????? (???????, debug/compiled artifacts, guard state).

UI-oriented future integration points:
- weekly/cumulative ?????????,
- ????? ???????/??????? ??????,
- ?????? analytics ?? tag/utm_source,
- ????????? ???????????,
- ????????? ?????????/??????? ?????????? ??? ???????????.
