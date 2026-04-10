## Test Run Policy

Run tests via module invocation only:
`python -m pytest -q -p no:cacheprovider`

Smoke/regression subset:
`python -m pytest -q -p no:cacheprovider tests\test_analytics_flow_utm_exact.py tests\test_filter_handlers_v1.py tests\test_filter_registry_v1.py`

This guarantees the active project interpreter/venv is used and keeps discovery limited to configured test paths.


### Temp / Cache Hygiene

- Run tests via `python -m pytest ...` from project root so discovery follows `pytest.ini`.
- Do not use root-level scratch paths for tests (`tmp*`, `pytest-cache-files-*`).
- Keep temporary/debug artifacts under project-owned paths:
  - `exports/debug/` for runtime diagnostics
  - `workspace/` for local working files
- Ignore temporary directories in git (`exports/tmp*`, `tests/tmp*`, `.pytest_cache/`, `__pycache__/`, `*.pyc`).

# amoCRM + Google Sheets + OpenClaw/Ollama Automation (Local Skeleton)


## Runtime Update (2026-04-08)

Service target is unchanged:
`open analytics -> set filter -> capture all/active/closed -> write to sheet`.

Current MVP priority:
`profile-driven analytics flow -> filter automation -> capture all/active/closed -> write top block to sheet`.

Confirmed working now:
- `source_kind=tag` via holder popup path
- filter apply
- capture `all/active/closed`
- compiled outputs (`compiled_profile`, `compiled_stage_pivot`)
- Google Sheets API write for first layout block
- last production markers: `tag_selection_success=true`, `Filter apply confirmed`, `successful_tabs=3/3`, `updatedCells=30`, `fallback used=false`

Not production-ready yet:
- `utm_exact` / `utm_prefix`
- batch-from-sheet-dsl production
- weekly refusals / AI summary / cleanup policy
## Current MVP Status (2026-04-08)

Service target: automate amoCRM report routine by profile:
`open screen -> set filter -> capture all/active/closed -> write to sheet`.

Current MVP priority:
`profile-driven analytics flow -> filter automation -> capture all/active/closed -> write top block to sheet`.

Confirmed working right now:
- `source_kind=tag`
- tag selection via holder-popup path
- filter apply
- capture `all / active / closed`
- compiled outputs: `compiled_profile` and `compiled_stage_pivot`
- Google Sheets API writer for first layout block

Last confirmed production markers:
- `tag_selection_success=true`
- `Filter apply confirmed`
- `successful_tabs=3/3`
- `updatedCells=30`
- `fallback used=false`

Not production-ready yet:
- `utm_exact`
- `utm_prefix`
- batch-from-sheet-dsl production mode
- weekly refusals
- AI summary
- log retention/cleanup


Этот проект — безопасный локальный каркас для пошаговой автоматизации на домашней Windows-машине.
Текущий шаг добавляет read-only MVP браузерного чтения аналитики amoCRM: скрипт открывает интерфейс, читает текущие цифры и сохраняет результат в `exports`.

## Этапы реализации

1. MVP заполнения листа "воронка отказов"
2. Weekly summary по отказам
3. Анализ сделок, звонков и презентаций

## Что уже есть

- Изолированная структура директорий внутри `project`
- Базовая конфигурация через `.env`
- Проверки безопасности путей (запрет выхода за пределы проекта)
- Логирование в консоль и файл
- Browser read-only MVP для amoCRM аналитики:
  - Playwright-сессия с `storage state`
  - большое окно браузера для стабильного layout (`--start-maximized`, `no_viewport=True`)
  - чтение текущего экрана аналитики
  - DOM-debug дампы для подбора селекторов
  - скриншот + экспорт JSON/CSV в `exports`
- Подготовительный config-driven слой:
  - `config/page_profiles.yaml`
  - `config/report_profiles.yaml`
  - `config/table_mappings.yaml`
  - `src/config_loader.py`
- Постоянные правила агентной разработки в `AGENTS.md`

## Установка

1. Установить Python 3.11+.
2. Создать и активировать виртуальное окружение.
3. Установить зависимости:
   `pip install -r requirements.txt`
4. Установить браузер для Playwright:
   `python -m playwright install chromium`
5. Скопировать `.env.example` в `.env` и заполнить значения, особенно:
   - `AMO_BASE_URL`
   - `AMO_ANALYTICS_URL`
   - `AMO_VIEWPORT_WIDTH` / `AMO_VIEWPORT_HEIGHT` (для headless режима)

## Первый ручной запуск

1. В `.env` поставить `AMO_HEADLESS=false`.
2. Запустить reader с ручной паузой:
   `python -m src.run_read_analytics --source-kind tag --filter-id manual --tab-mode all --wait-for-enter`
3. В окне браузера при необходимости войти в amoCRM.
4. Открыть нужный экран аналитики.
5. Выставить фильтры и вкладку вручную.
6. Вернуться в терминал и нажать Enter.
7. Reader считает текущий экран и сохранит screenshot + JSON/CSV.

## Ручной логин и ручная подготовка экрана

Если не хотите, чтобы скрипт автоматически открывал `AMO_ANALYTICS_URL`, используйте `--skip-open`:

`python -m src.run_read_analytics --source-kind tag --filter-id manual --tab-mode all --skip-open --wait-for-enter`

## Рекомендуемый практический режим (manual all-tab-modes)

Для ближайшей стабильной работы используйте полуавтоматический режим:

`python -m src.run_read_analytics --source-kind tag --filter-id manual --skip-open --wait-for-enter --all-tab-modes-manual`

Как это работает:

- после первого Enter reader читает текущий экран как `all` и сразу сохраняет export;
- затем просит вручную переключить вкладку на `АКТИВНЫЕ` и нажать Enter;
- читает `active` и сразу сохраняет export;
- затем просит вручную переключить вкладку на `ЗАКРЫТЫЕ` и нажать Enter;
- читает `closed` и сразу сохраняет export.

В этом режиме нет автокликов по вкладкам, поэтому он надежнее как workaround, пока auto-switching еще дорабатывается.

## Profile-driven analytics flow (новый шаг)

Добавлен первый profile-driven режим:

`python -m src.run_profile_analytics --report-id analytics_tag_single_example`

Что делает режим:

- загружает report profile из `config/report_profiles.yaml`;
- открывает `analytics_sales` экран;
- пытается открыть фильтр и выставить `filter_source` (`tag` или `utm_source`) + `filter_values`;
- нажимает `Применить`;
- запускает capture вкладок по URL `deals_type=all/active/closed`;
- сохраняет JSON/CSV по каждой вкладке.

На этом этапе это первый шаг к полному automation flow: `profile -> filter -> all/active/closed capture`.
Если automation фильтра не сработал из-за селекторов, смотрите debug screenshots в `workspace/screenshots` и debug dumps панели фильтра в `exports/debug/` (`*_filter_panel_visible_text_*.txt`, `*_filter_panel_selectors_*.json`).

Дополнительно включен scroll-debug панели фильтра: создаются пошаговые файлы `*_filter_panel_scroll_step_XX.txt` и объединенный `*_filter_panel_scroll_merged.txt`, чтобы увидеть полный список фильтров после прокрутки.
## Compile верхнего блока (первый writer шаг)

После сбора трех JSON (`all/active/closed`) можно собрать готовый compiled CSV для верхнего блока:

`python -m src.run_compile_top_block`

Что делает этот шаг:

- читает snapshot JSON для `all`, `active`, `closed` (автоматически берет последние из `exports/`);
- использует `top_cards` как основной источник;
- формирует плоский CSV в `exports/compiled/`:
  - `stage_name`
  - `all_count`
  - `active_count`
  - `closed_count`
- если этап отсутствует в одной из вкладок, ставит `0`.

Это промежуточный практический шаг перед записью в реальную таблицу (Google Sheets write пока не выполняется).

## Авто-прогон всех вкладок (URL-based)

Можно подготовить экран один раз вручную и запустить автоматическое переключение вкладок:

`python -m src.run_read_analytics --source-kind tag --filter-id manual --skip-open --wait-for-enter --all-tab-modes`

В этом режиме reader переключает вкладки через URL-параметр `deals_type`, без UI-кликов по вкладкам:

- `deals_type=all`
- `deals_type=active`
- `deals_type=closed`

Каждая успешно прочитанная вкладка экспортируется сразу (JSON + CSV).
Если чтение следующей вкладки не удалось, уже сохраненные файлы остаются в `exports/` и не теряются.

`--all-tab-modes-manual` остается запасным режимом: пользователь вручную переключает вкладки и подтверждает шаги Enter.

## Расширяемая конфигурация

Идея простая:

- код = движок чтения/обработки;
- config = что именно запускать и куда складывать результат.

Для ручного редактирования используются YAML-файлы в `config/`:

- `page_profiles.yaml` — какие типы страниц amoCRM есть в проекте;
- `report_profiles.yaml` — какие отчеты запускать, с какими фильтрами/вкладками/источниками;
- `table_mappings.yaml` — куда писать результат (целевые блоки/режимы записи) на следующих этапах.

В будущем новые теги, новые отчеты, новые страницы amoCRM (`analytics`, `deals`, `events`) можно будет добавлять через config без переписывания ядра.

## Важно про ограничения MVP

- Read-only поведение: никаких действий `save/submit/delete`.
- На этом шаге фильтры выставляются вручную в amoCRM UI.







## OpenClaw CDP Backend

?? ?? browser backend `openclaw_cdp`  ? ?  ?? OpenClaw-managed Chrome (?? CDP),  ?? ? ? Chromium ?.

 ?:

`python -m src.run_profile_analytics --report-id analytics_tag_single_example --browser-backend openclaw_cdp`

?? ?? env:

- `BROWSER_BACKEND=openclaw_cdp`
- `OPENCLAW_CDP_URL=http://127.0.0.1:18800`

? CDP endpoint ?,   ??  ? ?  OpenClaw browser profile.


## External Agent Bridge (tag external_agent mode)

For `--tag-selection-mode external_agent` you can run an external bridge command after handoff JSON is prepared.

Example:

`python -m src.run_profile_analytics --report-id analytics_tag_single_example --browser-backend openclaw_cdp --tag-selection-mode external_agent --external-agent-bridge-cmd "your_agent_bridge_command"`

Bridge environment variables:
- `EXTERNAL_AGENT_HANDOFF_PATH`
- `EXTERNAL_AGENT_TARGET_VALUE`
- `EXTERNAL_AGENT_URL_BEFORE`
- `EXTERNAL_AGENT_CDP_URL`

If bridge command is not set, flow falls back to manual Enter confirmation.


## Tag Selection In Script Mode (Enter + Esc)

For amoCRM tag filter in `script` mode the primary scenario is now keyboard-based:
1. Focus tag field
2. Type target tag
3. Press `Enter` to insert tag
4. Press `Esc` to close dropdown
5. Wait for `` button to become visible/enabled
6. Click ``
7. Verify URL contains `useFilter=y` and `tag[]`

Dropdown-item click path is kept only as fallback.


## Apply Step Reliability (amoCRM analytics filter)

When tag chip is already selected, flow now treats tag selection as successful and moves to apply-step.
Before click, reader scrolls filter panel to bottom, collects apply-button candidates, logs their debug payloads, and uses multi-step click fallback (`normal`, `scroll_then_click`, `force`, `bbox`, `js`).
Apply success is confirmed by URL/effect signals (URL changed or `useFilter` + `tag[]`).

## Writer MVP (Google Sheets UI test tab)

After successful profile capture, `run_profile_analytics` now also:

- builds one compiled result object for tabs `all/active/closed`;
- saves compiled JSON to `exports/compiled/`;
- opens Google Sheets in the same browser session;
- switches to test tab `analytics_writer_test`;
- clears the tab and writes fresh data from `A1`.

Writer MVP uses browser UI (no Google API credentials) and requires active user login in Google.

Configuration source:

- `config/table_mappings.yaml`
- mapping id: `analytics_writer_test_destination`
- fields: `sheet_url`, `tab_name`, `write_mode`, `start_cell`

You can also provide test sheet URL via env fallback:

- `GOOGLE_SHEETS_TEST_URL=...`

Run command:

`python -m src.run_profile_analytics --report-id analytics_tag_single_example --browser-backend openclaw_cdp --tag-selection-mode script`


## Layout Writer (Anchor-Based, Non-Destructive)

A new writer mode is available: `kind: google_sheets_layout_ui`.

Goal:
- keep existing sheet formatting/merged cells/colors intact;
- find target block by text anchors;
- update only numeric cells (`all/active/closed`) for mapped stages.

How it works:
- build stage pivot from compiled result: `stage -> {all, active, closed}`;
- locate block anchor by alias list;
- locate header row (`stage/all/active/closed` aliases);
- build stage-row map;
- write planned numeric cells only.

Dry-run mode (no writes):

`python -m src.run_profile_analytics --report-id analytics_tag_layout_example --browser-backend openclaw_cdp --tag-selection-mode script --writer-layout-dry-run`

Raw writer path is preserved for debug/test tab (`kind: google_sheets_ui`).
## Browser Filters v1 (2026-04-09)

`src/browser/analytics_flow.py` now uses handler-based routing for supported browser filters.

Supported filters v1:
- `tag`
- `pipeline`
- `date`
- `manager`
- `utm_source` exact
- `utm_source` prefix (best-effort)

Implementation files:
- `src/browser/filters/base.py`
- `src/browser/filters/registry.py`
- `src/browser/filters/tag_filter.py`
- `src/browser/filters/utm_filter.py`
- `src/browser/filters/pipeline_filter.py`
- `src/browser/filters/date_filter.py`
- `src/browser/filters/manager_filter.py`

Out of scope for this step:
- writer/discovery/DSL redesign
- refusals/AI summary production flow
- universal selectors for every amoCRM tenant-specific UI variant

## Runtime Validation Status (2026-04-09)

Post-refactor status for browser filter handlers v1:
- Runtime verified: `tag`
- Runtime pending: `utm_source exact`, `pipeline`, `date`, `manager`
- `pipeline/date` are currently covered only through batch DSL scenarios as secondary filters.
- `manager` is not present in current runtime-ready profiles/DSL examples.

## Runtime MVP (Current)

Real MVP path today:
1. Browser/OpenClaw opens amoCRM analytics and applies UI filters.
2. Capture reads tabs `all / active / closed`.
3. Compiled artifacts are produced (`compiled_profile`, `compiled_stage_pivot`).
4. Google Sheets API writer updates target layout block (dry-run/live modes).

Supported `source_kind` status:
- `tag`: runtime-stable path.
- `utm_source` exact: implemented with strict row/popup-scoped runtime path; runtime stability still under active validation.

## Runtime Boundaries (Who Owns What)

- Orchestration:
  - `src/browser/analytics_flow.py`
  - Owns sequence: open panel -> select source -> apply filter -> apply button -> capture tabs.

- Filter runtime handlers:
  - `src/browser/filters/registry.py` routes by filter key.
  - `src/browser/filters/tag_filter.py` owns holder-popup tag selection + chip verify logic.
  - `src/browser/filters/utm_filter.py` owns UTM handler apply/verify wiring.
  - `src/browser/filters/pipeline_filter.py`, `date_filter.py`, `manager_filter.py` own v1 filter handler paths.

- Writer/discovery:
  - Kept separate from browser filter mechanics.
  - API layout discovery/write remains in writer/integration modules and is not part of filter runtime logic.

## UTM Exact Row-Scoped Logs

Row-scoped exact flow health signals:
- `utm_row_scope_resolved=true`
- `utm_row_multisuggest_id=...`
- `utm_popup_multisuggest_id=...`
- `utm_input_multisuggest_id=...`
- `utm_popup_id_matches_row=true`
- `utm_input_id_matches_popup=true`
- `utm_exact_selection_success=true`

If flow fails, inspect:
- `utm_exact_fail_reason=active_popup_not_opened`
- `utm_exact_fail_reason=row_scoped_input_not_activated`
- `utm_exact_fail_reason=chip_not_detected`

## Golden Runtime Commands

Tag x3:
- `python -m src.run_profile_analytics --report-id analytics_tag_single_example --writer-layout-api-preferred --writer-layout-api-dry-run --browser-backend openclaw_cdp --tag-selection-mode script`

UTM exact x2:
- `python -m src.run_profile_analytics --report-id analytics_utm_single_example --writer-layout-api-preferred --writer-layout-api-dry-run --browser-backend openclaw_cdp --tag-selection-mode script`


## Writer Destination Diagnostics

When Google Sheets UI writer starts, logs now include destination context:
- `target_id`
- `sheet_url`
- `tab_name`
- `write_mode`

If tab is missing, runtime error includes `target_id`, `tab_name`, and a hint to verify `config/table_mappings.yaml`.
Writer also logs visible tab names detected in the sheet for faster troubleshooting.


## UTM Profile Routing (Legacy vs Layout)

Two UTM report profiles are intentionally separated:

- Legacy profile: `analytics_utm_single_example`
  - keeps legacy output target: `event_top_block_1`
- Layout writer profile: `analytics_utm_layout_example`
  - execution DSL source target: `analytics_layout_stage_blocks_destination`
  - writer output target: `analytics_layout_stage_blocks_destination`

Recommended commands for layout profile:

Dry-run:
`python -m src.run_profile_analytics --report-id analytics_utm_layout_example --execution-from-sheet-dsl --writer-layout-api-target-dsl-row 14 --writer-layout-api-dry-run --browser-backend openclaw_cdp --tag-selection-mode script`

Real write:
`python -m src.run_profile_analytics --report-id analytics_utm_layout_example --execution-from-sheet-dsl --writer-layout-api-target-dsl-row 14 --writer-layout-api-write --browser-backend openclaw_cdp --tag-selection-mode script`


## Apply Fallback Reliability (2026-04-10)

`analytics_flow` apply path is hardened for UI pointer interception cases.

What changed:
- Added missing diagnostics helper `_dump_apply_button_diagnostics(...)` in `AnalyticsFlow`.
- Diagnostics are best-effort only and cannot crash runtime.
- `_click_apply_in_panel(...)` now confirms apply via short polling instead of one immediate check.
- Click strategy remains deterministic: `normal -> force -> js`.

Success confirmation for apply uses existing runtime signals:
- URL/filter marker confirmation,
- panel/overlay close,
- URL change after click.

## Config Hygiene (2026-04-10)

- Removed placeholder/mojibake-style `????????` value from `config/report_profiles.yaml` (`analytics_tag_layout_example`).
- Current profile config should no longer emit `suspicious_entries=['????????']` warning from config loader.
- Added regression test to guard report profile config against `???` placeholders.
