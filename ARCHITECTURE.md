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
