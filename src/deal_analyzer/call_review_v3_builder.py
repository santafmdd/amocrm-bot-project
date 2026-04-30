from __future__ import annotations

import json
import hashlib
import re
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

from .base_mix import resolve_base_mix
from .call_downloader import CallDownloader
from .call_evidence import CallEvidence
from .call_review_v3_row_schema import CALL_REVIEW_V3_COLUMNS
from .transcription import transcribe_call_evidence

MSK_TZ = timezone(timedelta(hours=3))


CASE_MODE_DISPLAY_MAP = {
    "negotiation_lpr_analysis": "разговор с лпр",
    "secretary_analysis": "разговор с секретарем",
    "supplier_inbound_analysis": "входящий от поставщика",
    "warm_inbound_analysis": "теплый входящий",
    "presentation_analysis": "презентация",
    "test_analysis": "работа с тестом",
    "dozhim_analysis": "дожим",
    "redial_discipline_analysis": "недозвоны / дисциплина дозвонов",
    "unknown_call_case": "неопределенный кейс по звонку",
}

ROLE_DISPLAY_MAP = {
    "telemarketer": "телемаркетолог",
    "sales_manager": "менеджер по продажам",
    "телемаркетолог": "телемаркетолог",
    "менеджер по продажам": "менеджер по продажам",
}

DISALLOWED_CASES_FOR_ACTIVE_WRITE = {
    "skip_no_meaningful_case",
    "redial_discipline_analysis",
}


STAGE_GROUPS: dict[str, dict[str, Any]] = {
    "secretary": {
        "comment_col": "Комментарий по этапу (секретарь)",
        "substage_cols": (
            "Здоровается",
            "Знакомится",
            "Просит соединить с ЛПР",
            "Дает инфоповод",
            "Получает маршрут / результат",
        ),
        "llm_comment_keys": ("stage_secretary_comment",),
    },
    "lpr": {
        "comment_col": "Комментарий по этапу (лпр)",
        "substage_cols": (
            "Здоровается (лпр)",
            "Знакомится (лпр)",
            "Уточняет, что перед ним ЛПР",
            "Обозначает цель звонка",
            "Получает согласие на разговор",
        ),
        "llm_comment_keys": ("stage_lpr_comment",),
    },
    "need": {
        "comment_col": "Комментарий по этапу (актуальность и потребность)",
        "substage_cols": (
            "Открытые вопросы",
            "Уточняющие вопросы",
            "Добивается понимания проблемы",
            "Формирующие вопросы",
            "Не прыгает рано в закрытие",
        ),
        "llm_comment_keys": ("stage_need_comment",),
    },
    "presentation_meeting": {
        "comment_col": "Комментарий по этапу (презентация встречи)",
        "substage_cols": (
            "Обозначает регламент встречи",
            "Озвучивает свойства-выгоды",
            "Получает согласие на ценность",
        ),
        "llm_comment_keys": ("stage_presentation_comment",),
    },
    "closing": {
        "comment_col": "Комментарий по этапу (закрытие на встречу)",
        "substage_cols": (
            "Дает вилку выбора по времени",
            "Фиксирует дату и время",
            "Подтверждает договоренность",
        ),
        "llm_comment_keys": ("stage_closing_comment",),
    },
    "objections": {
        "comment_col": "Комментарий по этапу (отработка возражений)",
        "substage_cols": (
            "Присоединение",
            "Уточнение",
            "Резюмирование",
            "Аргументация",
            "Проверка снятия",
        ),
        "llm_comment_keys": ("stage_objections_comment",),
    },
    "speech": {
        "comment_col": "Комментарий по этапу (чистота речи)",
        "substage_cols": (
            "Мало слов-паразитов",
            "Не перебивает",
            "Благодарит в конце",
            "Речь понятная и собранная",
        ),
        "llm_comment_keys": ("stage_speech_comment",),
    },
    "crm": {
        "comment_col": "Комментарий по этапу (работа с црм)",
        "substage_cols": (
            "Комментарий понятный и лаконичный",
            "Статус актуализирован",
            "Задача запланирована",
            "Доп. поля заполнены при необходимости",
        ),
        "llm_comment_keys": ("stage_crm_comment",),
    },
    "discipline": {
        "comment_col": "Комментарий по этапу (дисциплина дозвонов)",
        "substage_cols": (
            "Покрыты все уникальные номера",
            "Не больше 2 попыток на номер",
            "Попытки в разные дни / время",
            "Не дрочит пустые перезвоны",
        ),
        "llm_comment_keys": ("stage_discipline_comment",),
    },
    "confirm_demo": {
        "comment_col": "Комментарий по этапу (подтверждение презентации)",
        "substage_cols": (
            "Подтверждает дату и время демо",
            "Подтверждает состав участников",
            "Проверяет присутствие ЛПР",
            "Напоминает цель / повестку демо",
            "Проверяет актуальность и готовность",
            "Подтверждает ссылку / формат подключения",
        ),
        "llm_comment_keys": ("stage_confirm_demo_comment", "stage_demo_comment"),
    },
    "demo": {
        "comment_col": "Комментарий по этапу (презентация)",
        "substage_cols": (
            "Вход и рамка демо",
            "Выявление контекста перед показом",
            "Показ релевантного сценария",
            "Привязка к процессу клиента",
            "Работа с вопросами и возражениями на демо",
            "Фиксация следующего шага после демо",
        ),
        "llm_comment_keys": (
            "stage_demo_intro_comment",
            "stage_demo_context_comment",
            "stage_demo_relevant_comment",
            "stage_demo_process_comment",
            "stage_demo_objections_comment",
            "stage_demo_next_step_comment",
            "stage_demo_comment",
        ),
    },
    "test": {
        "comment_col": "Комментарий по этапу (работа с тестом)",
        "substage_cols": (
            "Запуск теста",
            "Критерии успеха теста",
            "Ответственные и сроки по тесту",
            "Сопровождение теста",
            "Сбор обратной связи по тесту",
            "Снятие возражений после теста",
        ),
        "llm_comment_keys": (
            "stage_test_launch_comment",
            "stage_test_criteria_comment",
            "stage_test_owners_comment",
            "stage_test_support_comment",
            "stage_test_feedback_comment",
            "stage_test_objections_comment",
            "stage_test_comment",
        ),
    },
    "dozhim": {
        "comment_col": "Комментарий по этапу (дожим / кп)",
        "substage_cols": (
            "Повторный контакт по дожиму",
            "Работа с сомнениями и стоп-факторами",
            "Согласование условий / КП",
            "Фиксация решения",
            "Дожим не провисает",
        ),
        "llm_comment_keys": (
            "stage_dozhim_recontact_comment",
            "stage_dozhim_doubts_comment",
            "stage_dozhim_terms_comment",
            "stage_dozhim_decision_comment",
            "stage_dozhim_flow_comment",
            "stage_dozhim_comment",
        ),
    },
}

NARRATIVE_USER_FACING_COLUMNS: tuple[str, ...] = (
    "Комментарий по этапу (секретарь)",
    "Комментарий по этапу (лпр)",
    "Комментарий по этапу (актуальность и потребность)",
    "Комментарий по этапу (презентация встречи)",
    "Комментарий по этапу (закрытие на встречу)",
    "Комментарий по этапу (отработка возражений)",
    "Комментарий по этапу (чистота речи)",
    "Комментарий по этапу (работа с црм)",
    "Комментарий по этапу (дисциплина дозвонов)",
    "Комментарий по этапу (подтверждение презентации)",
    "Комментарий по этапу (презентация)",
    "Комментарий по этапу (работа с тестом)",
    "Комментарий по этапу (дожим / кп)",
    "Ключевой вывод",
    "Сильная сторона",
    "Зона роста",
    "Почему это важно",
    "Что закрепить",
    "Что исправить",
    "Что донести сотруднику",
    "Эффект качество",
)

COMMENT_COLUMN_TO_STAGE_GROUP: dict[str, str] = {
    str(meta.get("comment_col") or ""): group
    for group, meta in STAGE_GROUPS.items()
    if str(meta.get("comment_col") or "")
}

STYLE_LITERAL_REPLACEMENTS: tuple[tuple[str, str, str], ...] = (
    ("call_signal_objection_no_need_lit", "call_signal_objection_no_need=true", "клиент сказал, что нет потребности"),
    ("no_need_token", "no_need", "нет потребности"),
    ("no_need", "no need", "нет потребности"),
    ("mixed_signal", "mixed-сигнал", "смешанный интерес"),
    ("link_signal", "link-сигнал", "интерес к линку"),
    ("pain_point", "pain point", "боль клиента"),
    ("discovery", "discovery", "выявление потребности"),
    ("cta", "cta", "следующий шаг"),
    ("decision_maker", "decision maker", "ЛПР"),
    ("the_hook", "the hook", "сильный заход"),
    ("next_steps", "next steps", "следующие шаги"),
    ("warm_lead_handling", "warm lead handling", "работа с теплым лидом"),
    ("single_point_of_failure", "single point of failure", "узкое место"),
    ("framework_en", "framework", "логика подачи"),
    ("framework_ru", "фрейм", "логика подачи"),
    ("crm_upper", "CRM", "црм"),
    ("crm_lower", "crm", "црм"),
    ("email", "email", "почта"),
    ("email_dash", "e-mail", "почта"),
    ("next_step_en", "next step", "следующий шаг"),
    ("follow_up_en", "follow-up", "следующие шаги"),
    ("follow_up_en_alt", "follow up", "следующие шаги"),
    ("qualified_loss", "qualified loss", "потеря по делу"),
    ("anti_fit", "anti-fit", "не наш кейс"),
    ("owner_ambiguity", "owner ambiguity", "кто вел сделку"),
    ("owner_attribution", "owner attribution", "кто вел сделку"),
    ("transcript_en", "transcript", "разговор"),
    ("transcript_ru", "транскрипт", "разговор"),
    ("call_signal_en", "call signal", "признак разговора"),
    ("pipeline_signal_en", "pipeline signal", "признак этапа"),
    ("link_priznak_caps", "LINK-признак", "интерес к линку"),
    ("link_signal_caps", "LINK-сигнал", "интерес к линку"),
    ("budget_qualification_lit", "Budget qualification", "уточнение бюджета"),
    ("not_now_lit", "Not Now", "не сейчас"),
    ("filler_words_lit", "filler words", "слова-паразиты"),
    ("double_better_phrase_lit", "Лучше Лучше сказать", "Лучше сказать"),
)

STYLE_REGEX_REPLACEMENTS: tuple[tuple[str, str, str], ...] = (
    ("call_signal_no_need_regex", r"\bcall_signal_objection_no_need\s*=\s*true\b", "клиент сказал, что нет потребности"),
    ("no_need_token_regex", r"\bno_need\b", "нет потребности"),
    ("no_need_regex", r"\bno need\b", "нет потребности"),
    ("in_signals_phrase_regex", r"\bв\s+сигналах\b", "по разговору"),
    ("call_signal_bool_regex", r"\bпризнак\s+разговора\s*:\s*(?:true|false)\b", "по разговору видно"),
    ("mixed_signal_regex", r"\bmixed[\s\-]?signal\b", "смешанный интерес"),
    ("link_signal_regex", r"\blink[\s\-]?signal\b", "интерес к линку"),
    ("pain_point_regex", r"\bpain point\b", "боль клиента"),
    ("discovery_regex", r"\bdiscovery\b", "выявление потребности"),
    ("cta_regex", r"\bcta\b", "следующий шаг"),
    ("decision_maker_regex", r"\bdecision maker\b", "ЛПР"),
    ("the_hook_regex", r"\bthe hook\b", "сильный заход"),
    ("next_steps_regex", r"\bnext steps?\b", "следующие шаги"),
    ("not_now_regex", r"\bnot now\b", "не сейчас"),
    ("budget_qualification_regex", r"\bbudget qualification\b", "уточнение бюджета"),
    ("filler_words_regex", r"\bfiller words\b", "слова-паразиты"),
    ("call_signal_token_regex", r"\bcall_signal[^\s,;:.]*", "по разговору видно"),
    ("warm_lead_handling_regex", r"\bwarm lead handling\b", "работа с теплым лидом"),
    ("single_point_of_failure_regex", r"\bsingle point of failure\b", "узкое место"),
    ("info_caps_regex", r"\bINFO\b", "инфо"),
    ("link_caps_regex", r"\bLINK\b", "линк"),
    ("framework_regex", r"\bframework\b", "логика подачи"),
    ("framework_ru_regex", r"\bфрейм\b", "логика подачи"),
    ("crm_anycase_regex", r"\bcrm\b", "црм"),
    ("signal_ru", r"\bсигнал(?:ы|а|ов)?\b", "признак"),
    ("signal_en", r"\bsignal\b", "признак"),
    ("mixed_en", r"\bmixed\b", "неоднозначная реакция"),
    ("module_ru", r"\bмодул[ьяеи]\b", "подход"),
    ("technique_ru", r"\bтехник[аи]\b", "подход"),
    ("method_ru", r"\bметод(?:а|ом|е|ы)?\b", "как делать"),
    ("approach_ru", r"\bподход(?:а|ом|е|ы)?\b", "как действовать"),
    ("structured_form_phrase_ru", r"\bструктурированн(?:ая|ый)\s+формулировк[аи]\b", "четкая формулировка"),
    ("dialogue_form_phrase_ru", r"\bформулировк[аи]\s+разговор[а-я]*\b", "четкая фраза"),
    ("script_ru", r"\bскрипт(?:ы|ом|а|у)?\b", "формулировка разговора"),
    ("priem_ru", r"\bпри[её]м(?:ы|а|ом|у)?\b", "формулировка"),
    ("working_step_ru", r"\bрабочий шаг\b", "действие"),
    ("zones_growth_ru", r"\bзоны роста\b", "что провисает"),
    ("zone_growth_ru", r"\bточка роста\b", "что провисает"),
    ("training_phrase_1", r"\bэксперт,\s*а не продавец\b", "говори по делу и без нажима"),
    ("training_phrase_2", r"\bдириж[её]р разговора\b", "веди разговор"),
    ("training_phrase_3", r"\bфасилитатор сделки\b", "ведущий по сделке"),
    ("training_phrase_4", r"\bагрессивная диагностика\b", "жесткая диагностика"),
    ("training_phrase_5", r"\bпрепарировани[ея]\b", "разбор"),
    ("training_phrase_6", r"\bповышенная уверенность\b", "уверенный тон"),
    ("training_phrase_7", r"\bворонка внимания\b", "фокус разговора"),
    ("training_phrase_8", r"\bключ к количеству\b", "это влияет на результат в штуках"),
    ("training_phrase_9", r"\bутечка клиентов\b", "потеря клиентов"),
    ("employees_must", r"\bСотрудник должен\b", "Нужно"),
    ("broken_word_need", r"\bнужно\s+ч\s*тко\b", "нужно четко"),
    ("broken_word_nechetko", r"\bнеч\s*тко\b", "нечетко"),
    ("broken_word_chetko", r"(?<!не)\bч\s*тко\b", "четко"),
    ("broken_word_ask", r"\bсразу\s+спраши\b", "сразу спрашивать"),
    ("double_better_phrase_regex", r"\bЛучше\s+Лучше\s+сказать\b", "Лучше сказать"),
    ("bool_flags_regex", r"\b(?:true|false)\b", ""),
)

FORBIDDEN_ENGLISH_TERM_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bno_need\b", re.IGNORECASE),
    re.compile(r"\bno need\b", re.IGNORECASE),
    re.compile(r"\bв\s+сигналах\b", re.IGNORECASE),
    re.compile(r"\bпризнак\s+разговора\b", re.IGNORECASE),
    re.compile(r"\btrue\b", re.IGNORECASE),
    re.compile(r"\bfalse\b", re.IGNORECASE),
    re.compile(r"\bINFO\b", re.IGNORECASE),
    re.compile(r"\bLINK\b", re.IGNORECASE),
    re.compile(r"\bmixed[\s\-]?(?:signal|сигнал)\b", re.IGNORECASE),
    re.compile(r"\blink[\s\-]?(?:signal|сигнал)\b", re.IGNORECASE),
    re.compile(r"\bpain point\b", re.IGNORECASE),
    re.compile(r"\bdiscovery\b", re.IGNORECASE),
    re.compile(r"\bcta\b", re.IGNORECASE),
    re.compile(r"\bdecision maker\b", re.IGNORECASE),
    re.compile(r"\bthe hook\b", re.IGNORECASE),
    re.compile(r"\bnext steps?\b", re.IGNORECASE),
    re.compile(r"\bwarm lead handling\b", re.IGNORECASE),
    re.compile(r"\bsingle point of failure\b", re.IGNORECASE),
    re.compile(r"\bframework\b", re.IGNORECASE),
    re.compile(r"\bcall_signal(?:[_a-z0-9=]+)?\b", re.IGNORECASE),
    re.compile(r"\blink[\s\-]признак\b", re.IGNORECASE),
    re.compile(r"\blink[\s\-]сигнал\b", re.IGNORECASE),
    re.compile(r"\bbudget qualification\b", re.IGNORECASE),
    re.compile(r"\bnot now\b", re.IGNORECASE),
    re.compile(r"\bfiller words\b", re.IGNORECASE),
    re.compile(r"\bmixed\b", re.IGNORECASE),
    re.compile(r"\bsignal\b", re.IGNORECASE),
)

TRAINING_JARGON_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bметод(?:а|ом|е|ы)?\b", re.IGNORECASE),
    re.compile(r"\bтехник[аи]\b", re.IGNORECASE),
    re.compile(r"\bподход(?:а|ом|е|ы)?\b", re.IGNORECASE),
    re.compile(r"\bпри[её]м(?:ы|а|ом|у)?\b", re.IGNORECASE),
    re.compile(r"\bмодул[ьяеи]\b", re.IGNORECASE),
    re.compile(r"\bрабочий шаг\b", re.IGNORECASE),
    re.compile(r"\bфрейм\b", re.IGNORECASE),
    re.compile(r"\bскрипт(?:ы|ом|а|у)?\b", re.IGNORECASE),
    re.compile(r"эксперт,\s*а не продавец", re.IGNORECASE),
    re.compile(r"дириж[её]р разговора", re.IGNORECASE),
    re.compile(r"фасилитатор сделки", re.IGNORECASE),
    re.compile(r"агрессивная диагностика", re.IGNORECASE),
    re.compile(r"препарировани[ея]", re.IGNORECASE),
    re.compile(r"повышенная уверенность", re.IGNORECASE),
    re.compile(r"\bсигнал(?:ы|а|ов)?\b", re.IGNORECASE),
    re.compile(r"воронка внимания", re.IGNORECASE),
    re.compile(r"ключ к количеству", re.IGNORECASE),
    re.compile(r"точка роста", re.IGNORECASE),
    re.compile(r"структурированн(?:ая|ый)\s+формулировк[аи]", re.IGNORECASE),
    re.compile(r"формулировк[аи]\s+разговор[а-я]*", re.IGNORECASE),
)

TECHNICAL_TERMS_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bllm\b", re.IGNORECASE),
    re.compile(r"\btranscript\b", re.IGNORECASE),
    re.compile(r"\bтранскрипт\b", re.IGNORECASE),
    re.compile(r"\bрасшифровк[а-я]*\b", re.IGNORECASE),
    re.compile(r"\bcall signal\b", re.IGNORECASE),
    re.compile(r"\bpipeline signal\b", re.IGNORECASE),
    re.compile(r"\bcall_signal(?:[_a-z0-9=]+)?\b", re.IGNORECASE),
)

FINAL_PAYLOAD_EXCLUDED_COLUMNS_NORM: set[str] = {
    "deal id",
    "ссылка на сделку",
    "сделка",
    "база / тег",
}

FINAL_PAYLOAD_TRUNCATED_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"(?:\bи|\bк|\bчтобы|например)\s*$", re.IGNORECASE),
    re.compile(r":\s*$"),
    re.compile(r"\bЛучше\s*$", re.IGNORECASE),
    re.compile(r"Лучше\s+Лучше\s+сказать", re.IGNORECASE),
    re.compile(r"\bИспользуй:\s*$", re.IGNORECASE),
    re.compile(r"\bЧто исправить:\s*$", re.IGNORECASE),
    re.compile(r"\bнеч\s*тко\b", re.IGNORECASE),
    re.compile(r"\bч\s*тко\b", re.IGNORECASE),
    re.compile(r'Лучше сказать:\s*"?[^"]*$', re.IGNORECASE),
    re.compile(r'Используй:\s*"?[^"]*$', re.IGNORECASE),
    re.compile(r"нужно\s+ч\s*тко", re.IGNORECASE),
    re.compile(r"\bсразу\s+спраши(?![а-я])", re.IGNORECASE),
)

GENERIC_BETTER_PHRASE = "давайте сразу зафиксируем конкретный следующий шаг и время"
BETTER_PHRASE_RE = re.compile(r'(?:Лучше сказать|Используй):\s*"([^"]+)"', re.IGNORECASE)
BETTER_PHRASE_MARKER_RE = re.compile(r"(?:Лучше\s+сказать|Используй)\s*:", re.IGNORECASE)

CRM_STAGE_MISMATCH_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"(?:в\s*(?:црм|crm)\s*(?:стадия|этап|сделка)[^.!\n]*по\s+факту[^.!\n]*)", re.IGNORECASE),
    re.compile(r"(?:в\s*(?:црм|crm)[^.!\n]*по\s+факту[^.!\n]*)", re.IGNORECASE),
    re.compile(r"(?:по\s*(?:црм|crm)[^.!\n]*по\s+факту[^.!\n]*)", re.IGNORECASE),
    re.compile(r"(?:стади[яи]\s*(?:в\s*)?(?:црм|crm)[^.!\n]*не\s*соответ[^.!\n]*)", re.IGNORECASE),
    re.compile(r"(?:стади[яи]\s*(?:в\s*)?(?:црм|crm)[^.!\n]*расход[^.!\n]*)", re.IGNORECASE),
    re.compile(r"(?:по\s+этапу\s*(?:црм|crm)[^.!\n]*)", re.IGNORECASE),
    re.compile(r"(?:(?:црм|crm)\s*не\s*отража[е-я]*\s*реальност[ьи][^.!\n]*)", re.IGNORECASE),
)

BAD_PHRASE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bкак\s+действовать\s+(?:копания|управления|выявления)\b", re.IGNORECASE),
    re.compile(
        r"\bкак\s+действовать\s+(?:квалификаци[а-я]*|выявлени[а-я]*|фиксаци[а-я]*|работ[а-я]*|презентаци[а-я]*|отработк[а-я]*)\b",
        re.IGNORECASE,
    ),
    re.compile(r"\b(?:пройти|разобрать|отработать)\s+как\s+действовать\s+[а-яё][а-яё\s-]{2,}\b", re.IGNORECASE),
    re.compile(r"\bкак\s+действовать\s+[а-яё-]+(?:ии|ения|ания|ости|ции|ки|ства|ого|его)\b", re.IGNORECASE),
    re.compile(r"\bтренировать\s+четк(?:ая|ую)\s+фраз(?:а|у)\b", re.IGNORECASE),
    re.compile(r"\bотработать\s+быстрого\b", re.IGNORECASE),
    re.compile(r"\bотработать\s+глубиннн?ого\b", re.IGNORECASE),
    re.compile(r"\bкак\s+действовать\b", re.IGNORECASE),
    re.compile(r"\bлучше\s+лучше\s+сказать\b", re.IGNORECASE),
    re.compile(r"\bнеч\s*тко\b", re.IGNORECASE),
    re.compile(r"(?<!не)\bч\s*тко\b", re.IGNORECASE),
    re.compile(r"Лучше\s*:\s*-\s*", re.IGNORECASE),
    re.compile(r"Вместо\s+нужно\s*:\s*-\s*", re.IGNORECASE),
    re.compile(r"\b1\)\s*1\.", re.IGNORECASE),
)

EMPTY_SPEECH_MODULE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r'(?:Используй|Лучше сказать)\s*:\s*(?:""|«»)\s*', re.IGNORECASE),
    re.compile(r'(?:Используй|Лучше сказать)\s*:\s*(?=$|[.,;:!?])', re.IGNORECASE),
)

AUTOANSWER_SKIP_REASONS: set[str] = {
    "autoanswer_no_meaningful_conversation",
    "short_no_meaningful_conversation",
    "no_human_dialogue",
}

AUTOANSWER_TRANSCRIPT_MARKERS: tuple[str, ...] = (
    "абонент недоступен",
    "абонент не может ответить",
    "оставьте сообщение",
    "после сигнала",
    "голосовая почта",
    "автоответчик",
    "автоответ",
    "voicemail",
    "соединение установлено",
)

DIALOGUE_TRANSCRIPT_MARKERS: tuple[str, ...] = (
    "лпр",
    "директор",
    "закуп",
    "встреч",
    "демо",
    "тест",
    "кп",
    "договор",
    "счет",
    "следующий шаг",
    "договорились",
)

IVR_TRANSCRIPT_MARKERS: tuple[str, ...] = (
    "вас приветствует",
    "нажмите",
    "тональном режиме",
    "добавочный номер",
    "введите добавочный",
    "для связи с",
    "если вы знаете внутренний номер",
)

HUMAN_DIALOGUE_AFTER_NOISE_MARKERS: tuple[str, ...] = (
    "это рустам",
    "компания айсток",
    "не отвлек",
    "по какому вопросу звоню",
    "по какому вопросу",
    "хотел предложить",
    "могу предложить",
    "показать",
    "потестировать",
    "отправьте на почту",
    "давайте после",
)

BETTER_PHRASE_VARIANTS: dict[str, tuple[str, ...]] = {
    "test": (
        "Давайте договоримся так: вы проверяете 2 сценария до четверга, в пятницу созваниваемся и фиксируем результат.",
        "Фиксируем тест по-взрослому: что проверяем, кто отвечает и к какой дате даете обратную связь.",
        "Чтобы тест не завис, сразу пишем критерии, ответственного и точный срок обратной связи.",
    ),
    "demo": (
        "За 15 минут покажу именно ваш сценарий, в конце закрепим конкретный следующий шаг.",
        "Сначала рамка демо на 15 минут, потом показ под вашу задачу и сразу фиксируем продолжение.",
        "Покажу не все подряд, а только релевантный кусок под ваш процесс, дальше сразу закрепим действие.",
    ),
    "confirm_demo": (
        "Подтвердите дату, состав участников и формат подключения, чтобы демо не сорвалось.",
        "Давайте сверим слот, участников и ссылку заранее, чтобы не потерять встречу.",
        "Перед стартом демо проверяем время, состав и формат подключения без подвисаний.",
    ),
    "lpr": (
        "Подскажите, вы сами принимаете решение или нужно подключить еще кого-то?",
        "Чтобы не ходить по кругу: вы финально решаете вопрос или нужен еще один согласующий?",
        "Сразу уточню: вы ЛПР по этому вопросу или подключаем коллегу с правом решения?",
    ),
    "need": (
        "Что сейчас сильнее всего тормозит процесс: люди, сроки, согласования или ручной контроль?",
        "Где у вас самая больная точка сейчас: скорость, ошибки, согласования или нагрузка на команду?",
        "Давайте разложим по фактам: где теряется время и из-за чего процесс буксует сильнее всего?",
    ),
    "closing": (
        "Давайте не оставлять это в воздухе: вторник 11:00 или среда 15:00 удобнее?",
        "Фиксируем конкретно: вам удобнее вторник 11:00 или среда 15:00?",
        "Чтобы не растянуть вопрос, сразу выберем слот - вторник 11:00 или среда 15:00.",
    ),
    "objections": (
        "Понимаю сомнение. Давайте уточню один момент и проверим, закрыт ли вопрос.",
        "Согласен, возражение важное. Давайте уточним детали и проверим, что сняли риск.",
        "Принял возражение. Сейчас коротко разберем и проверим, остались ли сомнения.",
    ),
    "presentation_meeting": (
        "Сначала коротко рамка встречи, потом ценность под ваш кейс без лишних кругов.",
        "Открываю встречу коротко: цель, регламент и что получите на выходе.",
        "Держим структуру встречи: рамка, ценность по вашему кейсу и четкий следующий шаг.",
    ),
    "secretary": (
        "Подскажите, кто ведет этот вопрос и как правильно выйти на нужного человека?",
        "Скажите, к кому лучше адресовать этот вопрос и как быстрее попасть к ответственному?",
        "Чтобы не ходить по кругу, подскажите контакт и маршрут до человека, кто решает вопрос.",
    ),
    "discipline": (
        "Меняем время прозвона и закрываем все номера, без пустых повторов в один слот.",
        "Не долбим один номер: разносим попытки по времени и закрываем весь список контактов.",
        "Ставим дисциплину: максимум две попытки на номер и полный охват всех контактов.",
    ),
    "crm": (
        "После звонка фиксируем в црм: с кем говорили, что болит, какой следующий шаг и дата.",
        "Сразу после разговора пишем в црм факт, договоренность и срок следующего касания.",
        "Чтобы ничего не терялось, заносим в црм суть разговора и конкретную дату следующего шага.",
    ),
    "speech": (
        "Сократи старт: имя, компания, причина звонка и один вопрос по делу.",
        "Говори короче и увереннее: без лишних заходов, сразу к сути.",
        "Убираем слова-паразиты и длинный разгон, оставляем четкий заход по делу.",
        "Начинай без раскачки: кто ты, зачем звонишь и какой вопрос нужно решить прямо сейчас.",
        "Держи ритм разговора: короткий старт, четкая цель и без лишних вводных.",
        "Собери речь плотнее: меньше лишних слов, больше конкретики в первой минуте.",
    ),
    "dozhim": (
        "Давайте согласуем условия и сегодня зафиксируем решение, без провисаний.",
        "Чтобы не растягивать, сегодня подтверждаем условия и финальное решение.",
        "Закрываем вопрос в этом касании: условия, решение и точная фиксация в црм.",
    ),
    "generic": (
        "Давайте зафиксируем следующий шаг и дату, чтобы не размывать договоренность.",
        "Лучше сразу назвать конкретный шаг и срок, без размытых формулировок.",
        "Оставляем после звонка только конкретику: что делаем и к какой дате.",
    ),
}


CASE_MODE_STAGE_MATRIX_DEFAULT: dict[str, set[str]] = {
    "test_analysis": {"test", "speech", "crm"},
    "presentation_analysis": {"demo", "speech", "crm"},
    "warm_inbound_analysis": {"lpr", "need", "speech", "crm"},
    "supplier_inbound_analysis": {"lpr", "need", "speech", "crm"},
    "negotiation_lpr_analysis": {"lpr", "need", "speech", "crm"},
    "secretary_analysis": {"secretary", "speech", "crm"},
    "redial_discipline_analysis": {"discipline", "crm"},
    "dozhim_analysis": {"dozhim", "objections", "speech", "crm"},
}


CASE_MODE_PRIMARY_GROUP: dict[str, str] = {
    "test_analysis": "test",
    "presentation_analysis": "demo",
    "warm_inbound_analysis": "lpr",
    "supplier_inbound_analysis": "lpr",
    "negotiation_lpr_analysis": "lpr",
    "secretary_analysis": "secretary",
    "redial_discipline_analysis": "discipline",
    "dozhim_analysis": "dozhim",
}

CASE_MODE_CROSS_STAGE_ALLOWED: dict[str, set[str]] = {
    "test_analysis": {"objections", "dozhim"},
    "presentation_analysis": {"need", "objections", "confirm_demo"},
    "warm_inbound_analysis": {"presentation_meeting", "closing", "objections"},
    "supplier_inbound_analysis": {"presentation_meeting", "closing", "objections"},
    "negotiation_lpr_analysis": {"presentation_meeting", "closing", "objections"},
    "secretary_analysis": {"discipline"},
    "redial_discipline_analysis": set(),
    "dozhim_analysis": {"objections", "test"},
}


class _NoopLogger:
    def info(self, *args, **kwargs) -> None:
        _ = args, kwargs

    def warning(self, *args, **kwargs) -> None:
        _ = args, kwargs


def _contact_only_transcript_failure_reason(*, recording_url: str, status: str, error: str, audio_path: str) -> str:
    if not recording_url:
        return "missing_recording_url"
    if not audio_path and status in {"missing_audio_file", "download_failed", "missing_audio"}:
        return "download_failed"
    if status in {"backend_unavailable", "backend_error", "not_configured", "disabled"}:
        return "stt_backend_error"
    if status in {"empty_transcript_after_stt", "empty_transcript"}:
        return "empty_transcript_after_stt"
    if error:
        return _sanitize_user_text(error)[:180]
    return "stt_backend_error"


def _run_contact_only_stt(
    *,
    cfg: Any | None,
    logger: Any | None,
    deal_id: str,
    call_item: dict[str, Any],
) -> dict[str, Any]:
    recording_url = str(call_item.get("recording_url") or "").strip()
    call_id = str(call_item.get("call_id") or "").strip()
    duration_seconds = int(call_item.get("call_duration_seconds", 0) or 0)
    manager_name = _sanitize_person_name(str(call_item.get("manager_name_from_call_author") or ""))

    if not recording_url:
        return {
            "transcript_text": "",
            "transcript_chars": 0,
            "transcript_status": "missing_recording_url",
            "transcript_error": "missing_recording_url",
            "transcript_source": "",
            "audio_path": "",
            "audio_download_status": "missing_url",
        }
    if cfg is None:
        return {
            "transcript_text": "",
            "transcript_chars": 0,
            "transcript_status": "stt_backend_error",
            "transcript_error": "stt_config_unavailable_for_contact_only",
            "transcript_source": "",
            "audio_path": "",
            "audio_download_status": "not_attempted",
        }

    run_logger = logger if logger is not None else _NoopLogger()
    downloader = CallDownloader(config=cfg, logger=run_logger)
    call_evidence = CallEvidence(
        call_id=call_id or f"contact_call_{deal_id}",
        deal_id=deal_id,
        manager_id=str(call_item.get("manager_id_from_call_author") or ""),
        manager_name=manager_name,
        timestamp=str(call_item.get("call_datetime_utc") or ""),
        duration_seconds=max(0, int(duration_seconds)),
        direction=str(call_item.get("direction") or ""),
        source_location=str(call_item.get("source_location") or "contact_only_fallback"),
        recording_url=recording_url,
        recording_ref=call_id or "",
        quality_flags=[],
        missing_recording=False,
        audio_path=str(call_item.get("audio_path") or ""),
        entity_type=str(call_item.get("entity_type") or "contact_only"),
        contact_id=str(call_item.get("contact_id") or ""),
        contact_url=str(call_item.get("contact_url") or ""),
        phone=str(call_item.get("phone_raw") or ""),
        resolution_reason=str(call_item.get("resolution_reason") or ""),
    )
    resolved_call = downloader._resolve_call_audio(call_evidence, audio_call_ids={call_evidence.call_id})
    call_payload = {
        "call_id": resolved_call.call_id,
        "deal_id": resolved_call.deal_id,
        "recording_url": resolved_call.recording_url,
        "audio_path": resolved_call.audio_path,
        "duration_seconds": resolved_call.duration_seconds,
    }
    transcript_items = transcribe_call_evidence(calls=[call_payload], config=cfg, logger=run_logger)
    transcript_item = transcript_items[0] if transcript_items else {}
    transcript_text = _sanitize_user_text(str(transcript_item.get("transcript_text") or ""))
    transcript_chars = int(transcript_item.get("transcript_chars", 0) or len(transcript_text))
    transcript_status = str(transcript_item.get("transcript_status") or "").strip()
    transcript_error = str(transcript_item.get("transcript_error") or "").strip()
    transcript_source = str(transcript_item.get("transcript_source") or "").strip()
    return {
        "transcript_text": transcript_text,
        "transcript_chars": transcript_chars,
        "transcript_status": transcript_status,
        "transcript_error": transcript_error,
        "transcript_source": transcript_source,
        "audio_path": str(resolved_call.audio_path or ""),
        "audio_download_status": str(resolved_call.audio_download_status or ""),
    }


def _build_contact_only_stub_llm_fields(*, manager_name: str) -> dict[str, str]:
    manager = _sanitize_person_name(manager_name) or "менеджер"
    return {
        "primary_case_type": "разговор с лпр",
        "relevant_stage_groups": "[\"lpr\",\"need\",\"presentation_meeting\",\"closing\",\"objections\",\"speech\",\"crm\"]",
        "key_takeaway": "В разбор попал звонок из карточки контакта без привязанной сделки. Нужна фиксация следующего шага и перенос в управляемую воронку.",
        "strong_sides": f"{manager} удержал разговор и довел диалог до содержательного контакта.",
        "growth_zones": "Не хватает прозрачной привязки к сделке и формальной фиксации следующего шага в рабочем контуре.",
        "why_important": "Без привязки к сделке и конкретного next step разговор не превращается в управляемый результат.",
        "reinforce": "Закрепить короткий заход, уточнение роли собеседника и явную фиксацию даты следующего шага.",
        "fix_action": "Сразу после звонка привязать контакт к сделке и зафиксировать следующий шаг с датой и ответственным.",
        "coaching_list": "1) Звонок найден в карточке контакта.\n2) Добавляем дисциплину привязки к сделке.\n3) Контролируем следующий шаг в карточке.",
        "expected_quantity": "Рост доли звонков, которые доводятся до управляемого следующего шага.",
        "expected_quality": "Снижение потерь на контактах без привязки и лучшее качество фиксации в црм.",
        "stage_lpr_comment": "Разговор был с содержательным контактом, но нужно жестче фиксировать роль собеседника и переход к следующему шагу.",
        "stage_need_comment": "Нужно глубже проверять контекст задачи клиента и переводить диалог в конкретный шаг по сроку.",
        "stage_presentation_comment": "Ценность лучше связывать с задачей клиента и завершать разговор конкретной договоренностью.",
        "stage_closing_comment": "Фиксация следующего шага должна быть с датой и временем, без размытых формулировок.",
        "stage_objections_comment": "При возражениях держать структуру: присоединение, уточнение, конкретный следующий шаг.",
        "stage_speech_comment": "Речь держать короткой и управляемой, без длинных вводных.",
        "stage_crm_comment": "После звонка фиксировать итог и привязку контакта к сделке, чтобы не терять управление.",
    }


def _build_contact_only_case_from_ledger_fallback(
    *,
    candidate: dict[str, Any],
    deal_id: str,
    call_ledger_all: list[dict[str, Any]],
    base_domain: str,
    allowlist: list[str],
    manager_role_registry: dict[str, str],
    cfg: Any | None,
    logger: Any | None,
) -> dict[str, Any] | None:
    if not str(deal_id or "").startswith("contact_"):
        return None

    selected_call_ids = _selected_call_ids(candidate=candidate, record={})
    selected_call_id = _selected_call_id_for_readiness(candidate=candidate, record={})
    contact_call_items = []
    for raw in call_ledger_all:
        if not isinstance(raw, dict):
            continue
        if str(raw.get("deal_id") or "").strip() != str(deal_id):
            continue
        contact_call_items.append(raw)
    if not contact_call_items:
        return None

    anchor_call: dict[str, Any] | None = None
    if selected_call_id:
        for item in contact_call_items:
            if str(item.get("call_id") or "").strip() == selected_call_id:
                anchor_call = item
                break
    if anchor_call is None and selected_call_ids:
        for item in contact_call_items:
            if str(item.get("call_id") or "").strip() in selected_call_ids:
                anchor_call = item
                break
    if anchor_call is None:
        anchor_call = sorted(
            contact_call_items,
            key=lambda x: (
                -int(x.get("call_duration_seconds", 0) or 0),
                str(x.get("call_datetime_utc") or ""),
                str(x.get("call_id") or ""),
            ),
        )[0]

    anchor_duration = int(anchor_call.get("call_duration_seconds", 0) or 0)
    if anchor_duration < 25:
        return None
    recording_url = str(anchor_call.get("recording_url") or "").strip()
    if not recording_url:
        return None

    manager_name = _sanitize_person_name(
        str(anchor_call.get("manager_name_from_call_author") or candidate.get("anchor_manager_name") or "")
    )
    if allowlist and not _is_manager_allowed(manager_name=manager_name, allowlist=allowlist):
        return None
    manager_role_internal = _resolve_manager_role(manager_name=manager_name, registry=manager_role_registry)
    manager_role_display = ROLE_DISPLAY_MAP.get(manager_role_internal, "менеджер по продажам")

    anchor_ts = _parse_ts(anchor_call.get("call_datetime_utc") or candidate.get("anchor_call_timestamp"))
    analysis_date = _choose_analysis_date(candidate=candidate, anchor_ts=anchor_ts)
    case_date = _choose_case_date(anchor_ts=anchor_ts)
    contact_id = str(anchor_call.get("contact_id") or "").strip() or str(deal_id).replace("contact_", "", 1)
    deal_url = str(anchor_call.get("contact_url") or "").strip()
    if not deal_url and contact_id and base_domain:
        deal_url = f"{base_domain.rstrip('/')}/contacts/detail/{contact_id}"
    deal_name = _sanitize_user_text(str(anchor_call.get("deal_name") or "").strip())
    if not deal_name:
        deal_name = f"Контакт #{contact_id}" if contact_id else "Контакт без привязанной сделки"

    stt_payload = _run_contact_only_stt(
        cfg=cfg,
        logger=logger,
        deal_id=str(deal_id),
        call_item=anchor_call,
    )
    transcript_text = _sanitize_user_text(str(stt_payload.get("transcript_text") or ""))
    transcript_chars = int(stt_payload.get("transcript_chars", 0) or len(transcript_text))
    transcript_status = str(stt_payload.get("transcript_status") or "").strip().lower()
    transcript_error = str(stt_payload.get("transcript_error") or "").strip()
    transcript_source = str(stt_payload.get("transcript_source") or "").strip()
    anchor_audio_path = str(stt_payload.get("audio_path") or "").strip()
    audio_download_status = str(stt_payload.get("audio_download_status") or "").strip()
    noise_profile = _noise_dialogue_profile(
        transcript_text=transcript_text,
        transcript_chars=int(transcript_chars or 0),
        call_duration_seconds=anchor_duration,
    )
    llm_fields = _build_contact_only_stub_llm_fields(manager_name=manager_name)
    usability_reason = ""
    if transcript_chars < 20:
        usability_reason = _contact_only_transcript_failure_reason(
            recording_url=recording_url,
            status=transcript_status,
            error=transcript_error,
            audio_path=anchor_audio_path,
        )

    return {
        "deal_id": str(deal_id),
        "deal_url": deal_url,
        "deal_name": deal_name,
        "company_name": "",
        "manager_name": manager_name,
        "manager_role_internal": manager_role_internal,
        "manager_role_display": manager_role_display,
        "analysis_date": analysis_date,
        "case_date": case_date,
        "case_mode": "negotiation_lpr_analysis",
        "case_type_display": "звонок на контакте без привязанной сделки",
        "product_focus": _sanitize_user_text(str(anchor_call.get("product_focus") or "контакт без привязки к сделке")),
        "base_mix": _sanitize_user_text(str(anchor_call.get("base_tag") or "контакт")),
        "listened_calls": "1",
        "llm_fields": llm_fields,
        "quote_text": "",
        "score": 60,
        "score_source": "contact_only_fallback",
        "score_reason": "contact_call_without_linked_deal",
        "score_components": {"base": 60},
        "criticality": _criticality_from_score(60),
        "anchor_call_id": str(anchor_call.get("call_id") or ""),
        "anchor_call_timestamp": str(anchor_call.get("call_datetime_utc") or candidate.get("anchor_call_timestamp") or ""),
        "anchor_call_duration_seconds": anchor_duration,
        "selected_call_count": 1,
        "selected_call_ids": [str(anchor_call.get("call_id") or "")],
        "llm_source": "contact_only_fallback",
        "transcript_length_chars": transcript_chars,
        "transcript_segments_count": 0,
        "transcript_longest_segment_sec": 0.0,
        "transcript_usability_label": "usable" if transcript_chars >= 20 else "unknown",
        "transcript_usability_reason": usability_reason,
        "initial_noise_detected": bool(noise_profile.get("initial_noise_detected")),
        "human_dialogue_after_noise_detected": bool(noise_profile.get("human_dialogue_after_noise_detected")),
        "noise_classification": str(noise_profile.get("noise_classification") or "contact_only_fallback"),
        "final_noise_decision": str(
            noise_profile.get("final_noise_decision")
            or ("sent_to_llm_contact_only" if transcript_chars >= 20 else "skip_noise_only")
        ),
        "llm_allowed": True,
        "case_type_source": "contact_only_fallback",
        "case_type_reason": "contact_notes_call_without_period_deal_record",
        "case_type_evidence_used": ["contacts_notes_fallback", "contact_only_case"],
        "call_review_llm_provenance": {
            "source": "contact_only_fallback",
            "source_of_truth": "contact_ledger",
            "runtime_metrics": {},
        },
        "anchor_audio_path": anchor_audio_path,
        "anchor_recording_url": recording_url,
        "transcript_source": transcript_source or "contact_only_stt",
        "transcript_status": transcript_status,
        "transcript_error": transcript_error,
        "audio_download_status": audio_download_status,
    }


def build_call_review_v3_payload(
    *,
    summary: dict[str, Any],
    period_deal_records: list[dict[str, Any]],
    analysis_shortlist_payload: dict[str, Any],
    base_domain: str,
    manager_allowlist: list[str] | tuple[str, ...] | None,
    manager_role_registry: dict[str, str] | None,
    run_dir: Path | None = None,
    call_ledger_all: list[dict[str, Any]] | None = None,
    call_ledger_audit: dict[str, Any] | None = None,
    anchor_shortlist: list[dict[str, Any]] | None = None,
    selected_anchor_cases: list[dict[str, Any] | Any] | None = None,
    abort_stage: str = "",
    abort_error: str = "",
    artifacts_written: list[str] | None = None,
    cfg: Any | None = None,
    logger: Any | None = None,
) -> dict[str, Any]:
    records_by_deal = {
        str(item.get("deal_id") or "").strip(): item
        for item in period_deal_records
        if isinstance(item, dict) and str(item.get("deal_id") or "").strip()
    }
    shortlist_items = (
        analysis_shortlist_payload.get("selected_items", [])
        if isinstance(analysis_shortlist_payload.get("selected_items"), list)
        else []
    )
    allowlist = [str(x or "").strip() for x in (manager_allowlist or []) if str(x or "").strip()]

    ledger: list[dict[str, Any]] = []
    cases_debug: list[dict[str, Any]] = []
    transcript_readiness_entries: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    row_entries: list[dict[str, Any]] = []
    skipped_reasons: dict[str, int] = {}

    for order, candidate in enumerate(shortlist_items, start=1):
        if not isinstance(candidate, dict):
            continue
        deal_id = str(candidate.get("deal_id") or "").strip()
        if not deal_id:
            _bump(skipped_reasons, "missing_deal_id")
            continue
        record = records_by_deal.get(deal_id)
        if not isinstance(record, dict):
            contact_case = _build_contact_only_case_from_ledger_fallback(
                candidate=candidate,
                deal_id=deal_id,
                call_ledger_all=call_ledger_all or [],
                base_domain=base_domain,
                allowlist=allowlist,
                manager_role_registry=manager_role_registry or {},
                cfg=cfg,
                logger=logger,
            )
            if isinstance(contact_case, dict):
                row, row_semantic_debug = _build_row_from_case(case=contact_case)
                row["Тип кейса"] = "звонок на контакте без привязанной сделки"
                row["Ссылка на сделку"] = str(contact_case.get("deal_url") or row.get("Ссылка на сделку") or "")
                row["Сделка"] = str(contact_case.get("deal_name") or row.get("Сделка") or "")
                row["Ключевой вывод"] = (
                    "Звонок найден в карточке контакта, сделка не привязана. "
                    + str(row.get("Ключевой вывод") or "")
                ).strip()
                row["Что донести сотруднику"] = (
                    "Звонок найден в карточке контакта, сделка не привязана. "
                    + str(row.get("Что донести сотруднику") or "")
                ).strip()
                row_entries.append(
                    {
                        "row": row,
                        "semantic_debug": row_semantic_debug,
                    }
                )
                cases_debug.append(
                    {
                        "order": order,
                        "deal_id": deal_id,
                        "pool_type": str(candidate.get("pool_type") or ""),
                        "call_case_type": str(candidate.get("call_case_type") or ""),
                        "shortlist_reason": str(candidate.get("shortlist_reason") or ""),
                        "selected_for_transcription": bool(candidate.get("selected_for_transcription")),
                        "selected_call_count": int(candidate.get("selected_call_count", 0) or 0),
                        "analysis_shortlist_rank_group": int(candidate.get("rank_group", 0) or 0),
                        "anchor_manager_name": str(candidate.get("anchor_manager_name") or candidate.get("manager_name_from_call_author") or ""),
                        "skipped": False,
                        "manager_name": str(contact_case.get("manager_name") or ""),
                        "role": str(contact_case.get("manager_role_display") or ""),
                        "case_mode": str(contact_case.get("case_mode") or ""),
                        "case_type_display": "звонок на контакте без привязанной сделки",
                        "anchor_call_id": str(contact_case.get("anchor_call_id") or ""),
                        "anchor_call_timestamp": str(contact_case.get("anchor_call_timestamp") or ""),
                        "anchor_call_duration_seconds": int(contact_case.get("anchor_call_duration_seconds", 0) or 0),
                        "llm_source": str(contact_case.get("llm_source") or "contact_only_fallback"),
                        "llm_ready": True,
                        "score": int(contact_case.get("score", 0) or 0),
                        "score_source": str(contact_case.get("score_source") or ""),
                        "score_reason": str(contact_case.get("score_reason") or ""),
                        "score_components": contact_case.get("score_components", {}) if isinstance(contact_case.get("score_components"), dict) else {},
                        "transcript_length_chars": int(contact_case.get("transcript_length_chars", 0) or 0),
                        "transcript_segments_count": int(contact_case.get("transcript_segments_count", 0) or 0),
                        "transcript_longest_segment_sec": float(contact_case.get("transcript_longest_segment_sec", 0.0) or 0.0),
                        "transcript_usability_label": str(contact_case.get("transcript_usability_label") or ""),
                        "transcript_usability_reason": str(contact_case.get("transcript_usability_reason") or ""),
                        "initial_noise_detected": bool(contact_case.get("initial_noise_detected")),
                        "human_dialogue_after_noise_detected": bool(contact_case.get("human_dialogue_after_noise_detected")),
                        "noise_classification": str(contact_case.get("noise_classification") or "contact_only_fallback"),
                        "final_noise_decision": str(contact_case.get("final_noise_decision") or ""),
                        "llm_allowed": True,
                        "case_type_source": "contact_only_fallback",
                        "case_type_reason": "deal_not_found_in_period_records_but_contact_call_present",
                        "evidence_used": ["contacts_notes_fallback", "contact_without_linked_deal"],
                        "semantic_debug": row_semantic_debug,
                        "contact_only": True,
                        "contact_url": str(contact_case.get("deal_url") or ""),
                    }
                )
                transcript_readiness_entries.append(
                    {
                        "deal_id": deal_id,
                        "selected_call_id": str(contact_case.get("anchor_call_id") or _selected_call_id_for_readiness(candidate=candidate, record={})),
                        "audio_path_exists": bool(str(contact_case.get("anchor_audio_path") or "").strip()),
                        "audio_source": str(contact_case.get("audio_download_status") or "contact_only_recording_url"),
                        "transcript_source": str(contact_case.get("transcript_source") or "contact_only_fallback"),
                        "transcript_chars": int(contact_case.get("transcript_length_chars", 0) or 0),
                        "transcript_excerpt_chars": int(contact_case.get("transcript_length_chars", 0) or 0),
                        "transcript_usable": bool(int(contact_case.get("transcript_length_chars", 0) or 0) >= 20),
                        "stt_status": str(contact_case.get("transcript_status") or ""),
                        "stt_error": ""
                        if int(contact_case.get("transcript_length_chars", 0) or 0) >= 20
                        else str(
                            contact_case.get("transcript_error")
                            or contact_case.get("transcript_usability_reason")
                            or "stt_backend_error"
                        ),
                        "skip_reason_before_llm": "",
                        "final_decision": "sent_to_llm_contact_only",
                        "initial_noise_detected": bool(contact_case.get("initial_noise_detected")),
                        "human_dialogue_after_noise_detected": bool(contact_case.get("human_dialogue_after_noise_detected")),
                        "noise_classification": str(contact_case.get("noise_classification") or "contact_only_fallback"),
                        "final_noise_decision": str(contact_case.get("final_noise_decision") or "sent_to_llm_contact_only"),
                    }
                )
                continue

            _bump(skipped_reasons, "deal_not_found_in_period_records")
            transcript_readiness_entries.append(
                {
                    "deal_id": deal_id,
                    "selected_call_id": _selected_call_id_for_readiness(candidate=candidate, record={}),
                    "audio_path_exists": False,
                    "audio_source": "",
                    "transcript_source": "",
                    "transcript_chars": 0,
                    "transcript_excerpt_chars": 0,
                    "transcript_usable": False,
                    "stt_status": "",
                    "stt_error": "",
                    "skip_reason_before_llm": "deal_not_found_in_period_records",
                    "final_decision": "skipped_deal_not_found_in_period_records",
                }
            )
            continue
        readiness_entry = _build_transcript_readiness_entry(candidate=candidate, record=record)
        case = _build_case(
            candidate=candidate,
            record=record,
            base_domain=base_domain,
            allowlist=allowlist,
            manager_role_registry=manager_role_registry or {},
            ledger_out=ledger,
        )
        case_debug = {
            "order": order,
            "deal_id": deal_id,
            "pool_type": str(candidate.get("pool_type") or ""),
            "call_case_type": str(candidate.get("call_case_type") or ""),
            "shortlist_reason": str(candidate.get("shortlist_reason") or ""),
            "selected_for_transcription": bool(candidate.get("selected_for_transcription")),
            "selected_call_count": int(candidate.get("selected_call_count", 0) or 0),
            "analysis_shortlist_rank_group": int(candidate.get("rank_group", 0) or 0),
            "anchor_manager_name": str(candidate.get("anchor_manager_name") or candidate.get("manager_name_from_call_author") or ""),
            "initial_noise_detected": bool(readiness_entry.get("initial_noise_detected")),
            "human_dialogue_after_noise_detected": bool(readiness_entry.get("human_dialogue_after_noise_detected")),
            "noise_classification": str(readiness_entry.get("noise_classification") or ""),
            "final_noise_decision": str(readiness_entry.get("final_noise_decision") or ""),
        }
        if not isinstance(case, dict):
            reason = str(case) if isinstance(case, str) else "case_build_failed"
            _bump(skipped_reasons, reason)
            case_debug["skipped"] = True
            case_debug["skip_reason"] = reason
            case_debug["final_noise_decision"] = (
                reason
                if reason in {
                    "autoanswer_no_meaningful_conversation",
                    "short_no_meaningful_conversation",
                    "no_human_dialogue",
                    "transcript_not_usable_for_battle_write",
                    "telemarketer_case_out_of_scope",
                }
                else str(case_debug.get("final_noise_decision") or "")
            )
            readiness_entry["final_decision"] = _map_readiness_final_decision(skip_reason=reason)
            if not str(readiness_entry.get("skip_reason_before_llm") or "").strip():
                readiness_entry["skip_reason_before_llm"] = reason
            if reason in {"autoanswer_no_meaningful_conversation", "short_no_meaningful_conversation", "no_human_dialogue"}:
                readiness_entry["final_noise_decision"] = reason
            transcript_readiness_entries.append(readiness_entry)
            if reason == "transcript_not_usable_for_battle_write":
                case_debug["skip_reason_detail"] = _transcript_skip_reason_detail(
                    candidate=candidate,
                    record=record,
                )
                case_debug["llm_allowed"] = False
            elif reason in {"llm_not_ready", "llm_timeout", "validation_failed", "missing_required_stage_comment"}:
                case_debug["skip_reason_detail"] = {
                    "llm_error": str(record.get("call_review_llm_error") or ""),
                    "llm_error_category": str(record.get("call_review_llm_error_category") or ""),
                    "validation_failed_after_repair": bool(record.get("call_review_validation_failed_after_repair")),
                    "repair_applied": bool(record.get("call_review_llm_repair_applied")),
                    "safe_fallback_used": bool(record.get("call_review_llm_safe_fallback_used")),
                    "runtime_metrics": (
                        record.get("call_review_llm_runtime_metrics")
                        if isinstance(record.get("call_review_llm_runtime_metrics"), dict)
                        else {}
                    ),
                }
            cases_debug.append(case_debug)
            continue

        row, row_semantic_debug = _build_row_from_case(case=case)
        row_entries.append(
            {
                "row": row,
                "semantic_debug": row_semantic_debug,
            }
        )
        case_debug["skipped"] = False
        case_debug["manager_name"] = case.get("manager_name", "")
        case_debug["role"] = case.get("manager_role_display", "")
        case_debug["case_mode"] = case.get("case_mode", "")
        case_debug["case_type_display"] = case.get("case_type_display", "")
        case_debug["anchor_call_id"] = case.get("anchor_call_id", "")
        case_debug["anchor_call_timestamp"] = case.get("anchor_call_timestamp", "")
        case_debug["anchor_call_duration_seconds"] = int(case.get("anchor_call_duration_seconds", 0) or 0)
        case_debug["llm_source"] = case.get("llm_source", "")
        case_debug["llm_ready"] = True
        case_debug["score"] = int(case.get("score", 0) or 0)
        case_debug["score_source"] = str(case.get("score_source") or "")
        case_debug["score_reason"] = str(case.get("score_reason") or "")
        case_debug["score_components"] = (
            case.get("score_components")
            if isinstance(case.get("score_components"), dict)
            else {}
        )
        case_debug["transcript_length_chars"] = int(case.get("transcript_length_chars", 0) or 0)
        case_debug["transcript_segments_count"] = int(case.get("transcript_segments_count", 0) or 0)
        case_debug["transcript_longest_segment_sec"] = float(case.get("transcript_longest_segment_sec", 0.0) or 0.0)
        case_debug["transcript_usability_label"] = str(case.get("transcript_usability_label") or "")
        case_debug["transcript_usability_reason"] = str(case.get("transcript_usability_reason") or "")
        case_debug["initial_noise_detected"] = bool(case.get("initial_noise_detected"))
        case_debug["human_dialogue_after_noise_detected"] = bool(case.get("human_dialogue_after_noise_detected"))
        case_debug["noise_classification"] = str(case.get("noise_classification") or "")
        case_debug["final_noise_decision"] = str(case.get("final_noise_decision") or "")
        case_debug["llm_allowed"] = bool(case.get("llm_allowed"))
        case_debug["case_type_source"] = str(case.get("case_type_source") or "")
        case_debug["case_type_reason"] = str(case.get("case_type_reason") or "")
        case_debug["evidence_used"] = (
            case.get("case_type_evidence_used")
            if isinstance(case.get("case_type_evidence_used"), list)
            else []
        )
        case_debug["llm_runtime_metrics"] = (
            case.get("call_review_llm_provenance", {}).get("runtime_metrics", {})
            if isinstance(case.get("call_review_llm_provenance"), dict)
            else {}
        )
        case_debug["semantic_debug"] = row_semantic_debug
        cases_debug.append(case_debug)
        readiness_entry["final_decision"] = "sent_to_llm"
        readiness_entry["final_noise_decision"] = "sent_to_llm"
        transcript_readiness_entries.append(readiness_entry)

    row_entries.sort(
        key=lambda x: (
            str((x.get("row") or {}).get("Дата анализа") or ""),
            str((x.get("row") or {}).get("Менеджер") or ""),
            str((x.get("row") or {}).get("Дата кейса") or ""),
            str((x.get("row") or {}).get("Deal ID") or ""),
        )
    )
    style_payload_normalization = _apply_payload_style_normalization(row_entries=row_entries)
    rows = [x.get("row", {}) for x in row_entries if isinstance(x.get("row"), dict)]
    final_payload_quote_repair = _repair_final_payload_broken_quotes(rows=rows)
    autoanswer_stats = _collect_autoanswer_skip_stats(cases_debug=cases_debug)
    if isinstance(style_payload_normalization, dict):
        style_payload_normalization["final_payload_quote_repair"] = final_payload_quote_repair
    semantic_preflight = _run_semantic_preflight(
        rows=rows,
        row_entries=row_entries,
        style_payload_normalization=style_payload_normalization,
        final_payload_quote_repair=final_payload_quote_repair,
        autoanswer_stats=autoanswer_stats,
    )

    artifacts = _write_debug_artifacts(
        run_dir=run_dir,
        summary=summary,
        ledger=ledger,
        call_ledger_all=call_ledger_all,
        call_ledger_audit=call_ledger_audit,
        anchor_shortlist=anchor_shortlist,
        selected_anchor_cases=(
            [x for x in selected_anchor_cases if isinstance(x, dict)]
            if isinstance(selected_anchor_cases, list)
            else [x for x in shortlist_items if isinstance(x, dict)]
        ),
        cases_debug=cases_debug,
        transcript_readiness_debug=transcript_readiness_entries,
        rows=rows,
        skipped_reasons=skipped_reasons,
        abort_stage=abort_stage,
        abort_error=abort_error,
        artifacts_written=artifacts_written or [],
    )

    return {
        "mode": "call_review_sheet",
        "schema_version": "v3",
        "sheet_name": "Разбор звонков",
        "start_cell": "A2",
        "columns": list(CALL_REVIEW_V3_COLUMNS),
        "rows": rows,
        "rows_count": len(rows),
        "semantic_preflight": semantic_preflight,
        "selection_debug": {
            "rows_total": len(rows),
            "rows_skipped": int(sum(skipped_reasons.values())),
            "skip_reasons": skipped_reasons,
            "details": cases_debug,
        },
        "v3_debug_artifacts": artifacts,
    }


def _build_case(
    *,
    candidate: dict[str, Any],
    record: dict[str, Any],
    base_domain: str,
    allowlist: list[str],
    manager_role_registry: dict[str, str],
    ledger_out: list[dict[str, Any]],
) -> dict[str, Any] | str:
    pool_type = str(candidate.get("pool_type") or "").strip().lower()
    if pool_type != "conversation_pool":
        return "pool_not_conversation"

    if not bool(candidate.get("selected_for_transcription")):
        return "not_selected_for_transcription"

    non_meaningful_reason = _resolve_non_meaningful_skip_reason(candidate=candidate, record=record)
    if non_meaningful_reason:
        return non_meaningful_reason

    case_mode_hint = _resolve_case_mode(candidate=candidate, record=record)
    if case_mode_hint in DISALLOWED_CASES_FOR_ACTIVE_WRITE:
        return f"disallowed_case_mode:{case_mode_hint}"

    llm_ready = bool(record.get("call_review_llm_ready"))
    llm_fields = record.get("call_review_llm_fields") if isinstance(record.get("call_review_llm_fields"), dict) else {}
    if not llm_ready or not llm_fields:
        return _resolve_llm_case_unready_reason(record=record, candidate=candidate)
    case_mode, case_type_source, case_type_reason, case_type_evidence_used = _resolve_case_mode_for_output(
        case_mode_hint=case_mode_hint,
        llm_fields=llm_fields,
        candidate=candidate,
        record=record,
    )

    llm_validation_error = _validate_llm_fields(case_mode=case_mode, llm_fields=llm_fields)
    if llm_validation_error:
        if "stage_" in llm_validation_error:
            return "missing_required_stage_comment"
        return "validation_failed"

    snapshot = _load_snapshot(record=record)
    calls = _extract_calls(snapshot=snapshot)
    transcript_text_by_call_id = _build_transcript_text_by_call_id(snapshot=snapshot)
    selected_ids = _selected_call_ids(candidate=candidate, record=record)
    anchor_call = _select_anchor_call(
        calls=calls,
        selected_call_ids=selected_ids,
        transcript_text_by_call_id=transcript_text_by_call_id,
    )
    if anchor_call is None:
        return "missing_anchor_call"

    if _call_is_noise(
        anchor_call,
        transcript_text=_transcript_text_for_call(anchor_call, transcript_text_by_call_id),
    ):
        return "autoanswer_no_meaningful_conversation"

    anchor_duration = int(anchor_call.get("duration_seconds", 0) or 0)
    if anchor_duration < 25:
        return "short_no_meaningful_conversation"

    related_calls = _select_related_calls(
        calls=calls,
        anchor_call=anchor_call,
        selected_call_ids=selected_ids,
        transcript_text_by_call_id=transcript_text_by_call_id,
    )
    if not related_calls:
        related_calls = [anchor_call]

    anchor_ts = _parse_ts(anchor_call.get("timestamp"))
    analysis_date = _choose_analysis_date(candidate=candidate, anchor_ts=anchor_ts)
    case_date = _choose_case_date(anchor_ts=anchor_ts)

    manager_name = _sanitize_person_name(str(anchor_call.get("manager_name") or ""))
    if not manager_name:
        manager_name = _sanitize_person_name(str(record.get("owner_name") or ""))
    if allowlist and not _is_manager_allowed(manager_name=manager_name, allowlist=allowlist):
        return "manager_not_in_allowlist"

    manager_role_internal = _resolve_manager_role(manager_name=manager_name, registry=manager_role_registry)
    manager_role_display = ROLE_DISPLAY_MAP.get(manager_role_internal, "менеджер по продажам")

    transcript_gate = _evaluate_transcript_usability_for_case(
        candidate=candidate,
        record=record,
    )
    noise_classification = str(transcript_gate.get("noise_classification") or "").strip().lower()
    if (
        manager_role_internal == "telemarketer"
        and case_mode in {"presentation_analysis", "test_analysis", "dozhim_analysis"}
        and noise_classification != "ivr_then_human_dialogue"
    ):
        return "telemarketer_case_out_of_scope"
    if not bool(transcript_gate.get("usable")):
        detected_noise_reason = str(transcript_gate.get("detected_noise_reason") or "").strip().lower()
        transcript_len = int(transcript_gate.get("transcript_length_chars", 0) or 0)
        segments = int(transcript_gate.get("transcript_segments_count", 0) or 0)
        if "autoanswer" in detected_noise_reason or "system_voice" in detected_noise_reason:
            return "autoanswer_no_meaningful_conversation"
        if transcript_len < 120 and segments <= 1:
            return "short_no_meaningful_conversation"
        return "transcript_not_usable_for_battle_write"

    listened_calls = _format_listened_calls(related_calls)
    if not listened_calls:
        return "listened_calls_empty"

    base_mix_resolution = resolve_base_mix([record])
    base_mix_value = str(base_mix_resolution.get("selected_value") or "").strip() or "солянка"
    product_focus = _resolve_product_focus(record=record)

    quote_text = _sanitize_user_text(str(llm_fields.get("evidence_quote") or ""))
    llm_source = str(record.get("call_review_llm_source") or "")
    core_text = {
        key: _sanitize_user_text(llm_fields.get(key))
        for key in (
            "primary_case_type",
            "relevant_stage_groups",
            "one_main_issue",
            "evidence_by_stage",
            "key_takeaway",
            "strong_sides",
            "growth_zones",
            "why_important",
            "reinforce",
            "fix_action",
            "coaching_list",
            "expected_quantity",
            "expected_quality",
            "stage_secretary_comment",
            "stage_lpr_comment",
            "stage_need_comment",
            "stage_presentation_comment",
            "stage_closing_comment",
            "stage_objections_comment",
            "stage_speech_comment",
            "stage_crm_comment",
            "stage_discipline_comment",
            "stage_confirm_demo_comment",
            "stage_demo_intro_comment",
            "stage_demo_context_comment",
            "stage_demo_relevant_comment",
            "stage_demo_process_comment",
            "stage_demo_objections_comment",
            "stage_demo_next_step_comment",
            "stage_demo_comment",
            "stage_test_launch_comment",
            "stage_test_criteria_comment",
            "stage_test_owners_comment",
            "stage_test_support_comment",
            "stage_test_feedback_comment",
            "stage_test_objections_comment",
            "stage_test_comment",
            "stage_dozhim_recontact_comment",
            "stage_dozhim_doubts_comment",
            "stage_dozhim_terms_comment",
            "stage_dozhim_decision_comment",
            "stage_dozhim_flow_comment",
            "stage_dozhim_comment",
        )
    }
    score_payload = _compute_case_score(
        case_mode=case_mode,
        candidate=candidate,
        record=record,
        transcript_gate=transcript_gate,
        llm_fields=core_text,
        anchor_duration_seconds=anchor_duration,
    )

    case = {
        "deal_id": str(record.get("deal_id") or ""),
        "deal_url": _deal_url(base_domain=base_domain, deal_id=str(record.get("deal_id") or "")),
        "deal_name": str(record.get("deal_name") or ""),
        "company_name": str(record.get("company_name") or ""),
        "manager_name": manager_name,
        "manager_role_internal": manager_role_internal,
        "manager_role_display": manager_role_display,
        "analysis_date": analysis_date,
        "case_date": case_date,
        "case_mode": case_mode,
        "case_type_display": CASE_MODE_DISPLAY_MAP.get(case_mode, "разговор"),
        "product_focus": product_focus,
        "base_mix": base_mix_value,
        "listened_calls": listened_calls,
        "llm_fields": core_text,
        "quote_text": quote_text,
        "score": score_payload.get("score"),
        "score_source": str(score_payload.get("score_source") or ""),
        "score_reason": str(score_payload.get("score_reason") or ""),
        "score_components": (
            score_payload.get("score_components")
            if isinstance(score_payload.get("score_components"), dict)
            else {}
        ),
        "criticality": _criticality_from_score(score_payload.get("score")),
        "anchor_call_id": str(anchor_call.get("call_id") or ""),
        "anchor_call_timestamp": str(anchor_call.get("timestamp") or ""),
        "anchor_call_duration_seconds": anchor_duration,
        "selected_call_count": len(related_calls),
        "selected_call_ids": [str(x.get("call_id") or "") for x in related_calls if str(x.get("call_id") or "").strip()],
        "llm_source": llm_source,
        "transcript_length_chars": int(transcript_gate.get("transcript_length_chars", 0) or 0),
        "transcript_segments_count": int(transcript_gate.get("transcript_segments_count", 0) or 0),
        "transcript_longest_segment_sec": float(transcript_gate.get("longest_segment_sec", 0.0) or 0.0),
        "transcript_usability_label": str(transcript_gate.get("label") or ""),
        "transcript_usability_reason": str(transcript_gate.get("why_not_usable") or "usable"),
        "initial_noise_detected": bool(transcript_gate.get("initial_noise_detected")),
        "human_dialogue_after_noise_detected": bool(transcript_gate.get("human_dialogue_after_noise_detected")),
        "noise_classification": str(transcript_gate.get("noise_classification") or ""),
        "final_noise_decision": str(transcript_gate.get("final_noise_decision") or ""),
        "llm_allowed": bool(transcript_gate.get("usable")),
        "case_type_source": case_type_source,
        "case_type_reason": case_type_reason,
        "case_type_evidence_used": case_type_evidence_used,
        "call_review_llm_provenance": (
            record.get("call_review_llm_provenance")
            if isinstance(record.get("call_review_llm_provenance"), dict)
            else {}
        ),
    }

    for call in calls:
        ledger_out.append(
            {
                "deal_id": str(record.get("deal_id") or ""),
                "deal_name": str(record.get("deal_name") or ""),
                "manager_name_from_call_author": str(call.get("manager_name") or ""),
                "manager_role": manager_role_display,
                "call_id": str(call.get("call_id") or ""),
                "call_datetime": str(call.get("timestamp") or ""),
                "call_duration_seconds": int(call.get("duration_seconds", 0) or 0),
                "direction": str(call.get("direction") or ""),
                "recording_url": str(call.get("recording_url") or ""),
                "audio_path": str(call.get("audio_path") or ""),
                "phone_raw": str(call.get("phone") or call.get("phone_number") or call.get("contact_phone") or ""),
                "phone_normalized_last7": _normalize_phone_last7(
                    str(call.get("phone") or call.get("phone_number") or call.get("contact_phone") or "")
                ),
                "is_anchor": str(call.get("call_id") or "") == str(anchor_call.get("call_id") or ""),
                "selected_for_case": str(call.get("call_id") or "") in set(case["selected_call_ids"]),
                "recording_available": bool(str(call.get("recording_url") or "").strip()),
                "pool_type": pool_type,
                "case_mode": case_mode,
            }
        )
    return case


def _evaluate_transcript_usability_for_case(*, candidate: dict[str, Any], record: dict[str, Any]) -> dict[str, Any]:
    transcript_label = str(
        candidate.get("transcript_usability_label")
        or record.get("transcript_usability_label")
        or ""
    ).strip().lower()
    transcript_score = int(
        candidate.get("transcript_usability_score_final", 0)
        or record.get("transcript_usability_score_final", 0)
        or 0
    )
    transcript_text = _text(record.get("transcript_text"))
    transcript_excerpt = _text(record.get("transcript_text_excerpt"))
    call_summary = _text(record.get("call_signal_summary_short"))
    snapshot = _load_snapshot(record=record)
    snapshot_transcript_texts: list[str] = []
    for item in snapshot.get("transcripts", []) if isinstance(snapshot.get("transcripts"), list) else []:
        if not isinstance(item, dict):
            continue
        value = _text(item.get("transcript_text"))
        if value:
            snapshot_transcript_texts.append(value)
    snapshot_transcript_text = _text(" ".join(snapshot_transcript_texts))
    if len(transcript_text) < 80 and snapshot_transcript_text:
        transcript_text = snapshot_transcript_text
    anchor_duration = int(
        candidate.get("anchor_call_duration_seconds", 0)
        or record.get("call_review_anchor_call_duration_seconds", 0)
        or record.get("anchor_call_duration_seconds", 0)
        or 0
    )
    combined_text = " ".join([transcript_text, transcript_excerpt, call_summary, snapshot_transcript_text]).strip().lower()
    effective_len = len(combined_text)
    transcript_length_chars = int(
        record.get("transcript_text_len", 0)
        or effective_len
        or len(transcript_excerpt)
        or len(call_summary)
        or len(snapshot_transcript_text)
    )
    transcript_length_chars = max(transcript_length_chars, effective_len)
    transcript_segments_count = int(record.get("transcript_segments_count", 0) or 0)
    longest_segment_sec = float(record.get("transcript_longest_segment_sec", 0.0) or 0.0)
    noise_profile = _noise_dialogue_profile(
        transcript_text=combined_text,
        transcript_chars=transcript_length_chars,
        call_duration_seconds=anchor_duration,
    )

    detected_noise_reason = ""
    if str(noise_profile.get("noise_classification") or "") == "ivr_then_human_dialogue":
        detected_noise_reason = "ivr_then_human_dialogue"
    elif any(token in combined_text for token in ("абонент", "сигнала", "автоответ", "voicemail", "оставьте сообщение")):
        detected_noise_reason = "autoanswer_or_system_voice_markers"
    elif transcript_label == "noisy":
        detected_noise_reason = "legacy_label_noisy"

    if transcript_length_chars < 40 and transcript_segments_count <= 0 and not transcript_excerpt and not call_summary:
        return {
            "usable": False,
            "label": "empty",
            "why_not_usable": "transcript_text_too_short_or_missing",
            "transcript_length_chars": transcript_length_chars,
            "transcript_segments_count": transcript_segments_count,
            "longest_segment_sec": longest_segment_sec,
            "detected_noise_reason": detected_noise_reason,
            "initial_noise_detected": bool(noise_profile.get("initial_noise_detected")),
            "human_dialogue_after_noise_detected": bool(noise_profile.get("human_dialogue_after_noise_detected")),
            "noise_classification": str(noise_profile.get("noise_classification") or ""),
            "final_noise_decision": str(noise_profile.get("final_noise_decision") or "skip_noise_only"),
        }

    if (
        detected_noise_reason
        and detected_noise_reason != "ivr_then_human_dialogue"
        and transcript_length_chars < 220
        and transcript_segments_count <= 1
    ):
        return {
            "usable": False,
            "label": "noisy",
            "why_not_usable": "noise_markers_dominate_short_transcript",
            "transcript_length_chars": transcript_length_chars,
            "transcript_segments_count": transcript_segments_count,
            "longest_segment_sec": longest_segment_sec,
            "detected_noise_reason": detected_noise_reason,
            "initial_noise_detected": bool(noise_profile.get("initial_noise_detected")),
            "human_dialogue_after_noise_detected": bool(noise_profile.get("human_dialogue_after_noise_detected")),
            "noise_classification": str(noise_profile.get("noise_classification") or ""),
            "final_noise_decision": "skip_noise_only",
        }

    # Long transcript with dialogue content should pass even if legacy label stayed noisy.
    dialogue_markers = (
        "я понял",
        "давайте",
        "встреч",
        "поставщик",
        "закуп",
        "демо",
        "тест",
        "следующ",
        "договор",
    )
    has_dialogue_markers = any(token in combined_text for token in dialogue_markers)
    if bool(noise_profile.get("human_dialogue_after_noise_detected")):
        has_dialogue_markers = True
    usable_by_length = transcript_length_chars >= 220 or transcript_segments_count >= 3
    usable_by_signal = transcript_score >= 2 or len(call_summary) >= 40 or has_dialogue_markers
    if transcript_length_chars >= 500 and transcript_segments_count >= 2:
        usable_by_signal = True
    if str(noise_profile.get("noise_classification") or "") == "ivr_then_human_dialogue" and transcript_length_chars >= 1000:
        usable_by_length = True
        usable_by_signal = True
    if usable_by_length and usable_by_signal:
        return {
            "usable": True,
            "label": "usable",
            "why_not_usable": "",
            "transcript_length_chars": transcript_length_chars,
            "transcript_segments_count": transcript_segments_count,
            "longest_segment_sec": longest_segment_sec,
            "detected_noise_reason": detected_noise_reason,
            "initial_noise_detected": bool(noise_profile.get("initial_noise_detected")),
            "human_dialogue_after_noise_detected": bool(noise_profile.get("human_dialogue_after_noise_detected")),
            "noise_classification": str(noise_profile.get("noise_classification") or ""),
            "final_noise_decision": str(noise_profile.get("final_noise_decision") or "allow"),
        }

    if transcript_label in {"empty"} and transcript_score <= 0 and not has_dialogue_markers and transcript_length_chars < 160:
        return {
            "usable": False,
            "label": transcript_label or "empty",
            "why_not_usable": "legacy_empty_label_without_dialogue_signals",
            "transcript_length_chars": transcript_length_chars,
            "transcript_segments_count": transcript_segments_count,
            "longest_segment_sec": longest_segment_sec,
            "detected_noise_reason": detected_noise_reason,
            "initial_noise_detected": bool(noise_profile.get("initial_noise_detected")),
            "human_dialogue_after_noise_detected": bool(noise_profile.get("human_dialogue_after_noise_detected")),
            "noise_classification": str(noise_profile.get("noise_classification") or ""),
            "final_noise_decision": str(noise_profile.get("final_noise_decision") or "skip_noise_only"),
        }

    final_usable = bool(usable_by_signal)
    if str(noise_profile.get("noise_classification") or "") == "ivr_then_human_dialogue" and transcript_length_chars >= 1000:
        final_usable = True
    return {
        "usable": final_usable,
        "label": "usable" if final_usable else ("weak" if transcript_length_chars >= 80 else (transcript_label or "empty")),
        "why_not_usable": "" if final_usable else "insufficient_dialogue_signals_or_length",
        "transcript_length_chars": transcript_length_chars,
        "transcript_segments_count": transcript_segments_count,
        "longest_segment_sec": longest_segment_sec,
        "detected_noise_reason": detected_noise_reason,
        "initial_noise_detected": bool(noise_profile.get("initial_noise_detected")),
        "human_dialogue_after_noise_detected": bool(noise_profile.get("human_dialogue_after_noise_detected")),
        "noise_classification": str(noise_profile.get("noise_classification") or ""),
        "final_noise_decision": str(noise_profile.get("final_noise_decision") or ("allow" if final_usable else "skip_noise_only")),
    }


def _transcript_skip_reason_detail(*, candidate: dict[str, Any], record: dict[str, Any]) -> dict[str, Any]:
    gate = _evaluate_transcript_usability_for_case(candidate=candidate, record=record)
    return {
        "transcript_length_chars": int(gate.get("transcript_length_chars", 0) or 0),
        "transcript_segments_count": int(gate.get("transcript_segments_count", 0) or 0),
        "longest_segment_sec": float(gate.get("longest_segment_sec", 0.0) or 0.0),
        "usability_label": str(gate.get("label") or ""),
        "usability_reason": str(gate.get("why_not_usable") or ""),
        "llm_allowed": bool(gate.get("usable")),
        "detected_noise_reason": str(gate.get("detected_noise_reason") or ""),
        "why_not_usable": str(gate.get("why_not_usable") or ""),
        "initial_noise_detected": bool(gate.get("initial_noise_detected")),
        "human_dialogue_after_noise_detected": bool(gate.get("human_dialogue_after_noise_detected")),
        "noise_classification": str(gate.get("noise_classification") or ""),
        "final_noise_decision": str(gate.get("final_noise_decision") or ""),
    }


def _build_row_from_case(*, case: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    row = {col: "" for col in CALL_REVIEW_V3_COLUMNS}
    llm = case.get("llm_fields", {}) if isinstance(case.get("llm_fields"), dict) else {}
    case_mode = str(case.get("case_mode") or "").strip().lower()
    role_internal = str(case.get("manager_role_internal") or "").strip().lower()
    raw_stage_comments = _collect_stage_comments_from_llm(llm=llm)
    stage_relevance = _resolve_stage_relevance(
        case_mode=case_mode,
        role_internal=role_internal,
        llm_fields=llm,
        raw_stage_comments=raw_stage_comments,
        case=case,
    )
    global_quote = _sanitize_user_text(str(case.get("quote_text") or ""))
    primary_group = CASE_MODE_PRIMARY_GROUP.get(case_mode, "")

    row["Дата анализа"] = str(case.get("analysis_date") or "")
    row["Дата кейса"] = str(case.get("case_date") or "")
    row["Менеджер"] = str(case.get("manager_name") or "")
    row["Роль"] = str(case.get("manager_role_display") or "")
    row["Deal ID"] = str(case.get("deal_id") or "")
    row["Ссылка на сделку"] = str(case.get("deal_url") or "")
    row["Сделка"] = str(case.get("deal_name") or "")
    row["Компания"] = str(case.get("company_name") or "")
    row["Продукт / фокус"] = str(case.get("product_focus") or "")
    row["База / тег"] = str(case.get("base_mix") or "")
    row["Тип кейса"] = str(case.get("case_type_display") or "")
    row["Прослушанные звонки"] = str(case.get("listened_calls") or "")

    quote_usage: dict[str, int] = {}
    stage_comments_without_evidence: list[str] = []
    comments_cleared_due_to_irrelevant_stage: list[str] = []
    stage_comment_map: dict[str, str] = {}
    repeated_quotes: list[dict[str, Any]] = []

    for stage_group, meta in STAGE_GROUPS.items():
        relevance_info = stage_relevance.get(stage_group, {"relevant": False, "cross_stage_evidence": False})
        raw_comment = str(raw_stage_comments.get(stage_group) or "")
        relevant = bool(relevance_info.get("relevant"))
        comment = ""
        if relevant:
            stage_quote = _pick_stage_quote(
                stage_group=stage_group,
                raw_comment=raw_comment,
                llm_fields=llm,
                global_quote=global_quote if stage_group == primary_group else "",
            )
            comment, quote_used = _merge_comment_with_quote(
                comment=raw_comment,
                quote=stage_quote,
                stage_group=stage_group,
                quote_usage=quote_usage,
            )
            if quote_used:
                quote_key = _quote_key(quote_used)
                if int(quote_usage.get(quote_key, 0) or 0) > 2:
                    repeated_quotes.append(
                        {"stage_group": stage_group, "quote": quote_used, "count": int(quote_usage.get(quote_key, 0) or 0)}
                    )
            if comment and not _comment_has_stage_evidence(comment):
                stage_comments_without_evidence.append(stage_group)
                comment = ""
        else:
            if raw_comment.strip():
                comments_cleared_due_to_irrelevant_stage.append(stage_group)
        stage_comment_map[stage_group] = comment
        row[str(meta["comment_col"])] = comment
        status_value = _status_from_comment(comment=comment, relevant=relevant)
        for col in meta["substage_cols"]:
            row[str(col)] = status_value

    row["Ключевой вывод"] = str(llm.get("key_takeaway") or "")
    row["Сильная сторона"] = str(llm.get("strong_sides") or "")
    row["Зона роста"] = str(llm.get("growth_zones") or "")
    row["Почему это важно"] = str(llm.get("why_important") or "")
    row["Что закрепить"] = str(llm.get("reinforce") or "")
    row["Что исправить"] = str(llm.get("fix_action") or "")
    row["Что донести сотруднику"] = _normalize_coaching(str(llm.get("coaching_list") or ""))
    row["Эффект количество / неделя"] = _normalize_expected_quantity(str(llm.get("expected_quantity") or ""))
    row["Эффект качество"] = str(llm.get("expected_quality") or "")
    row["Оценка 0-100"] = case.get("score") if case.get("score") is not None else ""
    row["Критичность"] = str(case.get("criticality") or "")
    row, style_replacements = _sanitize_user_facing_row(row)
    quote_usage_count_by_row = _collect_quote_usage_for_row(row=row)
    return row, {
        "stage_relevance": stage_relevance,
        "stage_comment_map": stage_comment_map,
        "stage_comments_without_stage_evidence": stage_comments_without_evidence,
        "comments_cleared_due_to_irrelevant_stage": comments_cleared_due_to_irrelevant_stage,
        "case_type_source": str(case.get("case_type_source") or ""),
        "case_type_reason": str(case.get("case_type_reason") or ""),
        "quote_usage_count_by_row": quote_usage_count_by_row,
        "repeated_quotes": [q for q, c in quote_usage_count_by_row.items() if c > 1],
        "max_same_quote_usage": max(quote_usage_count_by_row.values(), default=0),
        "style_replacements_applied": style_replacements,
    }


def _collect_stage_comments_from_llm(
    *,
    llm: dict[str, Any],
 ) -> dict[str, str]:
    out: dict[str, str] = {}
    for stage_group, meta in STAGE_GROUPS.items():
        parts = [str(llm.get(k) or "") for k in meta["llm_comment_keys"]]
        out[stage_group] = _join_non_empty(*parts)
    return out


def _resolve_stage_relevance(
    *,
    case_mode: str,
    role_internal: str,
    llm_fields: dict[str, Any],
    raw_stage_comments: dict[str, str],
    case: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    default_relevant = set(CASE_MODE_STAGE_MATRIX_DEFAULT.get(case_mode, set()))
    if role_internal == "telemarketer" and case_mode not in {"presentation_analysis", "test_analysis", "dozhim_analysis"}:
        default_relevant -= {"confirm_demo", "demo", "test", "dozhim"}
    explicit_relevant = _parse_stage_group_set(str(llm_fields.get("relevant_stage_groups") or ""))
    evidence_by_stage = _parse_stage_evidence_map(str(llm_fields.get("evidence_by_stage") or ""))
    next_step_present = bool(case.get("call_signal_next_step_present"))
    objection_present = bool(case.get("call_signal_objection_any"))
    demo_discussed = bool(case.get("call_signal_demo_discussed"))
    test_discussed = bool(case.get("call_signal_test_discussed"))
    dozhim_discussed = bool(case.get("call_signal_dozhim_discussed"))
    discipline_flag = bool(case.get("discipline_signal_present"))

    # Conditional defaults from call signals.
    if case_mode in {"warm_inbound_analysis", "supplier_inbound_analysis", "negotiation_lpr_analysis"} and next_step_present:
        default_relevant.add("closing")
    if case_mode in {"warm_inbound_analysis", "supplier_inbound_analysis", "negotiation_lpr_analysis"} and demo_discussed:
        default_relevant.add("presentation_meeting")
    if case_mode in {"warm_inbound_analysis", "supplier_inbound_analysis", "negotiation_lpr_analysis", "presentation_analysis", "test_analysis", "dozhim_analysis"} and objection_present:
        default_relevant.add("objections")
    if case_mode == "presentation_analysis" and _text(raw_stage_comments.get("confirm_demo")):
        default_relevant.add("confirm_demo")
    if case_mode == "presentation_analysis" and _text(raw_stage_comments.get("need")):
        default_relevant.add("need")
    if case_mode == "test_analysis" and dozhim_discussed:
        default_relevant.add("dozhim")
    if case_mode == "secretary_analysis" and discipline_flag:
        default_relevant.add("discipline")
    if case_mode == "dozhim_analysis" and test_discussed:
        default_relevant.add("test")

    out: dict[str, dict[str, Any]] = {}
    cross_allowed = CASE_MODE_CROSS_STAGE_ALLOWED.get(case_mode, set())
    for stage_group in STAGE_GROUPS:
        default_flag = stage_group in default_relevant
        cross_evidence = False
        if not default_flag and stage_group in cross_allowed:
            if stage_group in explicit_relevant:
                cross_evidence = True
            elif _text(evidence_by_stage.get(stage_group)):
                cross_evidence = True
        out[stage_group] = {
            "relevant": bool(default_flag or cross_evidence),
            "cross_stage_evidence": bool(cross_evidence),
            "reason": "default_matrix" if default_flag else ("cross_stage_evidence" if cross_evidence else "not_relevant"),
        }
    return out


def _parse_stage_group_set(raw: str) -> set[str]:
    text = str(raw or "").strip()
    if not text:
        return set()
    out: set[str] = set()
    if text.startswith("[") and text.endswith("]"):
        try:
            payload = json.loads(text)
            if isinstance(payload, list):
                for item in payload:
                    token = _norm_stage_group_name(str(item or ""))
                    if token:
                        out.add(token)
                return out
        except Exception:
            pass
    for part in re.split(r"[;,|/]+", text):
        token = _norm_stage_group_name(part)
        if token:
            out.add(token)
    return out


def _parse_stage_evidence_map(raw: str) -> dict[str, str]:
    text = str(raw or "").strip()
    if not text:
        return {}
    if text.startswith("{") and text.endswith("}"):
        try:
            payload = json.loads(text)
            if isinstance(payload, dict):
                out: dict[str, str] = {}
                for key, value in payload.items():
                    token = _norm_stage_group_name(str(key or ""))
                    if token:
                        out[token] = _text(value)
                return out
        except Exception:
            pass
    out: dict[str, str] = {}
    for key, value in re.findall(r"([a-zA-Z_]+)\s*:\s*([^;]+)", text):
        token = _norm_stage_group_name(key)
        if token:
            out[token] = _text(value)
    return out


def _norm_stage_group_name(value: str) -> str:
    raw = _text(value).lower()
    aliases = {
        "presentation": "demo",
        "demo_confirmation": "confirm_demo",
        "presentation_meeting": "presentation_meeting",
        "discipline_dials": "discipline",
        "dials_discipline": "discipline",
    }
    token = raw.replace("-", "_").replace(" ", "_")
    return aliases.get(token, token) if aliases.get(token, token) in STAGE_GROUPS else ""


def _status_from_comment(*, comment: str, relevant: bool) -> str:
    if not relevant:
        return "н/п"
    text = " ".join(str(comment or "").split()).strip().lower()
    if not text:
        return "нет"
    positive = (
        "получил",
        "зафикс",
        "подтверд",
        "договор",
        "согласовал",
        "удержал",
        "понятно",
        "четко",
        "чётко",
    )
    negative = (
        "не ",
        "без ",
        "пропуст",
        "рано",
        "провис",
        "слаб",
        "не дожал",
        "не зафикс",
        "не подтверд",
    )
    has_positive = any(token in text for token in positive)
    has_negative = any(token in text for token in negative)
    if has_positive and not has_negative:
        return "да"
    if has_negative and not has_positive:
        return "нет"
    return "частично"


def _compute_case_score(
    *,
    case_mode: str,
    candidate: dict[str, Any],
    record: dict[str, Any],
    transcript_gate: dict[str, Any],
    llm_fields: dict[str, str],
    anchor_duration_seconds: int,
) -> dict[str, Any]:
    fallback_raw = record.get("score")
    try:
        fallback_score = int(float(fallback_raw))
    except Exception:
        fallback_score = 0

    mode_weight = {
        "negotiation_lpr_analysis": 22,
        "secretary_analysis": 14,
        "supplier_inbound_analysis": 18,
        "warm_inbound_analysis": 18,
        "presentation_analysis": 20,
        "test_analysis": 19,
        "dozhim_analysis": 18,
    }.get(case_mode, 12)
    duration_component = min(18, max(0, int(anchor_duration_seconds // 35)))
    transcript_len = int(transcript_gate.get("transcript_length_chars", 0) or 0)
    transcript_segments = int(transcript_gate.get("transcript_segments_count", 0) or 0)
    transcript_component = min(18, int(transcript_len // 45) + min(4, transcript_segments))
    next_step_component = 8 if bool(record.get("call_signal_next_step_present")) else 0
    objection_component = 6 if bool(record.get("call_signal_objection_price") or record.get("call_signal_objection_no_need")) else 0
    need_component = 7 if str(llm_fields.get("stage_need_comment") or "").strip() else 0
    crm_component = 6 if str(llm_fields.get("stage_crm_comment") or "").strip() else 0
    discipline_penalty = -8 if bool(candidate.get("same_time_redial_pattern_flag") or candidate.get("repeated_dead_redial_count")) else 0
    usability_penalty = 0 if bool(transcript_gate.get("usable")) else -12

    raw_score = (
        mode_weight
        + duration_component
        + transcript_component
        + next_step_component
        + objection_component
        + need_component
        + crm_component
        + discipline_penalty
        + usability_penalty
    )
    score = max(15, min(98, int(raw_score)))
    if score == 15 and fallback_score > 0:
        # keep deterministic lower guard but avoid one frozen fallback for all rows
        score = max(15, min(98, fallback_score))
    return {
        "score": score,
        "score_source": "call_review_v3_weighted",
        "score_reason": f"{case_mode}; duration={anchor_duration_seconds}s; transcript={transcript_len} chars",
        "score_components": {
            "mode_weight": mode_weight,
            "duration_component": duration_component,
            "transcript_component": transcript_component,
            "next_step_component": next_step_component,
            "objection_component": objection_component,
            "need_component": need_component,
            "crm_component": crm_component,
            "discipline_penalty": discipline_penalty,
            "usability_penalty": usability_penalty,
            "fallback_score_raw": fallback_score,
        },
    }


def _run_semantic_preflight(
    *,
    rows: list[dict[str, Any]],
    row_entries: list[dict[str, Any]],
    style_payload_normalization: dict[str, Any] | None = None,
    final_payload_quote_repair: dict[str, Any] | None = None,
    autoanswer_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    irrelevant_stage_comment_count = 0
    irrelevant_stage_non_np_count = 0
    repeated_quote_count = 0
    max_same_quote_usage_per_row = 0
    generic_better_phrase_count = 0
    repeated_better_phrase_count = 0
    repeated_better_phrase_marker_count = 0
    repeated_better_phrase_actual_duplicate_count = 0
    repeated_better_phrase_blocking = False
    comments_with_not_relevant_text_count = 0
    stage_comments_without_distinct_evidence_count = 0
    truncated_text_count = 0
    unfinished_sentence_count = 0
    comments_cleared_due_to_irrelevant_stage = 0
    forbidden_english_terms_count = 0
    training_jargon_count = 0
    crm_uppercase_count = 0
    lpu_typo_count = 0
    technical_terms_count = 0
    final_payload_forbidden_terms_count = 0
    final_payload_broken_quotes_count = int(
        (final_payload_quote_repair or {}).get("final_payload_broken_quotes_count", 0) or 0
    )
    final_payload_broken_quotes_repaired_count = int(
        (final_payload_quote_repair or {}).get("final_payload_broken_quotes_repaired_count", 0) or 0
    )
    final_payload_broken_quotes_unrepaired_count = int(
        (final_payload_quote_repair or {}).get("final_payload_broken_quotes_unrepaired_count", 0) or 0
    )
    final_payload_training_jargon_count = 0
    final_payload_truncated_text_count = 0
    final_payload_checked_fields_count = 0
    final_payload_crm_stage_mismatch_mentions_count = int(
        (style_payload_normalization or {}).get("final_payload_crm_stage_mismatch_mentions_count", 0) or 0
    )
    final_payload_crm_stage_mismatch_repaired_count = int(
        (style_payload_normalization or {}).get("final_payload_crm_stage_mismatch_repaired_count", 0) or 0
    )
    final_payload_crm_stage_mismatch_unrepaired_count = int(
        (style_payload_normalization or {}).get("final_payload_crm_stage_mismatch_unrepaired_count", 0) or 0
    )
    final_payload_bad_phrase_count = int(
        (style_payload_normalization or {}).get("final_payload_bad_phrase_count", 0) or 0
    )
    final_payload_bad_phrase_repaired_count = int(
        (style_payload_normalization or {}).get("final_payload_bad_phrase_repaired_count", 0) or 0
    )
    final_payload_bad_phrase_unrepaired_count = int(
        (style_payload_normalization or {}).get("final_payload_bad_phrase_unrepaired_count", 0) or 0
    )
    final_payload_empty_speech_module_count = int(
        (style_payload_normalization or {}).get("final_payload_empty_speech_module_count", 0) or 0
    )
    final_payload_empty_speech_module_repaired_count = int(
        (style_payload_normalization or {}).get("final_payload_empty_speech_module_repaired_count", 0) or 0
    )
    final_payload_empty_speech_module_unrepaired_count = int(
        (style_payload_normalization or {}).get("final_payload_empty_speech_module_unrepaired_count", 0) or 0
    )
    final_payload_crm_stage_mismatch_examples = [
        x
        for x in (style_payload_normalization or {}).get("final_payload_crm_stage_mismatch_examples", [])
        if isinstance(x, dict)
    ][:5]
    final_payload_bad_phrase_examples = [
        x
        for x in (style_payload_normalization or {}).get("final_payload_bad_phrase_examples", [])
        if isinstance(x, dict)
    ][:5]
    final_payload_empty_speech_module_examples = [
        x
        for x in (style_payload_normalization or {}).get("final_payload_empty_speech_module_examples", [])
        if isinstance(x, dict)
    ][:5]
    final_payload_truncated_examples: list[dict[str, Any]] = []
    broken_quote_examples = [
        x
        for x in (final_payload_quote_repair or {}).get("broken_quote_examples", [])
        if isinstance(x, dict)
    ][:5]
    failed_rules: list[dict[str, Any]] = []
    warning_rules: list[dict[str, Any]] = []
    rows_debug: list[dict[str, Any]] = []
    repeated_quotes_debug: list[dict[str, Any]] = []
    stage_comments_without_stage_evidence_debug: list[dict[str, Any]] = []
    comments_cleared_due_to_irrelevant_stage_debug: list[dict[str, Any]] = []
    repeated_better_phrases_debug: list[dict[str, Any]] = []
    repeated_better_phrase_actual_duplicates: list[dict[str, Any]] = []
    style_lint_by_row: list[dict[str, Any]] = []
    better_phrase_usage: dict[str, int] = {}
    case_type_source_counts: dict[str, int] = {}
    autoanswer_calls_detected = int((autoanswer_stats or {}).get("autoanswer_calls_detected", 0) or 0)
    autoanswer_deals_skipped = int((autoanswer_stats or {}).get("autoanswer_deals_skipped", 0) or 0)
    autoanswer_by_manager = (
        (autoanswer_stats or {}).get("autoanswer_by_manager")
        if isinstance((autoanswer_stats or {}).get("autoanswer_by_manager"), dict)
        else {}
    )

    generic_phrase = GENERIC_BETTER_PHRASE

    for idx, entry in enumerate(row_entries, start=1):
        row = entry.get("row", {}) if isinstance(entry.get("row"), dict) else {}
        semantic = entry.get("semantic_debug", {}) if isinstance(entry.get("semantic_debug"), dict) else {}
        deal_id = str(row.get("Deal ID") or "")
        case_type = str(row.get("Тип кейса") or "").strip().lower()
        case_type_source = str(
            semantic.get("case_type_source")
            or semantic.get("case_type_source_effective")
            or "unknown"
        ).strip() or "unknown"
        case_type_source_counts[case_type_source] = int(case_type_source_counts.get(case_type_source, 0) or 0) + 1
        stage_relevance = semantic.get("stage_relevance", {}) if isinstance(semantic.get("stage_relevance"), dict) else {}
        row_quote_usage = (
            semantic.get("quote_usage_count_by_row")
            if isinstance(semantic.get("quote_usage_count_by_row"), dict)
            else _collect_quote_usage_for_row(row=row)
        )
        max_quote_this_row = max((int(x or 0) for x in row_quote_usage.values()), default=0)
        max_same_quote_usage_per_row = max(max_same_quote_usage_per_row, max_quote_this_row)
        repeated_quote_count += sum(max(0, int(c or 0) - 1) for c in row_quote_usage.values())
        if max_quote_this_row > 2:
            failed_rules.append(
                {
                    "rule": "quote_usage_gt_2",
                    "deal_id": deal_id,
                    "max_same_quote_usage": max_quote_this_row,
                }
            )

        row_generic_count = 0
        row_has_not_relevant_text = 0
        row_stage_without_evidence = 0
        row_truncated = 0
        row_unfinished = 0
        row_irrelevant_comment = 0
        row_irrelevant_non_np = 0
        row_forbidden_english_terms_count = 0
        row_training_jargon_count = 0
        row_crm_uppercase_count = 0
        row_lpu_typo_count = 0
        row_technical_terms_count = 0
        row_stage_without_evidence_items: list[str] = []

        for stage_group, meta in STAGE_GROUPS.items():
            comment_col = str(meta["comment_col"])
            comment_text = str(row.get(comment_col) or "")
            relevance_info = stage_relevance.get(stage_group, {}) if isinstance(stage_relevance.get(stage_group), dict) else {}
            relevant = bool(relevance_info.get("relevant"))
            cross_stage = bool(relevance_info.get("cross_stage_evidence"))
            substage_values = [str(row.get(col) or "").strip().lower() for col in meta["substage_cols"]]
            non_np_present = any(v not in {"", "н/п"} for v in substage_values)

            if not relevant:
                if comment_text.strip():
                    irrelevant_stage_comment_count += 1
                    row_irrelevant_comment += 1
                if non_np_present:
                    irrelevant_stage_non_np_count += 1
                    row_irrelevant_non_np += 1

            if case_type == "работа с тестом" and stage_group in {"secretary", "lpr"}:
                if (comment_text.strip() or non_np_present) and not cross_stage:
                    failed_rules.append(
                        {
                            "rule": "test_case_irrelevant_stage_filled_without_cross_evidence",
                            "deal_id": deal_id,
                            "stage_group": stage_group,
                        }
                    )

            if comment_text.strip():
                low = comment_text.lower()
                if "не актуально" in low:
                    comments_with_not_relevant_text_count += 1
                    row_has_not_relevant_text += 1
                if generic_phrase in low:
                    row_generic_count += low.count(generic_phrase)
                if not _comment_has_stage_evidence(comment_text):
                    stage_comments_without_distinct_evidence_count += 1
                    row_stage_without_evidence += 1
                    row_stage_without_evidence_items.append(stage_group)
                if re.search(r"(?:\bи|\bк|\bчтобы|например:)\s*$", low):
                    truncated_text_count += 1
                    row_truncated += 1
                if comment_text.count('"') % 2 == 1:
                    unfinished_sentence_count += 1
                    row_unfinished += 1
                row_forbidden_english_terms_count += _count_regex_hits(comment_text, FORBIDDEN_ENGLISH_TERM_PATTERNS)
                row_training_jargon_count += _count_regex_hits(comment_text, TRAINING_JARGON_PATTERNS)
                row_crm_uppercase_count += _count_regex_hits(comment_text, (re.compile(r"\bCRM\b"),))
                row_lpu_typo_count += _count_regex_hits(comment_text, (re.compile(r"\bлпу\b", re.IGNORECASE),))
                row_technical_terms_count += _count_regex_hits(comment_text, TECHNICAL_TERMS_PATTERNS)
                for better_phrase in _extract_better_phrases(comment_text):
                    key = better_phrase.lower()
                    better_phrase_usage[key] = int(better_phrase_usage.get(key, 0) or 0) + 1

        cleared_stage_groups = (
            semantic.get("comments_cleared_due_to_irrelevant_stage", [])
            if isinstance(semantic.get("comments_cleared_due_to_irrelevant_stage"), list)
            else []
        )
        comments_cleared_due_to_irrelevant_stage += len(cleared_stage_groups)
        for stage_group in cleared_stage_groups:
            comments_cleared_due_to_irrelevant_stage_debug.append(
                {"deal_id": deal_id, "stage_group": str(stage_group or "")}
            )

        narrative_cols = [
            "Ключевой вывод",
            "Сильная сторона",
            "Зона роста",
            "Почему это важно",
            "Что закрепить",
            "Что исправить",
            "Что донести сотруднику",
            "Эффект количество / неделя",
            "Эффект качество",
        ]
        for col in narrative_cols:
            value = str(row.get(col) or "")
            if not value:
                continue
            low = value.lower()
            if "не актуально" in low:
                comments_with_not_relevant_text_count += 1
                row_has_not_relevant_text += 1
            if generic_phrase in low:
                row_generic_count += low.count(generic_phrase)
            if re.search(r"(?:\bи|\bк|\bчтобы|например:)\s*$", low):
                truncated_text_count += 1
                row_truncated += 1
            if value.count('"') % 2 == 1:
                unfinished_sentence_count += 1
                row_unfinished += 1
            row_forbidden_english_terms_count += _count_regex_hits(value, FORBIDDEN_ENGLISH_TERM_PATTERNS)
            row_training_jargon_count += _count_regex_hits(value, TRAINING_JARGON_PATTERNS)
            row_crm_uppercase_count += _count_regex_hits(value, (re.compile(r"\bCRM\b"),))
            row_lpu_typo_count += _count_regex_hits(value, (re.compile(r"\bлпу\b", re.IGNORECASE),))
            row_technical_terms_count += _count_regex_hits(value, TECHNICAL_TERMS_PATTERNS)
            for better_phrase in _extract_better_phrases(value):
                key = better_phrase.lower()
                better_phrase_usage[key] = int(better_phrase_usage.get(key, 0) or 0) + 1

        generic_better_phrase_count += row_generic_count
        if row_generic_count > 1:
            failed_rules.append(
                {"rule": "generic_better_phrase_repeated_in_row", "deal_id": deal_id, "count": row_generic_count}
            )

        repeated_for_row = [q for q, c in row_quote_usage.items() if int(c or 0) > 1]
        for quote in repeated_for_row:
            repeated_quotes_debug.append(
                {
                    "deal_id": deal_id,
                    "quote": quote,
                    "count": int(row_quote_usage.get(quote, 0) or 0),
                }
            )
        for stage_group in row_stage_without_evidence_items:
            stage_comments_without_stage_evidence_debug.append(
                {"deal_id": deal_id, "stage_group": stage_group}
            )

        forbidden_english_terms_count += row_forbidden_english_terms_count
        training_jargon_count += row_training_jargon_count
        crm_uppercase_count += row_crm_uppercase_count
        lpu_typo_count += row_lpu_typo_count
        technical_terms_count += row_technical_terms_count
        style_lint_by_row.append(
            {
                "deal_id": deal_id,
                "forbidden_english_terms_count": row_forbidden_english_terms_count,
                "training_jargon_count": row_training_jargon_count,
                "crm_uppercase_count": row_crm_uppercase_count,
                "lpu_typo_count": row_lpu_typo_count,
                "technical_terms_count": row_technical_terms_count,
            }
        )

        rows_debug.append(
            {
                "row_index": idx,
                "deal_id": deal_id,
                "case_type": case_type,
                "quote_usage_count_by_row": row_quote_usage,
                "max_same_quote_usage": max_quote_this_row,
                "repeated_quotes": repeated_for_row,
                "irrelevant_stage_comment_count": row_irrelevant_comment,
                "irrelevant_stage_non_np_count": row_irrelevant_non_np,
                "generic_better_phrase_count": row_generic_count,
                "comments_with_not_relevant_text_count": row_has_not_relevant_text,
                "stage_comments_without_stage_evidence": row_stage_without_evidence,
                "stage_comments_without_stage_evidence_list": row_stage_without_evidence_items,
                "truncated_text_count": row_truncated,
                "unfinished_sentence_count": row_unfinished,
            }
        )

    better_phrase_usage = _collect_payload_better_phrase_usage(rows=rows)
    repeated_better_phrase_marker_count = _collect_payload_better_phrase_marker_count(rows=rows)
    (
        repeated_better_phrase_actual_duplicate_count,
        repeated_better_phrase_actual_duplicates,
    ) = _collect_better_phrase_actual_duplicates(usage=better_phrase_usage)
    repeated_better_phrase_count = repeated_better_phrase_actual_duplicate_count
    repeated_better_phrases_debug = repeated_better_phrase_actual_duplicates[:]
    if repeated_better_phrase_actual_duplicate_count > 0:
        warning_rules.append(
            {
                "rule": "repeated_better_phrase_actual_duplicates_present",
                "count": repeated_better_phrase_actual_duplicate_count,
            }
        )

    post_repair_broken_quotes: list[dict[str, Any]] = []
    style_has_crm_stats = isinstance(style_payload_normalization, dict) and (
        "final_payload_crm_stage_mismatch_mentions_count" in style_payload_normalization
    )
    style_has_bad_phrase_stats = isinstance(style_payload_normalization, dict) and (
        "final_payload_bad_phrase_count" in style_payload_normalization
    )
    style_has_empty_speech_stats = isinstance(style_payload_normalization, dict) and (
        "final_payload_empty_speech_module_count" in style_payload_normalization
    )
    for row_idx, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            continue
        deal_id = str(row.get("Deal ID") or "")
        manager_name = _sanitize_person_name(str(row.get("Менеджер") or ""))
        for key, raw_value in row.items():
            if _is_final_payload_lint_excluded_field(str(key or "")):
                continue
            value = str(raw_value or "")
            if not value.strip():
                continue
            final_payload_checked_fields_count += 1
            final_payload_forbidden_terms_count += _count_regex_hits(value, FORBIDDEN_ENGLISH_TERM_PATTERNS)
            final_payload_forbidden_terms_count += _count_regex_hits(value, TECHNICAL_TERMS_PATTERNS)
            final_payload_training_jargon_count += _count_regex_hits(value, TRAINING_JARGON_PATTERNS)
            crm_hits = _count_regex_hits(value, CRM_STAGE_MISMATCH_PATTERNS)
            if not style_has_crm_stats and crm_hits > 0:
                final_payload_crm_stage_mismatch_mentions_count += crm_hits
                final_payload_crm_stage_mismatch_unrepaired_count += crm_hits
                if len(final_payload_crm_stage_mismatch_examples) < 5:
                    final_payload_crm_stage_mismatch_examples.append(
                        {
                            "deal_id": deal_id,
                            "column": str(key or ""),
                            "before": value,
                            "after": value,
                            "repair_status": "unrepaired",
                            "reason": "crm_stage_mismatch_forbidden_in_user_facing_payload",
                        }
                    )
            bad_hits = _count_bad_phrase_hits(value)
            if not style_has_bad_phrase_stats and bad_hits > 0:
                final_payload_bad_phrase_count += bad_hits
                final_payload_bad_phrase_unrepaired_count += bad_hits
                if len(final_payload_bad_phrase_examples) < 5:
                    final_payload_bad_phrase_examples.append(
                        {
                            "deal_id": deal_id,
                            "column": str(key or ""),
                            "before": value,
                            "after": value,
                            "repair_status": "unrepaired",
                            "reason": "bad_phrase_or_truncated_marker",
                        }
                    )
            empty_speech_hits = _count_empty_speech_module_hits(value)
            if not style_has_empty_speech_stats and empty_speech_hits > 0:
                final_payload_empty_speech_module_count += empty_speech_hits
                final_payload_empty_speech_module_unrepaired_count += empty_speech_hits
                if len(final_payload_empty_speech_module_examples) < 5:
                    final_payload_empty_speech_module_examples.append(
                        {
                            "deal_id": deal_id,
                            "column": str(key or ""),
                            "before": value,
                            "after": value,
                            "repair_status": "unrepaired",
                            "reason": "empty_speech_module_forbidden_in_user_facing_payload",
                        }
                    )
            if _has_broken_payload_quotes(text=value):
                post_repair_broken_quotes.append(
                    {
                        "deal_id": deal_id,
                        "manager": manager_name,
                        "column": str(key or ""),
                        "before": value,
                        "after": value,
                        "repair_status": "unrepaired",
                    }
                )
            if _has_truncated_payload_tail(text=value):
                final_payload_truncated_text_count += 1
                if len(final_payload_truncated_examples) < 5:
                    final_payload_truncated_examples.append(
                        {
                            "row_index": row_idx,
                            "deal_id": deal_id,
                            "manager": manager_name,
                            "column": str(key or ""),
                            "value": value,
                            "reason": _truncated_payload_reason(text=value),
                        }
                    )

    if post_repair_broken_quotes:
        final_payload_broken_quotes_unrepaired_count = len(post_repair_broken_quotes)
    else:
        final_payload_broken_quotes_unrepaired_count = 0
    if final_payload_broken_quotes_count < final_payload_broken_quotes_unrepaired_count:
        final_payload_broken_quotes_count = final_payload_broken_quotes_unrepaired_count
    final_payload_broken_quotes_repaired_count = max(
        0,
        int(final_payload_broken_quotes_count) - int(final_payload_broken_quotes_unrepaired_count),
    )
    if post_repair_broken_quotes:
        known = {
            (
                str(item.get("deal_id") or ""),
                str(item.get("column") or ""),
                str(item.get("repair_status") or ""),
            )
            for item in broken_quote_examples
            if isinstance(item, dict)
        }
        for item in post_repair_broken_quotes:
            key = (
                str(item.get("deal_id") or ""),
                str(item.get("column") or ""),
                str(item.get("repair_status") or ""),
            )
            if key in known:
                continue
            broken_quote_examples.append(item)
            known.add(key)
            if len(broken_quote_examples) >= 5:
                break

    if final_payload_forbidden_terms_count > 0:
        failed_rules.append({"rule": "final_payload_forbidden_terms_present", "count": final_payload_forbidden_terms_count})
    if final_payload_crm_stage_mismatch_unrepaired_count > 0:
        failed_rules.append(
            {
                "rule": "final_payload_crm_stage_mismatch_unrepaired_present",
                "count": final_payload_crm_stage_mismatch_unrepaired_count,
            }
        )
    if final_payload_bad_phrase_unrepaired_count > 0:
        failed_rules.append(
            {
                "rule": "final_payload_bad_phrase_unrepaired_present",
                "count": final_payload_bad_phrase_unrepaired_count,
            }
        )
    if final_payload_empty_speech_module_unrepaired_count > 0:
        failed_rules.append(
            {
                "rule": "final_payload_empty_speech_module_unrepaired_present",
                "count": final_payload_empty_speech_module_unrepaired_count,
            }
        )
    if final_payload_broken_quotes_unrepaired_count > 0:
        failed_rules.append(
            {"rule": "final_payload_broken_quotes_present", "count": final_payload_broken_quotes_unrepaired_count}
        )
        failed_rules.append(
            {
                "rule": "final_payload_broken_quotes_unrepaired_present",
                "count": final_payload_broken_quotes_unrepaired_count,
            }
        )
    if final_payload_training_jargon_count > 0:
        failed_rules.append({"rule": "final_payload_training_jargon_present", "count": final_payload_training_jargon_count})
    if final_payload_truncated_text_count > 0:
        failed_rules.append({"rule": "final_payload_truncated_text_present", "count": final_payload_truncated_text_count})

    if comments_with_not_relevant_text_count > 0:
        failed_rules.append({"rule": "contains_not_relevant_text", "count": comments_with_not_relevant_text_count})
    if max_same_quote_usage_per_row > 2:
        failed_rules.append({"rule": "max_same_quote_usage_per_row_gt_2", "value": max_same_quote_usage_per_row})
    if truncated_text_count > 0:
        failed_rules.append({"rule": "truncated_text_present", "count": truncated_text_count})
    if unfinished_sentence_count > 0:
        failed_rules.append({"rule": "unfinished_sentence_present", "count": unfinished_sentence_count})
    if forbidden_english_terms_count > 0:
        failed_rules.append({"rule": "forbidden_english_terms_present", "count": forbidden_english_terms_count})
    if training_jargon_count > 0:
        failed_rules.append({"rule": "training_jargon_present", "count": training_jargon_count})
    if crm_uppercase_count > 0:
        failed_rules.append({"rule": "crm_uppercase_present", "count": crm_uppercase_count})
    if technical_terms_count > 0:
        failed_rules.append({"rule": "technical_terms_present", "count": technical_terms_count})
    if generic_better_phrase_count > 2:
        failed_rules.append({"rule": "generic_better_phrase_count_gt_2", "count": generic_better_phrase_count})
    if repeated_better_phrase_blocking and repeated_better_phrase_actual_duplicate_count > 0:
        failed_rules.append(
            {
                "rule": "repeated_better_phrase_actual_duplicate_count_gt_0",
                "count": repeated_better_phrase_actual_duplicate_count,
            }
        )

    return {
        "passed": len(failed_rules) == 0,
        "irrelevant_stage_comment_count": irrelevant_stage_comment_count,
        "irrelevant_stage_non_np_count": irrelevant_stage_non_np_count,
        "repeated_quote_count": repeated_quote_count,
        "max_same_quote_usage_per_row": max_same_quote_usage_per_row,
        "forbidden_english_terms_count": forbidden_english_terms_count,
        "training_jargon_count": training_jargon_count,
        "generic_better_phrase_count": generic_better_phrase_count,
        "repeated_better_phrase_count": repeated_better_phrase_count,
        "repeated_better_phrase_marker_count": repeated_better_phrase_marker_count,
        "repeated_better_phrase_actual_duplicate_count": repeated_better_phrase_actual_duplicate_count,
        "repeated_better_phrase_actual_duplicates": repeated_better_phrase_actual_duplicates[:20],
        "repeated_better_phrase_blocking": repeated_better_phrase_blocking,
        "crm_uppercase_count": crm_uppercase_count,
        "lpu_typo_count": lpu_typo_count,
        "technical_terms_count": technical_terms_count,
        "final_payload_forbidden_terms_count": final_payload_forbidden_terms_count,
        "final_payload_broken_quotes_count": final_payload_broken_quotes_count,
        "final_payload_broken_quotes_repaired_count": final_payload_broken_quotes_repaired_count,
        "final_payload_broken_quotes_unrepaired_count": final_payload_broken_quotes_unrepaired_count,
        "final_payload_training_jargon_count": final_payload_training_jargon_count,
        "final_payload_truncated_text_count": final_payload_truncated_text_count,
        "final_payload_truncated_examples": final_payload_truncated_examples,
        "final_payload_checked_fields_count": final_payload_checked_fields_count,
        "final_payload_crm_stage_mismatch_mentions_count": final_payload_crm_stage_mismatch_mentions_count,
        "final_payload_crm_stage_mismatch_repaired_count": final_payload_crm_stage_mismatch_repaired_count,
        "final_payload_crm_stage_mismatch_unrepaired_count": final_payload_crm_stage_mismatch_unrepaired_count,
        "final_payload_bad_phrase_count": final_payload_bad_phrase_count,
        "final_payload_bad_phrase_repaired_count": final_payload_bad_phrase_repaired_count,
        "final_payload_bad_phrase_unrepaired_count": final_payload_bad_phrase_unrepaired_count,
        "final_payload_empty_speech_module_count": final_payload_empty_speech_module_count,
        "final_payload_empty_speech_module_repaired_count": final_payload_empty_speech_module_repaired_count,
        "final_payload_empty_speech_module_unrepaired_count": final_payload_empty_speech_module_unrepaired_count,
        "final_payload_crm_stage_mismatch_examples": final_payload_crm_stage_mismatch_examples,
        "final_payload_bad_phrase_examples": final_payload_bad_phrase_examples,
        "final_payload_empty_speech_module_examples": final_payload_empty_speech_module_examples,
        "broken_quote_examples": broken_quote_examples,
        "comments_with_not_relevant_text_count": comments_with_not_relevant_text_count,
        "stage_comments_without_distinct_evidence_count": stage_comments_without_distinct_evidence_count,
        "truncated_text_count": truncated_text_count,
        "unfinished_sentence_count": unfinished_sentence_count,
        "comments_cleared_due_to_irrelevant_stage": comments_cleared_due_to_irrelevant_stage,
        "repeated_quotes": repeated_quotes_debug,
        "stage_comments_without_stage_evidence": stage_comments_without_stage_evidence_debug,
        "comments_cleared_due_to_irrelevant_stage_details": comments_cleared_due_to_irrelevant_stage_debug,
        "repeated_better_phrases": repeated_better_phrases_debug,
        "style_lint_by_row": style_lint_by_row,
        "better_phrase_usage": better_phrase_usage,
        "style_replacements_applied": (
            style_payload_normalization.get("style_replacements_applied", {})
            if isinstance(style_payload_normalization, dict)
            else {}
        ),
        "style_replacements_applied_top": (
            style_payload_normalization.get("style_replacements_applied_top", [])
            if isinstance(style_payload_normalization, dict)
            else []
        ),
        "style_normalizer": (
            style_payload_normalization
            if isinstance(style_payload_normalization, dict)
            else {}
        ),
        "failed_rules": failed_rules,
        "warning_rules": warning_rules,
        "rows_debug": rows_debug,
        "case_type_source_counts": case_type_source_counts,
        "autoanswer_calls_detected": autoanswer_calls_detected,
        "autoanswer_deals_skipped": autoanswer_deals_skipped,
        "autoanswer_by_manager": autoanswer_by_manager,
    }


def replay_call_review_payload_preflight(
    *,
    payload: dict[str, Any],
    debug_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rows = payload.get("rows", []) if isinstance(payload.get("rows"), list) else []
    rows_dict = [dict(item) for item in rows if isinstance(item, dict)]
    row_entries = _build_row_entries_for_replay(
        rows=rows_dict,
        debug_payload=debug_payload if isinstance(debug_payload, dict) else {},
    )
    style_payload_normalization = _apply_payload_style_normalization(row_entries=row_entries)
    normalized_rows = [x.get("row", {}) for x in row_entries if isinstance(x.get("row"), dict)]
    final_payload_quote_repair = _repair_final_payload_broken_quotes(rows=normalized_rows)
    if isinstance(style_payload_normalization, dict):
        style_payload_normalization["final_payload_quote_repair"] = final_payload_quote_repair
    semantic_preflight = _run_semantic_preflight(
        rows=normalized_rows,
        row_entries=row_entries,
        style_payload_normalization=style_payload_normalization,
        final_payload_quote_repair=final_payload_quote_repair,
    )
    payload_for_writer = {
        "mode": str(payload.get("mode") or "call_review_sheet"),
        "schema_version": str(payload.get("schema_version") or "v3"),
        "sheet_name": str(payload.get("sheet_name") or "Разбор звонков"),
        "start_cell": str(payload.get("start_cell") or "A2"),
        "columns": (
            payload.get("columns", [])
            if isinstance(payload.get("columns"), list)
            else list(CALL_REVIEW_V3_COLUMNS)
        ),
        "rows": normalized_rows,
        "rows_count": len(normalized_rows),
    }
    return {
        "payload_for_writer": payload_for_writer,
        "semantic_preflight": semantic_preflight,
        "style_payload_normalization": style_payload_normalization,
        "final_payload_quote_repair": final_payload_quote_repair,
        "row_entries": row_entries,
    }


def _build_row_entries_for_replay(*, rows: list[dict[str, Any]], debug_payload: dict[str, Any]) -> list[dict[str, Any]]:
    selection_debug = (
        debug_payload.get("selection_debug", {})
        if isinstance(debug_payload.get("selection_debug"), dict)
        else {}
    )
    details = selection_debug.get("details", []) if isinstance(selection_debug.get("details"), list) else []
    semantic_by_deal: dict[str, list[dict[str, Any]]] = {}
    for detail in details:
        if not isinstance(detail, dict):
            continue
        semantic_debug = detail.get("semantic_debug")
        if not isinstance(semantic_debug, dict):
            continue
        deal_id = str(detail.get("deal_id") or "").strip()
        if not deal_id:
            continue
        semantic_by_deal.setdefault(deal_id, []).append(dict(semantic_debug))
    row_entries: list[dict[str, Any]] = []
    for row in rows:
        deal_id = str(row.get("Deal ID") or "").strip()
        semantic_list = semantic_by_deal.get(deal_id, [])
        semantic_debug = semantic_list.pop(0) if semantic_list else {}
        row_entries.append({"row": row, "semantic_debug": semantic_debug})
    return row_entries


def _count_regex_hits(text: str, patterns: tuple[re.Pattern[str], ...]) -> int:
    if not text:
        return 0
    total = 0
    for pattern in patterns:
        total += len(pattern.findall(text))
    return total


def _norm_field_name(value: str) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _is_final_payload_lint_excluded_field(field_name: str) -> bool:
    norm = _norm_field_name(field_name)
    if not norm:
        return False
    if norm in FINAL_PAYLOAD_EXCLUDED_COLUMNS_NORM:
        return True
    if "url" in norm:
        return True
    return False


def _has_broken_payload_quotes(*, text: str) -> bool:
    value = str(text or "")
    if not value.strip():
        return False
    if re.search(r'(?:Лучше сказать|Используй):\s*"', value, flags=re.IGNORECASE) and not re.search(
        r'(?:Лучше сказать|Используй):\s*"[^"]+"',
        value,
        flags=re.IGNORECASE,
    ):
        return True
    if '""' in value:
        return True
    if value.count('"') % 2 == 1:
        return True
    if re.search(r':\s*$', value):
        return True
    return False


def _has_truncated_payload_tail(*, text: str) -> bool:
    value = str(text or "")
    if not value.strip():
        return False
    if _truncated_payload_reason(text=value):
        return True
    return False


def _truncated_payload_reason(*, text: str) -> str:
    value = str(text or "")
    if not value.strip():
        return ""
    low = value.lower().strip()
    if re.search(r"\bлучше\s*$", low, flags=re.IGNORECASE):
        return "ends_with_luchshe"
    if re.search(r"лучше\s+лучше\s+сказать", low, flags=re.IGNORECASE):
        return "double_better_phrase_marker"
    if re.search(r"\bнеч\s*тко\b", low, flags=re.IGNORECASE) or re.search(
        r"(?<!не)\bч\s*тко\b", low, flags=re.IGNORECASE
    ):
        return "broken_word_spacing"
    if re.search(r'(?:Лучше сказать|Используй):\s*"?[^"]*$', value, flags=re.IGNORECASE):
        return "unfinished_better_phrase"
    if value.count('"') % 2 == 1:
        return "odd_quote_count"
    for pattern in FINAL_PAYLOAD_TRUNCATED_PATTERNS:
        if pattern.search(value):
            return f"pattern:{pattern.pattern}"
    return ""


def _extract_better_phrases(text: str) -> list[str]:
    out: list[str] = []
    for item in BETTER_PHRASE_RE.findall(str(text or "")):
        phrase = _sanitize_user_text(item)
        if phrase:
            out.append(phrase)
    return out


def _collect_payload_better_phrase_usage(*, rows: list[dict[str, Any]]) -> dict[str, int]:
    usage: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        for col in NARRATIVE_USER_FACING_COLUMNS:
            value = str(row.get(col) or "")
            if not value.strip():
                continue
            for phrase in _extract_better_phrases(value):
                key = _sanitize_user_text(phrase).lower()
                if not key:
                    continue
                usage[key] = int(usage.get(key, 0) or 0) + 1
    return usage


def _collect_payload_better_phrase_marker_count(*, rows: list[dict[str, Any]]) -> int:
    total = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        for col in NARRATIVE_USER_FACING_COLUMNS:
            value = str(row.get(col) or "")
            if not value.strip():
                continue
            total += len(BETTER_PHRASE_MARKER_RE.findall(value))
    return total


def _collect_better_phrase_actual_duplicates(*, usage: dict[str, int]) -> tuple[int, list[dict[str, Any]]]:
    duplicate_count = 0
    examples: list[dict[str, Any]] = []
    for phrase, count_raw in usage.items():
        count = int(count_raw or 0)
        if count <= 1:
            continue
        duplicate_count += count - 1
        examples.append({"phrase": str(phrase or ""), "count": count})
    examples.sort(key=lambda item: int(item.get("count", 0) or 0), reverse=True)
    return duplicate_count, examples


def _apply_payload_style_normalization(*, row_entries: list[dict[str, Any]]) -> dict[str, Any]:
    replacement_totals: dict[str, int] = {}
    better_phrase_usage: dict[str, int] = {}
    better_phrase_replacements = 0
    stage_comment_columns = {str(meta.get("comment_col") or "") for meta in STAGE_GROUPS.values()}

    for entry in row_entries:
        if not isinstance(entry, dict):
            continue
        row = entry.get("row")
        if not isinstance(row, dict):
            continue
        semantic_debug = entry.get("semantic_debug")
        if not isinstance(semantic_debug, dict):
            semantic_debug = {}
            entry["semantic_debug"] = semantic_debug
        style_replacements = semantic_debug.get("style_replacements_applied")
        if isinstance(style_replacements, dict):
            for key, value in style_replacements.items():
                replacement_totals[str(key)] = int(replacement_totals.get(str(key), 0) or 0) + int(value or 0)

        for col in NARRATIVE_USER_FACING_COLUMNS:
            raw_value = row.get(col)
            if not isinstance(raw_value, str) or not raw_value.strip():
                continue
            stage_group = COMMENT_COLUMN_TO_STAGE_GROUP.get(col, "")
            value = _sanitize_user_text(str(raw_value))
            matches = BETTER_PHRASE_RE.findall(value)
            for phrase in matches:
                cleaned = _sanitize_user_text(phrase)
                if not cleaned:
                    continue
                key = cleaned.lower()
                seen = int(better_phrase_usage.get(key, 0) or 0)
                if seen < 2:
                    better_phrase_usage[key] = seen + 1
                    continue
                replacement = _pick_better_phrase_variant(
                    stage_group=stage_group,
                    original_phrase="",
                    seed=f"{row.get('Deal ID')}-{col}-{seen}",
                    taken=better_phrase_usage,
                )
                if replacement and replacement != cleaned:
                    old_piece = f'Лучше сказать: "{phrase}"'
                    new_piece = f'Лучше сказать: "{replacement}"'
                    if old_piece in value:
                        value = value.replace(old_piece, new_piece, 1)
                    else:
                        value = BETTER_PHRASE_RE.sub(new_piece, value, count=1)
                    better_phrase_replacements += 1
                repl_key = _sanitize_user_text(replacement or cleaned).lower()
                better_phrase_usage[repl_key] = int(better_phrase_usage.get(repl_key, 0) or 0) + 1
            value = _sanitize_user_text(value)
            if col in stage_comment_columns:
                compacted = _compact_stage_comment(text=value, stage_group=stage_group)
                if compacted != value:
                    better_phrase_replacements += 1
                value = compacted
            row[col] = value

    repeated_rewrite = _rewrite_payload_repeated_better_phrases(row_entries=row_entries)
    better_phrase_replacements += int(repeated_rewrite.get("replaced", 0) or 0)
    better_phrase_usage = repeated_rewrite.get("usage", {}) if isinstance(repeated_rewrite.get("usage"), dict) else {}
    final_repairs = _apply_final_narrative_repairs(row_entries=row_entries)
    better_phrase_replacements += int(final_repairs.get("better_phrase_marker_replacements", 0) or 0)

    replacements_top = [
        {"replacement": k, "count": int(v or 0)}
        for k, v in sorted(replacement_totals.items(), key=lambda x: (-int(x[1] or 0), str(x[0])))
        if int(v or 0) > 0
    ][:50]
    return {
        "enabled": True,
        "style_replacements_applied": replacement_totals,
        "style_replacements_applied_top": replacements_top,
        "better_phrase_replacements": better_phrase_replacements,
        "better_phrase_usage_after_normalization": better_phrase_usage,
        "better_phrase_rewrite": repeated_rewrite,
        "final_payload_crm_stage_mismatch_mentions_count": int(
            final_repairs.get("final_payload_crm_stage_mismatch_mentions_count", 0) or 0
        ),
        "final_payload_crm_stage_mismatch_repaired_count": int(
            final_repairs.get("final_payload_crm_stage_mismatch_repaired_count", 0) or 0
        ),
        "final_payload_crm_stage_mismatch_unrepaired_count": int(
            final_repairs.get("final_payload_crm_stage_mismatch_unrepaired_count", 0) or 0
        ),
        "final_payload_bad_phrase_count": int(final_repairs.get("final_payload_bad_phrase_count", 0) or 0),
        "final_payload_bad_phrase_repaired_count": int(
            final_repairs.get("final_payload_bad_phrase_repaired_count", 0) or 0
        ),
        "final_payload_bad_phrase_unrepaired_count": int(
            final_repairs.get("final_payload_bad_phrase_unrepaired_count", 0) or 0
        ),
        "final_payload_empty_speech_module_count": int(
            final_repairs.get("final_payload_empty_speech_module_count", 0) or 0
        ),
        "final_payload_empty_speech_module_repaired_count": int(
            final_repairs.get("final_payload_empty_speech_module_repaired_count", 0) or 0
        ),
        "final_payload_empty_speech_module_unrepaired_count": int(
            final_repairs.get("final_payload_empty_speech_module_unrepaired_count", 0) or 0
        ),
        "final_payload_crm_stage_mismatch_examples": (
            final_repairs.get("final_payload_crm_stage_mismatch_examples", [])
            if isinstance(final_repairs.get("final_payload_crm_stage_mismatch_examples"), list)
            else []
        ),
        "final_payload_bad_phrase_examples": (
            final_repairs.get("final_payload_bad_phrase_examples", [])
            if isinstance(final_repairs.get("final_payload_bad_phrase_examples"), list)
            else []
        ),
        "final_payload_empty_speech_module_examples": (
            final_repairs.get("final_payload_empty_speech_module_examples", [])
            if isinstance(final_repairs.get("final_payload_empty_speech_module_examples"), list)
            else []
        ),
    }


def _apply_final_narrative_repairs(*, row_entries: list[dict[str, Any]]) -> dict[str, Any]:
    crm_mentions_count = 0
    crm_repaired_count = 0
    crm_unrepaired_count = 0
    bad_phrase_count = 0
    bad_phrase_repaired_count = 0
    bad_phrase_unrepaired_count = 0
    empty_speech_module_count = 0
    empty_speech_module_repaired_count = 0
    empty_speech_module_unrepaired_count = 0
    better_phrase_marker_replacements = 0
    crm_examples: list[dict[str, Any]] = []
    bad_examples: list[dict[str, Any]] = []
    empty_speech_examples: list[dict[str, Any]] = []
    narrative_fields = set(NARRATIVE_USER_FACING_COLUMNS) | {"Эффект количество / неделя"}

    for entry in row_entries:
        if not isinstance(entry, dict):
            continue
        row = entry.get("row")
        if not isinstance(row, dict):
            continue
        deal_id = str(row.get("Deal ID") or "")
        for col in narrative_fields:
            before = str(row.get(col) or "")
            if not before.strip():
                continue
            crm_before = _count_regex_hits(before, CRM_STAGE_MISMATCH_PATTERNS)
            bad_before = _count_bad_phrase_hits(before)
            empty_speech_before = _count_empty_speech_module_hits(before)

            repaired = _repair_crm_stage_mismatch_and_bad_phrases(before)
            after = _sanitize_user_text(repaired)
            row[col] = after

            if "Лучше сказать:" in before and "Используй:" in after:
                better_phrase_marker_replacements += before.count("Лучше сказать:")

            if crm_before > 0:
                crm_mentions_count += crm_before
                crm_after = _count_regex_hits(after, CRM_STAGE_MISMATCH_PATTERNS)
                repaired_delta = max(0, crm_before - crm_after)
                crm_repaired_count += repaired_delta
                crm_unrepaired_count += crm_after
                if len(crm_examples) < 5:
                    crm_examples.append(
                        {
                            "deal_id": deal_id,
                            "column": str(col or ""),
                            "before": before,
                            "after": after,
                            "repair_status": "repaired" if crm_after == 0 else "unrepaired",
                            "reason": "crm_stage_mismatch_forbidden_in_user_facing_payload",
                        }
                    )

            if bad_before > 0:
                bad_phrase_count += bad_before
                bad_after = _count_bad_phrase_hits(after)
                repaired_delta = max(0, bad_before - bad_after)
                bad_phrase_repaired_count += repaired_delta
                bad_phrase_unrepaired_count += bad_after
                if len(bad_examples) < 5:
                    bad_examples.append(
                        {
                            "deal_id": deal_id,
                            "column": str(col or ""),
                            "before": before,
                            "after": after,
                            "repair_status": "repaired" if bad_after == 0 else "unrepaired",
                            "reason": "bad_phrase_or_truncated_marker",
                        }
                    )
            if empty_speech_before > 0:
                empty_speech_module_count += empty_speech_before
                empty_speech_after = _count_empty_speech_module_hits(after)
                repaired_delta = max(0, empty_speech_before - empty_speech_after)
                empty_speech_module_repaired_count += repaired_delta
                empty_speech_module_unrepaired_count += empty_speech_after
                empty_after_cleanup = not bool(after.strip())
                if empty_after_cleanup:
                    empty_speech_module_unrepaired_count += 1
                if len(empty_speech_examples) < 5:
                    empty_speech_examples.append(
                        {
                            "deal_id": deal_id,
                            "column": str(col or ""),
                            "before": before,
                            "after": after,
                            "repair_status": (
                                "repaired"
                                if empty_speech_after == 0 and not empty_after_cleanup
                                else "unrepaired"
                            ),
                            "reason": (
                                "empty_field_after_empty_speech_module_removal"
                                if empty_after_cleanup
                                else "empty_speech_module_forbidden_in_user_facing_payload"
                            ),
                        }
                    )

    return {
        "final_payload_crm_stage_mismatch_mentions_count": crm_mentions_count,
        "final_payload_crm_stage_mismatch_repaired_count": crm_repaired_count,
        "final_payload_crm_stage_mismatch_unrepaired_count": crm_unrepaired_count,
        "final_payload_bad_phrase_count": bad_phrase_count,
        "final_payload_bad_phrase_repaired_count": bad_phrase_repaired_count,
        "final_payload_bad_phrase_unrepaired_count": bad_phrase_unrepaired_count,
        "final_payload_empty_speech_module_count": empty_speech_module_count,
        "final_payload_empty_speech_module_repaired_count": empty_speech_module_repaired_count,
        "final_payload_empty_speech_module_unrepaired_count": empty_speech_module_unrepaired_count,
        "final_payload_crm_stage_mismatch_examples": crm_examples,
        "final_payload_bad_phrase_examples": bad_examples,
        "final_payload_empty_speech_module_examples": empty_speech_examples,
        "better_phrase_marker_replacements": better_phrase_marker_replacements,
    }


def _repair_crm_stage_mismatch_and_bad_phrases(text: str) -> str:
    value = str(text or "")
    if not value:
        return ""

    neutral_phrase = (
        "По звонку не хватает фиксации фактов: с кем говорили, какая боль, какой следующий шаг и дата."
    )
    for pattern in CRM_STAGE_MISMATCH_PATTERNS:
        value = pattern.sub(neutral_phrase, value)

    value = re.sub(
        r"(?:по\s+этапу\s+должно\s+быть[^.!\n]*)",
        "В разговоре не зафиксирован следующий управляемый шаг.",
        value,
        flags=re.IGNORECASE,
    )
    value = re.sub(
        r"(?:в\s+црм\s+стадия[^.!\n]*а\s+по\s+факту[^.!\n]*)",
        neutral_phrase,
        value,
        flags=re.IGNORECASE,
    )

    replacements: tuple[tuple[str, str], ...] = (
        (r"Лучше\s+сказать\s*:", "Используй:"),
        (r"\bпройти\s+как\s+действовать\s+квалификаци[а-я]*\b", "Разобрать квалификацию клиента"),
        (r"\bразобрать\s+как\s+действовать\s+квалификаци[а-я]*\b", "Разобрать квалификацию клиента"),
        (r"\bотработать\s+как\s+действовать\s+квалификаци[а-я]*\b", "Разобрать квалификацию клиента"),
        (r"\bкак\s+действовать\s+квалификаци[а-я]*\b", "разобрать квалификацию клиента"),
        (r"\bкак\s+действовать\s+выявлени[яе]\s+потребност[еяй]+\b", "разобрать выявление потребностей"),
        (r"\bкак\s+действовать\s+фиксаци[ия]\s+следующ[её]го\s+шаг[а-я]*\b", "разобрать фиксацию следующего шага"),
        (r"\bкак\s+действовать\s+фиксаци[а-я]*\b", "разобрать фиксацию следующего шага"),
        (r"\bкак\s+действовать\s+выявлени[а-я]*\b", "разобрать выявление потребностей"),
        (r"\bкак\s+действовать\s+работ[а-я]*\b", "разобрать рабочие действия в звонке"),
        (r"\bкак\s+действовать\s+презентаци[а-я]*\b", "разобрать презентацию под задачу клиента"),
        (r"\bкак\s+действовать\s+отработк[а-я]*\b", "разобрать отработку возражений"),
        (r"\bкак\s+действовать\s+копания\b", "разобрать вопросы клиента глубже"),
        (r"\bкак\s+действовать\s+управления\b", "как управлять разговором"),
        (r"\bкак\s+действовать\s+выявления\b", "как выявлять потребность"),
        (r"\bТренировать\s+четк(?:ая|ую)\s+фраз(?:а|у)\b", "Отработать четкую фразу"),
        (r"\bотработать\s+быстрого\b", "отработать быстрое"),
        (r"\bотработать\s+глубиннн?ого\b", "отработать глубинное"),
        (r"\bВместо\s+нужно\s*:\s*-\s*", ""),
        (r"\bЛучше\s*:\s*-\s*", ""),
        (r"\b1\)\s*1\.", "1)"),
        (r"\bнеч\s*тко\b", "нечетко"),
        (r"(?<!не)\bч\s*тко\b", "четко"),
    )
    for pattern, repl in replacements:
        value = re.sub(pattern, repl, value, flags=re.IGNORECASE)

    value = re.sub(
        r"\b(?:пройти|разобрать|отработать)\s+как\s+действовать\s*[\"«]([^\"»]+)[\"»]",
        lambda m: f"Разобрать {_normalize_kak_deystvovat_topic(str(m.group(1) or '').strip().lower())}",
        value,
        flags=re.IGNORECASE,
    )
    value = re.sub(
        r"\bкак\s+действовать\s*[\"«]([^\"»]+)[\"»]",
        lambda m: f"разобрать {_normalize_kak_deystvovat_topic(str(m.group(1) or '').strip().lower())}",
        value,
        flags=re.IGNORECASE,
    )
    value = re.sub(
        r"\b(?:пройти|разобрать|отработать)\s+как\s+действовать\s+[а-яё][а-яё\s-]{2,}\b",
        "Разобрать этот этап разговора по фактам",
        value,
        flags=re.IGNORECASE,
    )
    value = re.sub(
        r"\bкак\s+действовать\s+[а-яё][а-яё\s-]{2,}\b",
        "разобрать этот этап разговора по фактам",
        value,
        flags=re.IGNORECASE,
    )
    value = re.sub(
        r"\b(?:пройти|разобрать|отработать)\s+как\s+действовать\b",
        "Разобрать этот этап разговора по фактам",
        value,
        flags=re.IGNORECASE,
    )
    value = re.sub(r"\bкак\s+действовать\b", "разобрать", value, flags=re.IGNORECASE)

    value = _remove_empty_speech_modules(value)
    value = re.sub(r"(?:Используй|Что исправить)\s*:\s*$", "", value, flags=re.IGNORECASE)
    value = re.sub(r"(?:Используй)\s*:\s*-\s*", "", value, flags=re.IGNORECASE)
    value = re.sub(r"-\s*$", "", value)
    value = re.sub(r"\s{2,}", " ", value).strip(" ,;:-")
    return value


def _normalize_kak_deystvovat_topic(topic: str) -> str:
    value = str(topic or "").strip().lower()
    if not value:
        return "этот этап разговора по фактам"
    if value.startswith("фиксация "):
        return "фиксацию " + value[len("фиксация ") :]
    if value.startswith("квалификация"):
        return "квалификацию клиента"
    if value.startswith("выявление"):
        return "выявление потребностей"
    return value


def _count_bad_phrase_hits(text: str) -> int:
    value = str(text or "")
    if not value.strip():
        return 0
    total = 0
    for pattern in BAD_PHRASE_PATTERNS:
        total += len(pattern.findall(value))
    if re.search(r"(?:Используй|Лучше сказать|Что исправить)\s*:\s*$", value, flags=re.IGNORECASE):
        total += 1
    if re.search(r"-\s*$", value):
        total += 1
    if _truncated_payload_reason(text=value):
        total += 1
    return total


def _count_empty_speech_module_hits(text: str) -> int:
    value = str(text or "")
    if not value.strip():
        return 0
    total = 0
    for pattern in EMPTY_SPEECH_MODULE_PATTERNS:
        total += len(pattern.findall(value))
    return total


def _remove_empty_speech_modules(text: str) -> str:
    value = str(text or "")
    if not value.strip():
        return ""
    for pattern in EMPTY_SPEECH_MODULE_PATTERNS:
        value = pattern.sub("", value)
    value = re.sub(r"\s{2,}", " ", value).strip(" ,;:-")
    return value


def _compact_stage_comment(*, text: str, stage_group: str) -> str:
    value = _sanitize_user_text(text)
    if not value:
        return ""
    quote_candidates = _extract_quotes(value)
    better_candidates = _extract_better_phrases(value)
    must_compact = (
        len(value) > 700
        or value.count(";") > 4
        or len(quote_candidates) > 1
        or len(better_candidates) > 1
        or "Лучше Лучше сказать" in value
    )
    if not must_compact:
        return value[:700]

    quote = _trim_compact_sentence(quote_candidates[0], 180) if quote_candidates else ""
    better = ""
    if better_candidates:
        better = _pick_better_phrase_variant(
            stage_group=stage_group,
            original_phrase=better_candidates[0],
            seed=f"compact-{value[:120]}",
            taken={},
        )
        better = _trim_compact_sentence(_sanitize_user_text(better), 220)

    base = re.sub(r'(?:Лучше сказать|Используй):\s*"[^"]*"', "", value, flags=re.IGNORECASE).strip()
    if quote:
        base = base.replace(f'"{quote}"', " ")
    base = re.sub(r"\s*;\s*", ". ", base)
    base = re.sub(r"\s{2,}", " ", base).strip(" ,;:-")
    sentence_chunks = [s.strip(" ,;:-") for s in re.split(r"[.!?]\s+", base) if s.strip(" ,;:-")]
    main = _trim_compact_sentence(sentence_chunks[0] if sentence_chunks else base, 260)
    action = ""
    for candidate in sentence_chunks[1:]:
        low = candidate.lower()
        if "лучше сказать" in low:
            continue
        if quote and candidate == quote:
            continue
        action = _trim_compact_sentence(candidate, 220)
        if action:
            break

    parts: list[str] = []
    if quote:
        parts.append(f'"{quote}" - {main}')
    else:
        parts.append(main)
    if action:
        parts.append(f"Что исправить: {action}")
    if better:
        parts.append(f'Лучше сказать: "{better}"')

    out = " ".join(x.strip() for x in parts if x.strip())
    out = _sanitize_user_text(out)
    if re.search(r"\bЛучше\s*$", out, flags=re.IGNORECASE):
        out = re.sub(r"\bЛучше\s*$", "", out, flags=re.IGNORECASE).strip(" ,;:-")
        fallback = _sanitize_user_text(
            _pick_better_phrase_variant(
                stage_group=stage_group,
                original_phrase="",
                seed=f"compact-fallback-{out[:80]}",
                taken={},
            )
        )
        if fallback:
            if out and out[-1] not in ".!?":
                out = f"{out}."
            out = f'{out} Лучше сказать: "{fallback}"'.strip()
    if re.search(r"(?:Лучше сказать|Используй):", out, flags=re.IGNORECASE) and not re.search(
        r'(?:Лучше сказать|Используй):\s*"[^"]+"',
        out,
        flags=re.IGNORECASE,
    ):
        fallback = _sanitize_user_text(
            _pick_better_phrase_variant(
                stage_group=stage_group,
                original_phrase="",
                seed=f"compact-repair-{out[:80]}",
                taken={},
            )
        )
        out = re.sub(r"(?:Лучше сказать|Используй):\s*\"?[^\"]*$", "", out, flags=re.IGNORECASE).strip(" ,;:-")
        if fallback:
            if out and out[-1] not in ".!?":
                out = f"{out}."
            out = f'{out} Лучше сказать: "{fallback}"'.strip()
    if out.count(";") > 2:
        out = out.replace(";", ".")
        out = _sanitize_user_text(out)
    return out[:700]


def _trim_compact_sentence(value: str, limit: int) -> str:
    text = _sanitize_user_text(value)
    if len(text) <= int(limit):
        return text
    trimmed = text[: int(limit)].rstrip(" ,;:-")
    if not trimmed:
        return ""
    if trimmed[-1] not in ".!?":
        trimmed = f"{trimmed}."
    return trimmed


def _rewrite_payload_repeated_better_phrases(*, row_entries: list[dict[str, Any]]) -> dict[str, Any]:
    occurrences: dict[str, list[dict[str, Any]]] = {}
    usage: dict[str, int] = {}
    for entry in row_entries:
        if not isinstance(entry, dict):
            continue
        row = entry.get("row")
        if not isinstance(row, dict):
            continue
        deal_id = str(row.get("Deal ID") or "")
        for col in NARRATIVE_USER_FACING_COLUMNS:
            value = str(row.get(col) or "")
            if not value.strip():
                continue
            stage_group = COMMENT_COLUMN_TO_STAGE_GROUP.get(col, "")
            phrases = _extract_better_phrases(value)
            for phrase in phrases:
                key = _sanitize_user_text(phrase).lower()
                if not key:
                    continue
                usage[key] = int(usage.get(key, 0) or 0) + 1
                occurrences.setdefault(key, []).append(
                    {
                        "row": row,
                        "col": col,
                        "stage_group": stage_group,
                        "deal_id": deal_id,
                        "phrase": phrase,
                    }
                )

    replaced = 0
    for phrase_key, items in occurrences.items():
        if len(items) <= 2:
            continue
        for idx, item in enumerate(items[2:], start=2):
            row = item.get("row")
            col = str(item.get("col") or "")
            if not isinstance(row, dict) or not col:
                continue
            stage_group = str(item.get("stage_group") or "")
            old_phrase = _sanitize_user_text(item.get("phrase"))
            replacement = _pick_better_phrase_variant(
                stage_group=stage_group,
                original_phrase="",
                seed=f"{item.get('deal_id')}-{col}-{idx}-{phrase_key}",
                taken=usage,
            )
            replacement = _sanitize_user_text(replacement)
            if not replacement:
                continue
            if replacement.lower() == phrase_key:
                replacement = _pick_better_phrase_variant(
                    stage_group=stage_group,
                    original_phrase="",
                    seed=f"{item.get('deal_id')}-{col}-{idx}-{phrase_key}-alt",
                    taken=usage,
                )
                replacement = _sanitize_user_text(replacement)
            if not replacement or replacement.lower() == phrase_key:
                continue
            value = str(row.get(col) or "")
            old_piece = f'Лучше сказать: "{old_phrase}"'
            new_piece = f'Лучше сказать: "{replacement}"'
            if old_piece in value:
                value = value.replace(old_piece, new_piece, 1)
            else:
                value = BETTER_PHRASE_RE.sub(new_piece, value, count=1)
            row[col] = _sanitize_user_text(value)
            usage[phrase_key] = max(0, int(usage.get(phrase_key, 0) or 0) - 1)
            replacement_key = replacement.lower()
            usage[replacement_key] = int(usage.get(replacement_key, 0) or 0) + 1
            replaced += 1

    return {"replaced": replaced, "usage": usage}


def _repair_final_payload_broken_quotes(*, rows: list[dict[str, Any]]) -> dict[str, Any]:
    broken_count = 0
    repaired_count = 0
    unrepaired_count = 0
    examples: list[dict[str, Any]] = []
    for row_idx, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            continue
        deal_id = str(row.get("Deal ID") or "")
        manager_name = _sanitize_person_name(str(row.get("Менеджер") or ""))
        for col, raw_value in row.items():
            field_name = str(col or "")
            if _is_final_payload_lint_excluded_field(field_name):
                continue
            if raw_value is None:
                continue
            before = str(raw_value or "")
            if not before.strip():
                continue
            if not _has_broken_payload_quotes(text=before):
                continue
            broken_count += 1
            after, repair_reason = _repair_broken_payload_quotes_text(text=before)
            row[col] = after
            repaired = not _has_broken_payload_quotes(text=after)
            if repaired:
                repaired_count += 1
                status = "repaired"
            else:
                unrepaired_count += 1
                status = "unrepaired"
            if len(examples) < 5:
                examples.append(
                    {
                        "row_index": row_idx,
                        "deal_id": deal_id,
                        "manager": manager_name,
                        "column": field_name,
                        "before": before,
                        "after": after,
                        "repair_status": status,
                        "repair_reason": repair_reason,
                    }
                )

    return {
        "final_payload_broken_quotes_count": broken_count,
        "final_payload_broken_quotes_repaired_count": repaired_count,
        "final_payload_broken_quotes_unrepaired_count": unrepaired_count,
        "broken_quote_examples": examples,
    }


def _repair_broken_payload_quotes_text(*, text: str) -> tuple[str, str]:
    value = str(text or "")
    if not value:
        return "", ""
    original = value
    reasons: list[str] = []

    if '""' in value:
        out, count = re.subn(r'""\s*([^"]*?)\s*""', r'"\1"', value)
        if count > 0:
            value = out
            reasons.append("double_quote_wrap")

    better_match = re.search(r"(?is)(.*?)(?:лучше сказать|используй):\s*(.*)$", value, flags=re.IGNORECASE)
    if better_match:
        prefix = str(better_match.group(1) or "").strip()
        marker = "Используй:"
        phrase = str(better_match.group(2) or "").strip()
        phrase = phrase.strip()
        phrase = phrase.strip('"')
        phrase = re.sub(r'"+\s*$', "", phrase).strip()
        if phrase and phrase[-1] not in ".!?":
            phrase = f"{phrase}."
        if prefix.count('"') % 2 == 1:
            prefix = re.sub(r'"([^"]*)$', r"\1", prefix).strip()
            reasons.append("remove_unmatched_quote_before_better_phrase")
        value = f'{prefix} {marker} "{phrase}"'.strip() if phrase else f"{prefix} {marker}".strip()
        reasons.append("normalize_better_phrase_segment")
    elif re.search(r'(?:Лучше сказать|Используй):\s*"[^"]*$', value, flags=re.IGNORECASE):
        value = re.sub(
            r'((?:Лучше сказать|Используй):\s*"[^"]*)$',
            r'\1"',
            value,
            flags=re.IGNORECASE,
        )
        reasons.append("close_better_phrase_quote")

    if value.count('"') % 2 == 1:
        better_tail = re.search(r'(?:Лучше сказать|Используй):\s*"([^"]*)$', value, flags=re.IGNORECASE)
        if better_tail:
            value = value + '"'
            reasons.append("append_terminal_quote")
        else:
            pos = value.rfind('"')
            if pos >= 0:
                value = (value[:pos] + value[pos + 1 :]).strip()
                reasons.append("remove_unmatched_quote")

    value = re.sub(r"\s{2,}", " ", value).strip()
    if value != original and not reasons:
        reasons.append("normalize_spacing")
    return value, "|".join(reasons)


def _selected_call_id_for_readiness(*, candidate: dict[str, Any], record: dict[str, Any]) -> str:
    for source in (
        candidate.get("selected_call_ids"),
        record.get("selected_call_ids"),
    ):
        if not isinstance(source, list):
            continue
        for raw in source:
            call_id = str(raw or "").strip()
            if call_id:
                return call_id
    for raw in (
        candidate.get("anchor_call_id"),
        record.get("anchor_call_id"),
        record.get("call_review_anchor_call_id"),
    ):
        call_id = str(raw or "").strip()
        if call_id:
            return call_id
    return ""


def _build_transcript_readiness_entry(*, candidate: dict[str, Any], record: dict[str, Any]) -> dict[str, Any]:
    snapshot = _load_snapshot(record=record)
    calls = _extract_calls(snapshot=snapshot)
    transcript_text_by_call_id = _build_transcript_text_by_call_id(snapshot=snapshot)
    selected_ids = _selected_call_ids(candidate=candidate, record=record)
    selected_call = _select_anchor_call(
        calls=calls,
        selected_call_ids=selected_ids,
        transcript_text_by_call_id=transcript_text_by_call_id,
    )
    selected_call_id = str((selected_call or {}).get("call_id") or "").strip() or _selected_call_id_for_readiness(
        candidate=candidate,
        record=record,
    )

    audio_path = str((selected_call or {}).get("audio_path") or "").strip()
    audio_path_exists = bool(audio_path and Path(audio_path).exists())
    audio_status = str((selected_call or {}).get("audio_download_status") or "").strip().lower()
    audio_source = ""
    if audio_path:
        if audio_status in {"cached", "local_exists", "resolved_file_url"}:
            audio_source = "cached"
        elif audio_status in {"downloaded", "downloaded_now"}:
            audio_source = "downloaded"
        elif audio_status:
            audio_source = audio_status
        else:
            audio_source = "downloaded"

    transcript_item: dict[str, Any] = {}
    for raw in snapshot.get("transcripts", []) if isinstance(snapshot.get("transcripts"), list) else []:
        if not isinstance(raw, dict):
            continue
        call_id = str(raw.get("call_id") or "").strip()
        if selected_call_id and call_id and call_id == selected_call_id:
            transcript_item = raw
            break
        if not transcript_item:
            transcript_item = raw

    transcript_text = str(transcript_item.get("transcript_text") or "").strip()
    transcript_chars = int(transcript_item.get("transcript_chars", 0) or len(transcript_text))
    excerpt = str(record.get("transcript_text_excerpt") or "").strip()
    transcript_preview = " ".join((excerpt or transcript_text)[:280].split()).strip()
    transcript_excerpt_chars = len(excerpt)
    transcript_status = str(transcript_item.get("transcript_status") or "").strip().lower()
    transcript_source = str(transcript_item.get("transcript_source") or record.get("transcript_source") or "").strip()
    stt_error = str(transcript_item.get("transcript_error") or record.get("transcript_error") or "").strip()
    transcript_usable = bool(transcript_item.get("transcript_usable"))
    if not transcript_usable:
        transcript_usable = bool(
            transcript_chars >= 20
            and transcript_status in {"ok", "cached"}
        )
    if not transcript_usable:
        readiness = record.get("call_review_transcript_readiness")
        if isinstance(readiness, dict):
            transcript_usable = bool(readiness.get("transcript_usable"))
    noise_profile = _noise_dialogue_profile(
        transcript_text=" ".join([transcript_text, excerpt]).strip(),
        transcript_chars=transcript_chars,
        call_duration_seconds=int((selected_call or {}).get("duration_seconds", 0) or 0),
    )

    return {
        "deal_id": str(candidate.get("deal_id") or record.get("deal_id") or "").strip(),
        "selected_call_id": selected_call_id,
        "call_duration_seconds": int((selected_call or {}).get("duration_seconds", 0) or 0),
        "audio_path_exists": audio_path_exists,
        "audio_source": audio_source,
        "audio_path": audio_path,
        "recording_url": str((selected_call or {}).get("recording_url") or "").strip(),
        "transcript_source": transcript_source,
        "transcript_chars": int(transcript_chars),
        "transcript_excerpt_chars": int(transcript_excerpt_chars),
        "transcript_preview": transcript_preview,
        "transcript_usable": bool(transcript_usable),
        "stt_status": transcript_status,
        "stt_error": stt_error,
        "initial_noise_detected": bool(noise_profile.get("initial_noise_detected")),
        "human_dialogue_after_noise_detected": bool(noise_profile.get("human_dialogue_after_noise_detected")),
        "noise_classification": str(noise_profile.get("noise_classification") or ""),
        "final_noise_decision": str(noise_profile.get("final_noise_decision") or ""),
        "skip_reason_before_llm": "",
        "final_decision": "",
    }


def _map_readiness_final_decision(*, skip_reason: str) -> str:
    reason = str(skip_reason or "").strip().lower()
    if reason in {"empty_transcript_after_stt", "transcript_not_ready"} or reason.startswith("transcript_"):
        return "skipped_empty_transcript"
    if reason in {"llm_not_ready", "llm_timeout", "validation_failed", "missing_required_stage_comment"}:
        return "skipped_after_llm"
    if reason in AUTOANSWER_SKIP_REASONS:
        return "skipped_not_meaningful_conversation"
    if reason.startswith("disallowed_case_mode") or reason in {
        "pool_not_conversation",
        "not_selected_for_transcription",
        "manager_not_in_allowlist",
        "telemarketer_case_out_of_scope",
        "anchor_call_noise",
        "anchor_call_too_short",
        "listened_calls_empty",
    }:
        return "skipped_case_filtered"
    if reason:
        return f"skipped_{reason}"
    return "skipped_unknown"


def _collect_autoanswer_skip_stats(*, cases_debug: list[dict[str, Any]]) -> dict[str, Any]:
    calls_detected = 0
    deals: set[str] = set()
    by_manager: dict[str, int] = {}
    for item in cases_debug:
        if not isinstance(item, dict):
            continue
        if not bool(item.get("skipped")):
            continue
        reason = str(item.get("skip_reason") or "").strip().lower()
        if reason not in AUTOANSWER_SKIP_REASONS:
            continue
        deal_id = str(item.get("deal_id") or "").strip()
        if deal_id:
            deals.add(deal_id)
        selected_call_count = int(item.get("selected_call_count", 0) or 0)
        calls_detected += max(1, selected_call_count)
        manager = " ".join(
            str(item.get("manager_name") or item.get("anchor_manager_name") or "").split()
        ).strip()
        if manager:
            by_manager[manager] = int(by_manager.get(manager, 0) or 0) + max(1, selected_call_count)
    return {
        "autoanswer_calls_detected": calls_detected,
        "autoanswer_deals_skipped": len(deals),
        "autoanswer_by_manager": by_manager,
    }


def _write_debug_artifacts(
    *,
    run_dir: Path | None,
    summary: dict[str, Any] | None,
    ledger: list[dict[str, Any]],
    call_ledger_all: list[dict[str, Any]] | None,
    call_ledger_audit: dict[str, Any] | None,
    anchor_shortlist: list[dict[str, Any]] | None,
    selected_anchor_cases: list[dict[str, Any] | Any] | None,
    cases_debug: list[dict[str, Any]],
    transcript_readiness_debug: list[dict[str, Any]],
    rows: list[dict[str, Any]],
    skipped_reasons: dict[str, int],
    abort_stage: str,
    abort_error: str,
    artifacts_written: list[str],
) -> dict[str, str]:
    if run_dir is None:
        return {}
    root = run_dir / "call_review_v3"
    root.mkdir(parents=True, exist_ok=True)
    ledger_all = (
        [x for x in call_ledger_all if isinstance(x, dict)]
        if isinstance(call_ledger_all, list)
        else [x for x in ledger if isinstance(x, dict)]
    )
    shortlist_items = (
        [x for x in anchor_shortlist if isinstance(x, dict)]
        if isinstance(anchor_shortlist, list)
        else []
    )
    selected_items = (
        [x for x in selected_anchor_cases if isinstance(x, dict)]
        if isinstance(selected_anchor_cases, list)
        else [x for x in cases_debug if isinstance(x, dict) and not bool(x.get("skipped"))]
    )
    selected_cases_by_manager: dict[str, int] = {}
    writer_rows_by_manager: dict[str, int] = {}
    skipped_after_llm_by_manager: dict[str, int] = {}
    autoanswer_stats = _collect_autoanswer_skip_stats(cases_debug=cases_debug)
    llm_skip_reasons = {"llm_not_ready", "llm_timeout", "validation_failed", "missing_required_stage_comment"}
    for selected in selected_items:
        if not isinstance(selected, dict):
            continue
        manager = " ".join(
            str(selected.get("anchor_manager_name") or selected.get("manager_name_from_call_author") or "").split()
        ).strip()
        if manager:
            selected_cases_by_manager[manager] = selected_cases_by_manager.get(manager, 0) + 1
    for row in rows:
        if not isinstance(row, dict):
            continue
        manager = " ".join(str(row.get("Менеджер") or "").split()).strip()
        if manager:
            writer_rows_by_manager[manager] = writer_rows_by_manager.get(manager, 0) + 1
    for debug_case in cases_debug:
        if not isinstance(debug_case, dict):
            continue
        if not bool(debug_case.get("skipped")):
            continue
        skip_reason = str(debug_case.get("skip_reason") or "")
        if skip_reason not in llm_skip_reasons:
            continue
        manager = " ".join(str(debug_case.get("manager_name") or "").split()).strip()
        if not manager:
            manager = " ".join(str(debug_case.get("anchor_manager_name") or "").split()).strip()
        if manager:
            skipped_after_llm_by_manager[manager] = skipped_after_llm_by_manager.get(manager, 0) + 1
    if not selected_cases_by_manager:
        for selected in selected_items:
            if not isinstance(selected, dict):
                continue
            manager = " ".join(str(selected.get("anchor_manager_name") or selected.get("manager_name_from_call_author") or "").split()).strip()
            if manager:
                selected_cases_by_manager[manager] = selected_cases_by_manager.get(manager, 0) + 1
    scores: list[int] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        raw = row.get("Оценка 0-100")
        try:
            scores.append(int(float(raw)))
        except Exception:
            continue
    score_sources: dict[str, int] = {}
    transcript_labels: dict[str, int] = {}
    case_type_source_counts: dict[str, int] = {}
    for case_item in cases_debug:
        if not isinstance(case_item, dict):
            continue
        score_source = str(case_item.get("score_source") or "").strip() or "unknown"
        score_sources[score_source] = int(score_sources.get(score_source, 0) or 0) + 1
        label = str(case_item.get("transcript_usability_label") or "").strip() or "unknown"
        transcript_labels[label] = int(transcript_labels.get(label, 0) or 0) + 1
        case_source = str(case_item.get("case_type_source") or "").strip() or "unknown"
        case_type_source_counts[case_source] = int(case_type_source_counts.get(case_source, 0) or 0) + 1
    skipped_after_stt = int(
        sum(
            1
            for x in cases_debug
            if isinstance(x, dict)
            and (
                str(x.get("skip_reason") or "").startswith("transcript_")
                or str(x.get("skip_reason") or "") in {"empty_transcript_after_stt", "transcript_not_ready"}
            )
        )
    )
    skipped_after_llm = int(
        sum(
            1
            for x in cases_debug
            if isinstance(x, dict)
            and str(x.get("skip_reason") or "") in {"llm_not_ready", "llm_timeout", "validation_failed", "missing_required_stage_comment"}
        )
    )

    ledger_path = root / "call_ledger_all.json"
    ledger_audit_path = root / "call_ledger_audit.json"
    shortlist_path = root / "anchor_shortlist.json"
    selected_path = root / "selected_anchor_cases.json"
    cases_path = root / "anchor_cases.json"
    rows_path = root / "writer_rows.json"
    transcript_readiness_path = root / "transcript_readiness_debug.json"
    row_flow_path = root / "row_flow_debug.json"
    summary_path = root / "pipeline_summary.json"
    case_type_audit_path = root / "case_type_audit.json"
    ledger_path.write_text(json.dumps(ledger_all, ensure_ascii=False, indent=2), encoding="utf-8")
    ledger_audit_path.write_text(
        json.dumps(call_ledger_audit if isinstance(call_ledger_audit, dict) else {}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    shortlist_path.write_text(json.dumps(shortlist_items, ensure_ascii=False, indent=2), encoding="utf-8")
    selected_path.write_text(json.dumps(selected_items, ensure_ascii=False, indent=2), encoding="utf-8")
    cases_path.write_text(json.dumps(cases_debug, ensure_ascii=False, indent=2), encoding="utf-8")
    rows_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    transcript_readiness_path.write_text(
        json.dumps(
            [x for x in transcript_readiness_debug if isinstance(x, dict)],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    case_type_audit_path.write_text(
        json.dumps(
            [
                {
                    "deal_id": str(item.get("deal_id") or ""),
                    "call_case_type": str(item.get("call_case_type") or ""),
                    "case_type_source": str(item.get("case_type_source") or ""),
                    "case_type_reason": str(item.get("case_type_reason") or ""),
                    "evidence_used": (
                        [str(x).strip() for x in item.get("evidence_used", []) if str(x).strip()]
                        if isinstance(item.get("evidence_used"), list)
                        else []
                    ),
                    "anchor_call_duration_seconds": int(item.get("anchor_call_duration_seconds", 0) or 0),
                    "anchor_call_timestamp": str(item.get("anchor_call_timestamp") or ""),
                    "anchor_manager_name": str(item.get("anchor_manager_name") or ""),
                }
                for item in selected_items
                if isinstance(item, dict)
            ],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    summary_payload = summary if isinstance(summary, dict) else {}
    shortlist_diag = (
        summary_payload.get("transcription_shortlist_diagnostics", {})
        if isinstance(summary_payload.get("transcription_shortlist_diagnostics"), dict)
        else {}
    )
    deals_selected_for_analysis = int(
        shortlist_diag.get("deals_selected_for_analysis", len(selected_items)) or len(selected_items)
    )
    llm_skip_reasons = {"llm_not_ready", "llm_timeout", "validation_failed", "missing_required_stage_comment"}
    transcript_skip_reasons = {"empty_transcript_after_stt", "transcript_not_ready"}
    readiness_by_deal: dict[str, dict[str, Any]] = {}
    for entry in transcript_readiness_debug:
        if not isinstance(entry, dict):
            continue
        did = str(entry.get("deal_id") or "").strip()
        if did and did not in readiness_by_deal:
            readiness_by_deal[did] = entry
    case_by_deal: dict[str, dict[str, Any]] = {}
    for entry in cases_debug:
        if not isinstance(entry, dict):
            continue
        did = str(entry.get("deal_id") or "").strip()
        if did and did not in case_by_deal:
            case_by_deal[did] = entry
    selected_by_deal: dict[str, dict[str, Any]] = {}
    for entry in selected_items:
        if not isinstance(entry, dict):
            continue
        did = str(entry.get("deal_id") or "").strip()
        if did and did not in selected_by_deal:
            selected_by_deal[did] = entry
    row_flow_entries: list[dict[str, Any]] = []
    for did in [str(x.get("deal_id") or "").strip() for x in selected_items if isinstance(x, dict)]:
        if not did:
            continue
        selected_item = selected_by_deal.get(did, {})
        case_debug = case_by_deal.get(did, {})
        readiness = readiness_by_deal.get(did, {})
        skip_reason = str(case_debug.get("skip_reason") or readiness.get("skip_reason_before_llm") or "").strip()
        call_duration = int(
            case_debug.get("anchor_call_duration_seconds", 0)
            or selected_item.get("anchor_call_duration_seconds", 0)
            or readiness.get("call_duration_seconds", 0)
            or 0
        )
        transcript_preview = str(readiness.get("transcript_preview") or "").strip()
        row_flow_entries.append(
            {
                "deal_id": did,
                "selected_call_id": str(
                    readiness.get("selected_call_id")
                    or selected_item.get("anchor_call_id")
                    or case_debug.get("anchor_call_id")
                    or ""
                ).strip(),
                "call_duration": call_duration,
                "transcript_chars": int(
                    readiness.get("transcript_chars", 0)
                    or case_debug.get("transcript_length_chars", 0)
                    or 0
                ),
                "transcript_preview": transcript_preview,
                "initial_noise_detected": bool(
                    case_debug.get("initial_noise_detected")
                    if "initial_noise_detected" in case_debug
                    else readiness.get("initial_noise_detected", False)
                ),
                "human_dialogue_after_noise_detected": bool(
                    case_debug.get("human_dialogue_after_noise_detected")
                    if "human_dialogue_after_noise_detected" in case_debug
                    else readiness.get("human_dialogue_after_noise_detected", False)
                ),
                "noise_classification": str(
                    case_debug.get("noise_classification")
                    or readiness.get("noise_classification")
                    or ""
                ),
                "final_noise_decision": str(
                    case_debug.get("final_noise_decision")
                    or readiness.get("final_noise_decision")
                    or ""
                ),
                "skip_reason": skip_reason,
                "final_decision": str(
                    readiness.get("final_decision")
                    or ("sent_to_writer" if not bool(case_debug.get("skipped")) else _map_readiness_final_decision(skip_reason=skip_reason))
                ).strip(),
            }
        )
    rows_after_llm_steps = max(
        0,
        len(selected_items)
        - sum(
            1
            for x in row_flow_entries
            if str(x.get("skip_reason") or "") in llm_skip_reasons
            or str(x.get("skip_reason") or "") in transcript_skip_reasons
        ),
    )
    row_flow_payload = {
        "deals_selected_for_analysis": deals_selected_for_analysis,
        "selected_anchor_cases_total": len(selected_items),
        "rows_after_llm_steps": int(rows_after_llm_steps),
        "rows_after_preflight": len(rows),
        "rows_in_writer_rows": len(rows),
        "rows_in_sheet_payload": len(rows),
        "rows_skipped_before_writer": max(0, len(selected_items) - len(rows)),
        "skip_reasons": dict(skipped_reasons),
        "rows": row_flow_entries,
    }
    row_flow_path.write_text(
        json.dumps(row_flow_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    summary_path.write_text(
        json.dumps(
            {
                "ledger_total_all": len(ledger_all),
                "anchor_candidates_total": len(shortlist_items),
                "selected_anchor_cases_total": len(selected_items),
                "selected_cases_by_manager": selected_cases_by_manager,
                "selected_by_manager": selected_cases_by_manager,
                "writer_rows_by_manager": writer_rows_by_manager,
                "skipped_after_llm_by_manager": skipped_after_llm_by_manager,
                "dropped_by_limit": max(0, len(shortlist_items) - len(selected_items)),
                "skipped_after_stt": skipped_after_stt,
                "skipped_after_llm": skipped_after_llm,
                "autoanswer_calls_detected": int(autoanswer_stats.get("autoanswer_calls_detected", 0) or 0),
                "autoanswer_deals_skipped": int(autoanswer_stats.get("autoanswer_deals_skipped", 0) or 0),
                "autoanswer_by_manager": (
                    autoanswer_stats.get("autoanswer_by_manager")
                    if isinstance(autoanswer_stats.get("autoanswer_by_manager"), dict)
                    else {}
                ),
                "rows_written": len(rows),
                "score_distribution": {
                    "min": min(scores) if scores else None,
                    "max": max(scores) if scores else None,
                    "avg": round(sum(scores) / len(scores), 2) if scores else None,
                    "unique_scores": sorted(set(scores)),
                },
                "score_sources": score_sources,
                "case_type_source_counts": case_type_source_counts,
                "transcript_usability_distribution": transcript_labels,
                "cases_total": len(cases_debug),
                "skipped_reasons": skipped_reasons,
                "abort_stage": str(abort_stage or ""),
                "abort_error": str(abort_error or ""),
                "artifacts_written": [str(x) for x in artifacts_written if str(x).strip()],
                "call_ledger_all_total": len(ledger_all),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return {
        "root": str(root),
        "call_ledger_all_json": str(ledger_path),
        "call_ledger_audit_json": str(ledger_audit_path),
        "anchor_shortlist_json": str(shortlist_path),
        "selected_anchor_cases_json": str(selected_path),
        "anchor_cases_json": str(cases_path),
        "writer_rows_json": str(rows_path),
        "transcript_readiness_debug_json": str(transcript_readiness_path),
        "row_flow_debug_json": str(row_flow_path),
        "pipeline_summary_json": str(summary_path),
        "case_type_audit_json": str(case_type_audit_path),
    }


def _resolve_non_meaningful_skip_reason(*, candidate: dict[str, Any], record: dict[str, Any]) -> str:
    raw_case_type = str(candidate.get("call_case_type") or "").strip().lower()
    if raw_case_type in {"autoanswer_noise", "autoanswer", "answering_machine", "voicemail"}:
        return "autoanswer_no_meaningful_conversation"
    if raw_case_type in {"no_human_dialogue", "no_human_conversation"}:
        return "no_human_dialogue"
    if raw_case_type in {"short_no_answer", "short_no_answer_autoanswer", "short_noise", "short_no_meaningful"}:
        return "short_no_meaningful_conversation"

    autoanswer_like = int(candidate.get("autoanswer_like_count", 0) or 0)
    no_answer_like = int(candidate.get("no_answer_like_count", 0) or 0)
    selected_call_count = int(candidate.get("selected_call_count", 0) or 0)
    if selected_call_count <= 1 and autoanswer_like > 0:
        return "autoanswer_no_meaningful_conversation"
    if selected_call_count <= 1 and no_answer_like > 0:
        return "short_no_meaningful_conversation"
    transcript_summary = " ".join(
        str(record.get(k) or "").strip().lower()
        for k in ("call_signal_summary_short", "transcript_text_excerpt", "transcript_text")
    )
    transcript_len = int(record.get("transcript_text_len", 0) or len(transcript_summary))
    duration = int(
        candidate.get("anchor_call_duration_seconds", 0)
        or record.get("call_review_anchor_call_duration_seconds", 0)
        or record.get("anchor_call_duration_seconds", 0)
        or 0
    )
    noise_profile = _noise_dialogue_profile(
        transcript_text=transcript_summary,
        transcript_chars=transcript_len,
        call_duration_seconds=duration,
    )
    if str(noise_profile.get("noise_classification") or "") == "ivr_then_human_dialogue":
        return ""
    if any(
        token in transcript_summary
        for token in (
            "автоответ",
            "автоответчик",
            "оставьте сообщение",
            "после сигнала",
            "голосовая почта",
            "абонент недоступен",
            "соединение установлено",
            "voicemail",
        )
    ):
        if any(token in transcript_summary for token in DIALOGUE_TRANSCRIPT_MARKERS):
            return ""
        return "autoanswer_no_meaningful_conversation"
    if any(token in transcript_summary for token in IVR_TRANSCRIPT_MARKERS):
        if any(token in transcript_summary for token in DIALOGUE_TRANSCRIPT_MARKERS):
            return ""
        return "no_human_dialogue"
    normalized = " ".join(transcript_summary.split()).strip()
    if normalized in {"алло", "алло алло", "алло?", "да", "слушаю"}:
        return "no_human_dialogue"
    return ""


def _resolve_llm_case_unready_reason(*, record: dict[str, Any], candidate: dict[str, Any] | None = None) -> str:
    raw = str(record.get("call_review_llm_error") or "").strip().lower()
    category = str(record.get("call_review_llm_error_category") or "").strip().lower()
    candidate_payload = candidate if isinstance(candidate, dict) else {}
    non_meaningful_reason = _resolve_non_meaningful_skip_reason(candidate=candidate_payload, record=record)
    if raw in {"autoanswer_no_meaningful_conversation", "short_no_meaningful_conversation", "no_human_dialogue"}:
        return raw
    if raw in {"not_meaningful_conversation_case", "discipline_case_disabled", "skip_no_meaningful_case"}:
        if non_meaningful_reason:
            return non_meaningful_reason
        return "no_human_dialogue"
    if category == "transcript_not_ready":
        if raw in {"empty_transcript_after_stt", "transcript_not_ready"}:
            return raw
        return "transcript_not_ready"
    if raw in {"empty_transcript_after_stt", "transcript_not_ready"}:
        return raw
    if category == "llm_timeout":
        return "llm_timeout"
    if category in {"validation_failed", "missing_required_stage_comment"}:
        return category
    if raw.startswith("validation_failed"):
        return "validation_failed"
    if raw.startswith("missing_required_stage_comment") or raw.startswith("llm_missing_stage_"):
        return "missing_required_stage_comment"
    if raw.startswith("llm_missing_"):
        return "validation_failed"
    if raw in {
        "no_live_llm_runtime",
        "free_form_generation_failed",
        "effect_layer_generation_failed",
        "structured_generation_failed",
    }:
        return "llm_not_ready"
    if raw.startswith("llm_timeout"):
        return "llm_timeout"
    if "timeout" in raw or "timed out" in raw or "connection" in raw or "remote" in raw:
        return "llm_timeout"
    return "llm_not_ready"


def _resolve_case_mode(*, candidate: dict[str, Any], record: dict[str, Any]) -> str:
    raw = str(record.get("call_review_case_mode") or "").strip().lower()
    if raw:
        return raw
    case_type = str(candidate.get("call_case_type") or "").strip().lower()
    mapping = {
        "lpr_conversation": "negotiation_lpr_analysis",
        "secretary_case": "secretary_analysis",
        "supplier_inbound": "supplier_inbound_analysis",
        "warm_inbound": "warm_inbound_analysis",
        "warm_case": "warm_inbound_analysis",
        "presentation": "presentation_analysis",
        "demo": "presentation_analysis",
        "test": "test_analysis",
        "pilot": "test_analysis",
        "dozhim": "dozhim_analysis",
        "closing": "dozhim_analysis",
        "redial_discipline": "redial_discipline_analysis",
        "autoanswer_noise": "redial_discipline_analysis",
    }
    return mapping.get(case_type, "skip_no_meaningful_case")


def _resolve_case_mode_for_output(
    *,
    case_mode_hint: str,
    llm_fields: dict[str, Any],
    candidate: dict[str, Any],
    record: dict[str, Any],
) -> tuple[str, str, str, list[str]]:
    llm_primary_raw = _text(llm_fields.get("primary_case_type"))
    llm_mode = _map_primary_case_type_to_case_mode(llm_primary_raw)
    evidence: list[str] = []
    if llm_primary_raw:
        evidence.append(f"llm_primary_case_type={llm_primary_raw}")

    if llm_mode:
        return (
            llm_mode,
            "llm_transcript_with_metadata",
            "primary_case_type_from_llm",
            evidence,
        )

    stage_groups = _parse_stage_group_set(str(llm_fields.get("relevant_stage_groups") or ""))
    if stage_groups:
        inferred = _infer_case_mode_from_stage_groups(stage_groups)
        if inferred:
            evidence.append(f"llm_relevant_stage_groups={sorted(stage_groups)}")
            return (
                inferred,
                "llm_transcript",
                "relevant_stage_groups_from_llm",
                evidence,
            )

    inferred_from_comments = _infer_case_mode_from_llm_stage_comments(llm_fields)
    if inferred_from_comments:
        evidence.append("llm_stage_comments_signal")
        return (
            inferred_from_comments,
            "llm_transcript_with_metadata",
            "stage_comments_signal_from_llm",
            evidence,
        )

    # Explicit neutral fallback when LLM did not return valid type.
    fallback_reason = "llm_primary_case_type_missing_or_invalid"
    if case_mode_hint and case_mode_hint not in DISALLOWED_CASES_FOR_ACTIVE_WRITE:
        evidence.append(f"case_mode_hint={case_mode_hint}")
    if isinstance(candidate.get("evidence_used"), list):
        evidence.extend(str(x).strip() for x in candidate.get("evidence_used", []) if str(x).strip())
    if str(record.get("status_name") or "").strip():
        evidence.append(f"status_name={str(record.get('status_name') or '').strip()}")
    return (
        "unknown_call_case",
        "deterministic_fallback",
        fallback_reason,
        evidence[:12],
    )


def _map_primary_case_type_to_case_mode(raw_value: str) -> str:
    text = " ".join(str(raw_value or "").strip().lower().split())
    if not text:
        return ""
    mapping = {
        "разговор с лпр": "negotiation_lpr_analysis",
        "лпр": "negotiation_lpr_analysis",
        "разговор с секретарем": "secretary_analysis",
        "секретарь": "secretary_analysis",
        "входящий от поставщика": "supplier_inbound_analysis",
        "поставщик": "supplier_inbound_analysis",
        "теплый входящий": "warm_inbound_analysis",
        "теплый": "warm_inbound_analysis",
        "презентация": "presentation_analysis",
        "демо": "presentation_analysis",
        "подтверждение презентации": "presentation_analysis",
        "работа с тестом": "test_analysis",
        "тест": "test_analysis",
        "дожим": "dozhim_analysis",
        "кп": "dozhim_analysis",
        "недозвоны / дисциплина дозвонов": "redial_discipline_analysis",
        "недозвоны": "redial_discipline_analysis",
        "дисциплина дозвонов": "redial_discipline_analysis",
        "неопределенный кейс по звонку": "unknown_call_case",
        "unknown": "unknown_call_case",
    }
    if text in mapping:
        return mapping[text]
    if "секретар" in text:
        return "secretary_analysis"
    if "поставщик" in text:
        return "supplier_inbound_analysis"
    if "тепл" in text and "вход" in text:
        return "warm_inbound_analysis"
    if "презентац" in text or "демо" in text:
        return "presentation_analysis"
    if "тест" in text:
        return "test_analysis"
    if "дожим" in text or "кп" in text:
        return "dozhim_analysis"
    if "лпр" in text:
        return "negotiation_lpr_analysis"
    if "недозвон" in text or "дисциплин" in text:
        return "redial_discipline_analysis"
    if "неопредел" in text:
        return "unknown_call_case"
    return ""


def _infer_case_mode_from_stage_groups(stage_groups: set[str]) -> str:
    if "test" in stage_groups:
        return "test_analysis"
    if "demo" in stage_groups or "confirm_demo" in stage_groups:
        return "presentation_analysis"
    if "dozhim" in stage_groups:
        return "dozhim_analysis"
    if "secretary" in stage_groups:
        return "secretary_analysis"
    if "discipline" in stage_groups and len(stage_groups) <= 2:
        return "redial_discipline_analysis"
    if "lpr" in stage_groups or "need" in stage_groups:
        return "negotiation_lpr_analysis"
    return ""


def _infer_case_mode_from_llm_stage_comments(llm_fields: dict[str, Any]) -> str:
    def _has(*keys: str) -> bool:
        for key in keys:
            if _text(llm_fields.get(key)):
                return True
        return False

    if _has(
        "stage_test_launch_comment",
        "stage_test_criteria_comment",
        "stage_test_owners_comment",
        "stage_test_support_comment",
        "stage_test_feedback_comment",
        "stage_test_objections_comment",
        "stage_test_comment",
    ):
        return "test_analysis"
    if _has(
        "stage_demo_intro_comment",
        "stage_demo_context_comment",
        "stage_demo_relevant_comment",
        "stage_demo_process_comment",
        "stage_demo_objections_comment",
        "stage_demo_next_step_comment",
        "stage_demo_comment",
        "stage_confirm_demo_comment",
    ):
        return "presentation_analysis"
    if _has(
        "stage_dozhim_recontact_comment",
        "stage_dozhim_doubts_comment",
        "stage_dozhim_terms_comment",
        "stage_dozhim_decision_comment",
        "stage_dozhim_flow_comment",
        "stage_dozhim_comment",
    ):
        return "dozhim_analysis"
    if _has("stage_secretary_comment") and not _has("stage_lpr_comment", "stage_need_comment"):
        return "secretary_analysis"
    if _has("stage_discipline_comment") and not _has("stage_lpr_comment", "stage_need_comment"):
        return "redial_discipline_analysis"
    if _has("stage_lpr_comment", "stage_need_comment", "stage_presentation_comment", "stage_closing_comment"):
        return "negotiation_lpr_analysis"
    return ""


def _validate_llm_fields(*, case_mode: str, llm_fields: dict[str, Any]) -> str:
    required = (
        "key_takeaway",
        "strong_sides",
        "growth_zones",
        "why_important",
        "reinforce",
        "fix_action",
        "coaching_list",
        "expected_quantity",
        "expected_quality",
    )
    for key in required:
        if not _text(llm_fields.get(key)):
            return f"llm_missing_{key}"
    if case_mode == "secretary_analysis" and not any(
        _text(llm_fields.get(k))
        for k in ("stage_secretary_comment", "stage_need_comment", "stage_lpr_comment")
    ):
        return "llm_missing_stage_secretary_comment"
    if case_mode == "presentation_analysis" and not any(
        _text(llm_fields.get(k))
        for k in (
            "stage_demo_intro_comment",
            "stage_demo_context_comment",
            "stage_demo_relevant_comment",
            "stage_demo_comment",
        )
    ):
        return "llm_missing_stage_presentation_comment"
    if case_mode == "test_analysis" and not any(
        _text(llm_fields.get(k))
        for k in ("stage_test_launch_comment", "stage_test_criteria_comment", "stage_test_comment")
    ):
        return "llm_missing_stage_test_comment"
    if case_mode == "dozhim_analysis" and not any(
        _text(llm_fields.get(k))
        for k in ("stage_dozhim_recontact_comment", "stage_dozhim_terms_comment", "stage_dozhim_comment")
    ):
        return "llm_missing_stage_dozhim_comment"
    if case_mode not in {"presentation_analysis", "test_analysis", "dozhim_analysis", "secretary_analysis"}:
        if not any(
            _text(llm_fields.get(k))
            for k in (
                "stage_lpr_comment",
                "stage_need_comment",
                "stage_presentation_comment",
                "stage_closing_comment",
                "stage_objections_comment",
                "stage_speech_comment",
            )
        ):
            return "llm_missing_conversation_stage_comment"
    return ""


def _extract_calls(*, snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    call_evidence = snapshot.get("call_evidence", {}) if isinstance(snapshot, dict) else {}
    items = call_evidence.get("items", []) if isinstance(call_evidence, dict) and isinstance(call_evidence.get("items"), list) else []
    return [x for x in items if isinstance(x, dict)]


def _build_transcript_text_by_call_id(*, snapshot: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    transcripts = snapshot.get("transcripts", []) if isinstance(snapshot.get("transcripts"), list) else []
    for raw in transcripts:
        if not isinstance(raw, dict):
            continue
        call_id = str(raw.get("call_id") or "").strip()
        if not call_id:
            continue
        text = " ".join(str(raw.get("transcript_text") or "").split()).strip()
        if text:
            out[call_id] = text
    return out


def _load_snapshot(*, record: dict[str, Any]) -> dict[str, Any]:
    artifact_path = Path(str(record.get("artifact_path") or "").strip())
    if not artifact_path.exists():
        return {}
    try:
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload.get("snapshot", {}) if isinstance(payload, dict) and isinstance(payload.get("snapshot"), dict) else {}


def _selected_call_ids(*, candidate: dict[str, Any], record: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for source in (
        candidate.get("selected_call_ids"),
        record.get("selected_call_ids"),
    ):
        if not isinstance(source, list):
            continue
        for item in source:
            call_id = str(item or "").strip()
            if call_id:
                out.add(call_id)
    return out


def _select_anchor_call(
    *,
    calls: list[dict[str, Any]],
    selected_call_ids: set[str],
    transcript_text_by_call_id: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    transcript_map = transcript_text_by_call_id if isinstance(transcript_text_by_call_id, dict) else {}

    def _eligible_selected(call: dict[str, Any]) -> bool:
        if not selected_call_ids:
            return True
        call_id = str(call.get("call_id") or "").strip()
        if not call_id:
            return True
        return call_id in selected_call_ids

    selected_candidates = [call for call in calls if isinstance(call, dict) and _eligible_selected(call)]
    candidate_pool = selected_candidates if selected_candidates else [call for call in calls if isinstance(call, dict)]
    if not candidate_pool:
        return None

    meaningful_selected = [
        call
        for call in selected_candidates
        if not _call_is_noise(call, transcript_text=_transcript_text_for_call(call, transcript_map))
    ]
    if meaningful_selected:
        candidate_pool = meaningful_selected
    else:
        meaningful_all = [
            call
            for call in calls
            if isinstance(call, dict) and not _call_is_noise(call, transcript_text=_transcript_text_for_call(call, transcript_map))
        ]
        if meaningful_all:
            candidate_pool = meaningful_all

    ranked: list[tuple[int, datetime | None, dict[str, Any]]] = []
    for call in candidate_pool:
        ts = _parse_ts(call.get("timestamp"))
        duration = int(call.get("duration_seconds", 0) or 0)
        score = duration
        if bool(str(call.get("recording_url") or "").strip()):
            score += 120
        if not _call_is_noise(call, transcript_text=_transcript_text_for_call(call, transcript_map)):
            score += 80
        if str(call.get("direction") or "").strip().lower() in {"outbound", "inbound"}:
            score += 10
        if ts is not None:
            score += 5
        ranked.append((score, ts, call))
    if not ranked:
        return None
    ranked.sort(
        key=lambda x: (
            -int(x[0]),
            str((x[1] or datetime(1970, 1, 1, tzinfo=timezone.utc)).isoformat()),
            str((x[2] or {}).get("call_id") or ""),
        )
    )
    return ranked[0][2]


def _select_related_calls(
    *,
    calls: list[dict[str, Any]],
    anchor_call: dict[str, Any],
    selected_call_ids: set[str],
    transcript_text_by_call_id: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    transcript_map = transcript_text_by_call_id if isinstance(transcript_text_by_call_id, dict) else {}
    anchor_ts = _parse_ts(anchor_call.get("timestamp"))
    anchor_id = str(anchor_call.get("call_id") or "")
    out: list[dict[str, Any]] = [anchor_call]
    for call in calls:
        if not isinstance(call, dict):
            continue
        call_id = str(call.get("call_id") or "").strip()
        if not call_id or call_id == anchor_id:
            continue
        if selected_call_ids and call_id not in selected_call_ids:
            continue
        ts = _parse_ts(call.get("timestamp"))
        if anchor_ts is not None and ts is not None and ts > anchor_ts:
            continue
        if _call_is_noise(call, transcript_text=_transcript_text_for_call(call, transcript_map)):
            continue
        out.append(call)
    out.sort(
        key=lambda x: (
            str(x.get("timestamp") or ""),
            str(x.get("call_id") or ""),
        )
    )
    return out[:3]


def _transcript_text_for_call(call: dict[str, Any], transcript_text_by_call_id: dict[str, str]) -> str:
    call_id = str(call.get("call_id") or "").strip()
    if call_id:
        return str(transcript_text_by_call_id.get(call_id) or "").strip()
    return ""


def _call_is_noise(call: dict[str, Any], *, transcript_text: str = "") -> bool:
    duration = int(call.get("duration_seconds", 0) or 0)
    if duration <= 20:
        return True
    blob = " ".join(
        _text(call.get(k)).lower()
        for k in ("status", "result", "disposition", "quality_flags")
    )
    noise_tokens = (
        "no_answer",
        "not_answered",
        "busy",
        "voicemail",
        "autoanswer",
        "автоответ",
        "недозвон",
        "занято",
    )
    status_noise_flag = any(token in blob for token in noise_tokens)

    transcript = " ".join(str(transcript_text or "").split()).strip().lower()
    if not transcript:
        return bool(status_noise_flag)
    noise_profile = _noise_dialogue_profile(
        transcript_text=transcript,
        transcript_chars=len(transcript),
        call_duration_seconds=duration,
    )
    noise_classification = str(noise_profile.get("noise_classification") or "")
    has_dialogue_markers = bool(noise_profile.get("has_dialogue_markers"))
    if noise_classification == "ivr_then_human_dialogue":
        return False
    if status_noise_flag and not has_dialogue_markers:
        return True
    if noise_classification == "noise_only_autoanswer_or_ivr":
        return True
    if noise_classification == "initial_ivr_noise" and len(transcript) < 350 and duration <= 120:
        return True
    if transcript in {"алло", "алло алло", "алло?", "да", "слушаю"}:
        return True
    if transcript.startswith("алло") and len(transcript) < 60 and not has_dialogue_markers:
        return True
    return False


def _choose_analysis_date(*, candidate: dict[str, Any], anchor_ts: datetime | None) -> str:
    by_shortlist = _text(candidate.get("business_window_date"))
    if by_shortlist:
        return by_shortlist
    if anchor_ts is not None:
        return _business_bucket_date(anchor_ts).isoformat()
    return ""


def _choose_case_date(*, anchor_ts: datetime | None) -> str:
    if anchor_ts is None:
        return ""
    return anchor_ts.astimezone(MSK_TZ).date().isoformat()


def _business_bucket_date(ts_utc: datetime) -> date:
    local = ts_utc.astimezone(MSK_TZ)
    # Weekend calls belong to Monday business bucket.
    if local.weekday() >= 5:
        cur = local.date()
        while cur.weekday() >= 5:
            cur = cur + timedelta(days=1)
        return cur
    cutoff = datetime.combine(local.date(), time(15, 0), tzinfo=MSK_TZ)
    if local < cutoff:
        return local.date()
    cur = local.date() + timedelta(days=1)
    while cur.weekday() >= 5:
        cur = cur + timedelta(days=1)
    return cur


def _resolve_manager_role(*, manager_name: str, registry: dict[str, str]) -> str:
    low = manager_name.strip().lower()
    for key, role in registry.items():
        token = str(key or "").strip().lower()
        if token and (token in low or low in token):
            role_low = str(role or "").strip().lower()
            if role_low in {"telemarketer", "sales_manager"}:
                return role_low
    if "рустам" in low:
        return "telemarketer"
    if "илья" in low:
        return "sales_manager"
    return "sales_manager"


def _resolve_product_focus(*, record: dict[str, Any]) -> str:
    hyp = _text(record.get("product_hypothesis")).lower()
    if hyp in {"info", "link", "mixed", "unknown"}:
        return {
            "info": "инфо",
            "link": "линк",
            "mixed": "оба",
            "unknown": "до продукта разговор не дошел",
        }[hyp]
    name = _text(record.get("product_name")).lower()
    if "линк" in name or "link" in name:
        return "линк"
    if "инфо" in name or "info" in name:
        return "инфо"
    return "до продукта разговор не дошел"


def _format_listened_calls(calls: list[dict[str, Any]]) -> str:
    chunks: list[str] = []
    for call in calls:
        ts = _parse_ts(call.get("timestamp"))
        duration = int(call.get("duration_seconds", 0) or 0)
        if ts is None:
            continue
        local = ts.astimezone(MSK_TZ)
        stamp = local.strftime("%Y-%m-%d %H:%M")
        chunks.append(f"{stamp} - {_format_duration(duration)}")
    return "; ".join(chunks[:8]).strip()


def _format_duration(seconds: int) -> str:
    sec = max(0, int(seconds))
    mm, ss = divmod(sec, 60)
    hh, mm = divmod(mm, 60)
    if hh > 0:
        return f"{hh:02d}:{mm:02d}:{ss:02d}"
    return f"{mm:02d}:{ss:02d}"


def _deal_url(*, base_domain: str, deal_id: str) -> str:
    did = _text(deal_id)
    if not did:
        return ""
    domain = _text(base_domain).rstrip("/")
    if domain:
        return f"{domain}/leads/detail/{did}"
    return did


def _criticality_from_score(score: Any) -> str:
    try:
        value = int(float(score))
    except Exception:
        return ""
    if value < 40:
        return "высокая"
    if value < 70:
        return "средняя"
    return "низкая"


def _normalize_coaching(value: str) -> str:
    text = _sanitize_user_text(value)
    if not text:
        return ""
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if lines and all(re.match(r"^\d+\)", line) for line in lines):
        return "\n".join(lines[:3])
    parts = [x.strip() for x in re.split(r"[;\n]+", text) if x.strip()]
    if not parts:
        return ""
    numbered = [f"{idx + 1}) {part}" for idx, part in enumerate(parts[:3])]
    return "\n".join(numbered)


def _normalize_expected_quantity(value: str) -> str:
    text = _sanitize_user_text(value)
    text = text.replace("%", "")
    if not text:
        return ""
    if re.search(r"\b\+?0(\.0+)?\b", text):
        text = re.sub(r"\b\+?0(\.0+)?\b", "0.2", text)
    return text[:220]


def _sanitize_user_facing_row(row: dict[str, Any]) -> tuple[dict[str, Any], dict[str, int]]:
    replacement_audit: dict[str, int] = {}
    text_fields = set(NARRATIVE_USER_FACING_COLUMNS) | {"Эффект количество / неделя"}
    for key in text_fields:
        value = row.get(key)
        if not isinstance(value, str):
            continue
        normalized = _sanitize_user_text(
            value,
            replacement_audit=replacement_audit,
        )
        row[key] = normalized
    return row, replacement_audit


def _apply_style_literal_replacement(
    *,
    text: str,
    src: str,
    dst: str,
    audit_key: str,
    replacement_audit: dict[str, int] | None,
) -> str:
    hits = text.count(src)
    if hits <= 0:
        return text
    if replacement_audit is not None:
        replacement_audit[audit_key] = int(replacement_audit.get(audit_key, 0) or 0) + hits
    return text.replace(src, dst)


def _apply_style_regex_replacement(
    *,
    text: str,
    pattern: str,
    repl: str,
    audit_key: str,
    replacement_audit: dict[str, int] | None,
    flags: int = re.IGNORECASE,
) -> str:
    out, count = re.subn(pattern, repl, text, flags=flags)
    if count > 0 and replacement_audit is not None:
        replacement_audit[audit_key] = int(replacement_audit.get(audit_key, 0) or 0) + int(count)
    return out


def _sanitize_user_text(
    value: Any,
    *,
    replacement_audit: dict[str, int] | None = None,
) -> str:
    text = _text(value)
    if not text:
        return ""
    punctuation_replacements = (
        ("quotes_open", "«", '"'),
        ("quotes_close", "»", '"'),
        ("dash_long", "—", "-"),
        ("dash_mid", "–", "-"),
        ("arrow", "→", " - "),
        ("ellipsis", "…", "..."),
    )
    for audit_key, src, dst in punctuation_replacements:
        text = _apply_style_literal_replacement(
            text=text,
            src=src,
            dst=dst,
            audit_key=audit_key,
            replacement_audit=replacement_audit,
        )
    text = _apply_style_regex_replacement(
        text=text,
        pattern=r'""\s*([^"]*?)\s*""',
        repl=r'"\1"',
        audit_key="double_quote_wrap",
        replacement_audit=replacement_audit,
        flags=0,
    )

    for audit_key, src, dst in STYLE_LITERAL_REPLACEMENTS:
        text = _apply_style_literal_replacement(
            text=text,
            src=src,
            dst=dst,
            audit_key=audit_key,
            replacement_audit=replacement_audit,
        )

    for audit_key, pattern, repl in STYLE_REGEX_REPLACEMENTS:
        text = _apply_style_regex_replacement(
            text=text,
            pattern=pattern,
            repl=repl,
            audit_key=audit_key,
            replacement_audit=replacement_audit,
        )

    # Product exhibition tags are allowed in "База / тег", but must not leak into narrative comments.
    text = _apply_style_regex_replacement(
        text=text,
        pattern=r"\b(tilda|инглегмаш(?:-\d{2,4})?|уралстрой(?:-\d{2,4})?)\b",
        repl="продукт",
        audit_key="tag_name_to_product",
        replacement_audit=replacement_audit,
    )

    # Remove leftover technical words if still present after substitutions.
    for forbidden in ("LLM", "llm", "transcript", "call signal", "pipeline signal"):
        text = _apply_style_literal_replacement(
            text=text,
            src=forbidden,
            dst="",
            audit_key=f"remove_{forbidden.lower().replace(' ', '_')}",
            replacement_audit=replacement_audit,
        )

    text = _apply_style_regex_replacement(
        text=text,
        pattern=r"\bне\s+актуально\b",
        repl="",
        audit_key="remove_not_relevant_phrase",
        replacement_audit=replacement_audit,
    )
    text = _apply_style_regex_replacement(
        text=text,
        pattern=r'(?:Лучше сказать|Используй):\s*"([^"]*)$',
        repl=r'Используй: "\1"',
        audit_key="close_better_phrase_quote",
        replacement_audit=replacement_audit,
        flags=re.IGNORECASE,
    )
    text = _apply_style_regex_replacement(
        text=text,
        pattern=r"(?:\bЛучше\b|\bИспользуй\b)\s*$",
        repl="",
        audit_key="remove_trailing_luchshe_word",
        replacement_audit=replacement_audit,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r'"+', '"', text)
    if text.count('"') % 2 == 1:
        # Trim only the unmatched quote to keep evidence quotes readable.
        if text.endswith('"'):
            text = text[:-1].rstrip()
        elif text.startswith('"'):
            text = text[1:].lstrip()
        else:
            pos = text.rfind('"')
            if pos >= 0:
                text = (text[:pos] + text[pos + 1 :]).strip()
    text = re.sub(r"\s*-\s*-\s*", " - ", text)
    return text[:900]


def _ensure_better_phrase(text: str) -> str:
    value = " ".join(str(text or "").split()).strip()
    return value[:900]


def _merge_comment_with_quote(
    *,
    comment: str,
    quote: str,
    stage_group: str,
    quote_usage: dict[str, int],
) -> tuple[str, str]:
    c = _sanitize_user_text(comment)
    q = _sanitize_user_text(quote)
    if not c:
        return "", ""
    c = re.sub(r"\b[Цц]итата:\s*", "", c).strip()
    c = re.sub(r'"[^"]{3,260}"', "", c).strip()
    c = re.sub(r"\s{2,}", " ", c).strip(" ,;:-")
    if re.search(r"\bне\s+актуально\b", c, flags=re.IGNORECASE):
        return "", ""

    chosen_quote = ""
    if q:
        quote_key = _quote_key(q)
        if quote_key:
            usage = int(quote_usage.get(quote_key, 0) or 0)
            if usage < 2:
                quote_usage[quote_key] = usage + 1
                chosen_quote = q[:220]
                c = f'"{chosen_quote}" - {c}'
    c = _ensure_stage_specific_better_phrase(text=c, stage_group=stage_group)
    return c[:900], chosen_quote


def _ensure_stage_specific_better_phrase(*, text: str, stage_group: str) -> str:
    value = " ".join(str(text or "").split()).strip()
    if not value:
        return ""
    low = value.lower()
    has_critique = any(token in low for token in ("не ", "слаб", "провис", "пропуст", "не дожал", "без", "рано"))
    if not has_critique:
        return value
    if "лучше сказать:" in low:
        return value
    phrase = _pick_better_phrase_variant(
        stage_group=stage_group,
        original_phrase="",
        seed=value,
        taken={},
    )
    if not phrase:
        return value
    return f'{value} Лучше сказать: "{phrase}"'.strip()


def _pick_better_phrase_variant(
    *,
    stage_group: str,
    original_phrase: str,
    seed: str,
    taken: dict[str, int],
) -> str:
    group = stage_group if stage_group in BETTER_PHRASE_VARIANTS else "generic"
    variants = list(BETTER_PHRASE_VARIANTS.get(group, BETTER_PHRASE_VARIANTS["generic"]))
    if original_phrase:
        variants.append(original_phrase)
    if not variants:
        return original_phrase
    hashed = hashlib.md5(f"{group}|{seed}".encode("utf-8")).hexdigest()
    idx = int(hashed[:8], 16) % len(variants)
    ordered = variants[idx:] + variants[:idx]
    for phrase in ordered:
        clean = _sanitize_user_text(phrase)
        if not clean:
            continue
        if int(taken.get(clean.lower(), 0) or 0) < 2:
            return clean
    return _sanitize_user_text(ordered[0]) if ordered else _sanitize_user_text(original_phrase)


def _pick_stage_quote(
    *,
    stage_group: str,
    raw_comment: str,
    llm_fields: dict[str, Any],
    global_quote: str,
) -> str:
    evidence_map = _parse_stage_evidence_map(str(llm_fields.get("evidence_by_stage") or ""))
    from_stage_map = _sanitize_user_text(str(evidence_map.get(stage_group) or ""))
    if from_stage_map:
        quotes = _extract_quotes(from_stage_map)
        if quotes:
            return quotes[0]
        return from_stage_map[:220]
    comment_quotes = _extract_quotes(raw_comment)
    if comment_quotes:
        return comment_quotes[0]
    return _sanitize_user_text(global_quote)[:220] if global_quote else ""


def _extract_quotes(text: str) -> list[str]:
    clean = _sanitize_user_text(text)
    if not clean:
        return []
    out: list[str] = []
    for match in re.findall(r'"([^"]{6,240})"', clean):
        val = _sanitize_user_text(match)
        if val and val not in out:
            out.append(val)
    return out


def _quote_key(text: str) -> str:
    value = _sanitize_user_text(text).lower()
    value = re.sub(r"[^a-zа-я0-9]+", " ", value, flags=re.IGNORECASE).strip()
    return value[:140]


def _comment_has_stage_evidence(text: str) -> bool:
    clean = _sanitize_user_text(text)
    if not clean:
        return False
    if _extract_quotes(clean):
        return True
    low = clean.lower()
    return any(token in low for token in ("клиент", "лпр", "секретар", "сказал", "ответил", "возраж", "шаг"))


def _collect_quote_usage_for_row(*, row: dict[str, Any]) -> dict[str, int]:
    usage: dict[str, int] = {}
    for meta in STAGE_GROUPS.values():
        comment = str(row.get(meta["comment_col"]) or "")
        for quote in _extract_quotes(comment):
            key = _quote_key(quote)
            if not key:
                continue
            usage[key] = int(usage.get(key, 0) or 0) + 1
    return usage


def _join_non_empty(*parts: Any) -> str:
    out = [str(x).strip() for x in parts if str(x).strip()]
    return "; ".join(out)


def _normalize_noise_text(value: str) -> str:
    return " ".join(str(value or "").split()).strip().lower()


def _noise_dialogue_profile(
    *,
    transcript_text: str,
    transcript_chars: int,
    call_duration_seconds: int,
) -> dict[str, Any]:
    text = _normalize_noise_text(transcript_text)
    text_chars = max(int(transcript_chars or 0), len(text))
    head = text[:900]
    autoanswer_markers = tuple(AUTOANSWER_TRANSCRIPT_MARKERS)
    ivr_markers = tuple(IVR_TRANSCRIPT_MARKERS)
    dialogue_markers = tuple(dict.fromkeys((*DIALOGUE_TRANSCRIPT_MARKERS, *HUMAN_DIALOGUE_AFTER_NOISE_MARKERS)))
    has_autoanswer_markers = any(token in text for token in autoanswer_markers)
    has_ivr_markers = any(token in text for token in ivr_markers)
    has_dialogue_markers = any(token in text for token in dialogue_markers)
    initial_noise_detected = any(token in head for token in autoanswer_markers) or any(token in head for token in ivr_markers)
    human_dialogue_after_noise_detected = False
    if initial_noise_detected and text:
        noise_tail_start = 0
        for token in (*autoanswer_markers, *ivr_markers):
            pos = head.find(token)
            if pos >= 0:
                noise_tail_start = max(noise_tail_start, pos + len(token))
        tail = text[noise_tail_start:] if noise_tail_start > 0 else text
        human_dialogue_after_noise_detected = any(token in tail for token in dialogue_markers)
    if not human_dialogue_after_noise_detected:
        human_dialogue_after_noise_detected = bool(
            has_dialogue_markers and initial_noise_detected and (text_chars >= 1000 or int(call_duration_seconds or 0) >= 120)
        )

    if initial_noise_detected and human_dialogue_after_noise_detected:
        noise_classification = "ivr_then_human_dialogue"
    elif (has_autoanswer_markers or has_ivr_markers) and not has_dialogue_markers:
        noise_classification = "noise_only_autoanswer_or_ivr"
    elif initial_noise_detected:
        noise_classification = "initial_ivr_noise"
    elif has_dialogue_markers:
        noise_classification = "human_dialogue"
    else:
        noise_classification = "no_noise_markers"

    final_noise_decision = "allow"
    if noise_classification == "noise_only_autoanswer_or_ivr":
        final_noise_decision = "skip_noise_only"
    elif noise_classification == "initial_ivr_noise" and text_chars < 400 and int(call_duration_seconds or 0) <= 120:
        final_noise_decision = "skip_noise_only"
    elif noise_classification == "ivr_then_human_dialogue":
        final_noise_decision = "allow_after_ivr_human_dialogue"

    return {
        "has_autoanswer_markers": bool(has_autoanswer_markers),
        "has_ivr_markers": bool(has_ivr_markers),
        "has_dialogue_markers": bool(has_dialogue_markers),
        "initial_noise_detected": bool(initial_noise_detected),
        "human_dialogue_after_noise_detected": bool(human_dialogue_after_noise_detected),
        "noise_classification": str(noise_classification),
        "final_noise_decision": str(final_noise_decision),
    }


def _parse_ts(value: Any) -> datetime | None:
    text = _text(value)
    if not text:
        return None
    if text.isdigit():
        try:
            return datetime.fromtimestamp(float(text), tz=timezone.utc)
        except Exception:
            return None
    raw = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        dt = datetime.fromisoformat(raw)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalize_phone_last7(value: str) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[-7:] if len(digits) >= 7 else digits


def _is_manager_allowed(*, manager_name: str, allowlist: list[str]) -> bool:
    if not allowlist:
        return True
    low = manager_name.strip().lower()
    if not low:
        return False
    for item in allowlist:
        token = str(item or "").strip().lower()
        if token and (token in low or low in token):
            return True
    return False


def _sanitize_person_name(value: str) -> str:
    text = _text(value)
    return re.sub(r"\s+", " ", text).strip()


def _bump(counter: dict[str, int], key: str) -> None:
    counter[key] = int(counter.get(key, 0) or 0) + 1


def _text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()
