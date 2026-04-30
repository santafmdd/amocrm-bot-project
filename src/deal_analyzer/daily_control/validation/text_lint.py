from __future__ import annotations

import re
from typing import Any


DAILY_NARRATIVE_FIELDS: tuple[str, ...] = (
    "main_pattern",
    "strong_sides",
    "growth_zones",
    "why_it_matters",
    "what_to_reinforce",
    "what_to_fix",
    "what_to_tell_employee",
    "expected_quant_impact",
    "expected_qual_impact",
    "evidence_short",
    "data_limitations",
)

BAD_GRAMMAR_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"\bотработать\s+быстрого\b", "отработать_быстрого"),
    (r"\bотработать\s+глубиннного\b", "отработать_глубиннного"),
    (r"\bразобрать\s+выявления\b", "разобрать_выявления"),
    (r"\bразобрать\s+быстрого\b", "разобрать_быстрого"),
    (r"\bкак\s+отрабатывать\b", "как_отрабатывать"),
    (r"\bpre-call\s+проверка\s+отработать\b", "precall_bad_phrase"),
    (r"\bфиксация\s+фактов\s+по\s+звонку\s+в\s+црм\s+по\s+звонку\b", "double_po_zvonku"),
    (r"\bцрм\s+по\s+звонку\b.*\bпо\s+звонку\b", "crm_double_context"),
    (r"\bформулировка\s+в\s+ежедневку\b", "formulirovka_v_ezhednevku"),
    (r"\bисправление\s+отработать\s+даст\b", "ispravlenie_otrabotat_dast"),
)

FOREIGN_GREETING_RE = re.compile(r"(你好|您好|hello\b|hi\b|greetings)", re.IGNORECASE)
MARKDOWN_FENCE_RE = re.compile(r"```")
LATIN_WORD_RE = re.compile(r"\b[a-z][a-z0-9_-]{1,}\b", re.IGNORECASE)
CJK_RE = re.compile(r"[\u3400-\u9FFF]")
EMPTY_QUOTES_RE = re.compile(r"(''|\"\")")

ALLOWED_LATIN = {
    "link",
    "info",
    "plm",
    "crm",
    "amocrm",
    "id",
    "url",
    "http",
    "https",
    "api",
    "json",
    "llm",
    "stt",
    "roks",
    "oap",
}
TECHNICAL_LATIN_WARNING = {"api", "json", "llm", "stt"}


def _clean(value: Any) -> str:
    return " ".join(str(value or "").replace("\n", " ").replace("\r", " ").split()).strip()


def _norm(value: Any) -> str:
    return " ".join(
        re.sub(r"[^0-9a-zа-яё/ ]+", " ", str(value or "").lower().replace("ё", "е")).split()
    )


def _latin_terms(text: str) -> list[str]:
    return [str(item or "").lower() for item in LATIN_WORD_RE.findall(str(text or ""))]


def _split_latin_terms(text: str) -> tuple[list[str], list[str], list[str]]:
    allowed: list[str] = []
    technical_warning: list[str] = []
    foreign: list[str] = []
    for term in _latin_terms(text):
        if term in ALLOWED_LATIN:
            allowed.append(term)
            if term in TECHNICAL_LATIN_WARNING:
                technical_warning.append(term)
            continue
        foreign.append(term)
    return allowed, technical_warning, foreign


def _foreign_language_is_blocker(text: str, foreign_terms: list[str]) -> bool:
    if not foreign_terms:
        return False
    latin_chars = sum(sum(1 for ch in token if "a" <= ch <= "z") for token in foreign_terms)
    ratio = float(latin_chars) / max(1.0, float(len(str(text or ""))))
    if len(foreign_terms) >= 4:
        return True
    if latin_chars >= 24:
        return True
    if len(foreign_terms) >= 2 and ratio >= 0.18:
        return True
    return False


def _is_generic_crm_advice(text: str) -> bool:
    norm = _norm(text)
    if "црм" not in norm:
        return False
    context_tokens = ("договор", "дата", "следующ", "карточк", "факт", "разговор")
    return not any(token in norm for token in context_tokens)


def _is_truncated(text: str) -> bool:
    trimmed = _clean(text)
    if not trimmed:
        return False
    low = trimmed.lower()
    if low.endswith((" и", " к", " чтобы", ":", "-", "лучше")):
        return True
    if trimmed.count('"') % 2 != 0:
        return True
    return False


def lint_daily_text_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counters: dict[str, int] = {
        "foreign_language_count": 0,
        "foreign_language_warning_count": 0,
        "foreign_greeting_count": 0,
        "chinese_text_count": 0,
        "markdown_fence_count": 0,
        "allowed_latin_terms_count": 0,
        "technical_terms_warning_count": 0,
        "bad_grammar_marker_count": 0,
        "generic_crm_advice_count": 0,
        "empty_quote_count": 0,
        "truncated_text_count": 0,
        "checked_fields_count": 0,
    }
    forbidden_markers: set[str] = set()
    examples: list[dict[str, Any]] = []

    for row_index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        manager = str(row.get("manager_name") or "")
        deal_ids = str(row.get("deal_ids") or row.get("deal_id") or "")
        for field in DAILY_NARRATIVE_FIELDS:
            if field not in row:
                continue
            text = _clean(row.get(field, ""))
            if not text:
                continue

            counters["checked_fields_count"] += 1
            markers: list[str] = []

            if FOREIGN_GREETING_RE.search(text):
                counters["foreign_greeting_count"] += 1
                markers.append("foreign_greeting")
            if CJK_RE.search(text):
                counters["chinese_text_count"] += 1
                markers.append("chinese_text")
            if MARKDOWN_FENCE_RE.search(text):
                counters["markdown_fence_count"] += 1
                markers.append("markdown_fence")

            allowed_terms, technical_terms, foreign_terms = _split_latin_terms(text)
            if allowed_terms:
                counters["allowed_latin_terms_count"] += len(allowed_terms)
            if technical_terms:
                counters["technical_terms_warning_count"] += len(technical_terms)
                markers.append("technical_term_in_user_text")
            if foreign_terms:
                if _foreign_language_is_blocker(text, foreign_terms):
                    counters["foreign_language_count"] += 1
                    markers.append("foreign_language")
                else:
                    counters["foreign_language_warning_count"] += 1
                    markers.append("foreign_language_warning")

            for pattern, marker in BAD_GRAMMAR_PATTERNS:
                if re.search(pattern, text, flags=re.IGNORECASE):
                    counters["bad_grammar_marker_count"] += 1
                    forbidden_markers.add(marker)
                    markers.append(marker)

            if EMPTY_QUOTES_RE.search(text):
                counters["empty_quote_count"] += 1
                markers.append("empty_quotes")
            if _is_generic_crm_advice(text):
                counters["generic_crm_advice_count"] += 1
                markers.append("generic_crm_advice")
            if _is_truncated(text):
                counters["truncated_text_count"] += 1
                markers.append("truncated_text")

            if markers and len(examples) < 25:
                examples.append(
                    {
                        "row_index": row_index,
                        "manager_name": manager,
                        "deal_ids": deal_ids,
                        "field": field,
                        "markers": markers,
                        "value": text[:300],
                    }
                )

    result = {
        **counters,
        "forbidden_markers": sorted(forbidden_markers),
        "problem_examples": examples,
        "warnings": {
            "bad_grammar_marker_count": counters["bad_grammar_marker_count"],
            "generic_crm_advice_count": counters["generic_crm_advice_count"],
            "empty_quote_count": counters["empty_quote_count"],
            "truncated_text_count": counters["truncated_text_count"],
            "foreign_language_warning_count": counters["foreign_language_warning_count"],
            "allowed_latin_terms_count": counters["allowed_latin_terms_count"],
            "technical_terms_warning_count": counters["technical_terms_warning_count"],
            "forbidden_markers": sorted(forbidden_markers),
        },
        "blockers": {
            "foreign_greeting_count": counters["foreign_greeting_count"],
            "foreign_language_count": counters["foreign_language_count"],
            "chinese_text_count": counters["chinese_text_count"],
            "markdown_fence_count": counters["markdown_fence_count"],
        },
    }

    result.update(
        {
            "daily_text_foreign_language_count": counters["foreign_language_count"],
            "daily_text_foreign_language_warning_count": counters["foreign_language_warning_count"],
            "daily_text_foreign_greeting_count": counters["foreign_greeting_count"],
            "daily_text_bad_grammar_marker_count": counters["bad_grammar_marker_count"],
            "daily_text_generic_crm_advice_count": counters["generic_crm_advice_count"],
            "daily_text_empty_quote_count": counters["empty_quote_count"],
            "daily_text_truncated_text_count": counters["truncated_text_count"],
            "daily_text_checked_fields_count": counters["checked_fields_count"],
            "daily_text_allowed_latin_terms_count": counters["allowed_latin_terms_count"],
            "daily_text_technical_terms_warning_count": counters["technical_terms_warning_count"],
            "daily_text_forbidden_markers": sorted(forbidden_markers),
            "daily_text_problem_examples": examples,
        }
    )
    return result


def lint_has_blockers(lint: dict[str, Any]) -> bool:
    return (
        int(lint.get("foreign_greeting_count", 0) or 0) > 0
        or int(lint.get("foreign_language_count", 0) or 0) > 0
        or int(lint.get("chinese_text_count", 0) or 0) > 0
        or int(lint.get("markdown_fence_count", 0) or 0) > 0
    )
