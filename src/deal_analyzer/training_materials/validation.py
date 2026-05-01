
from __future__ import annotations

import hashlib
import re
from typing import Any

from ..daily_control.source_reader import clean_text


URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
HEADING_RE = re.compile(r"^\s*#{1,3}\s+", re.MULTILINE)
EMPTY_QUOTES_RE = re.compile(r'(""\s*$|\'\'\s*$)', re.MULTILINE)

TRAINING_DOC_MIN_CHARS = 7000
POST_TASK_DOC_MIN_CHARS = 2500
TRAINING_MIN_SECTIONS = 8
TRAINING_MIN_SPEECH_MODULES = 10
TRAINING_MIN_CHECKLIST_ITEMS = 7

FORBIDDEN_PHRASES = (
    "лучше сказать:",
    "crm stage mismatch",
    "этап в crm не соответствует",
    "crm не отражает реальность звонка",
)

ALLOWED_LATIN_TERMS = {
    "link", "info", "plm", "crm", "amocrm", "lpr", "kpi", "istock", "roks",
    "demo", "email", "mail", "tilda", "call", "lead", "pipeline", "funnel",
    "budget", "launch", "date", "smart", "sales", "script", "pitch", "icp",
    "qualification", "follow-up", "follow", "up", "discovery", "cold", "value",
    "proposition", "objection", "handling", "spin", "bant", "meddic", "challenger",
    "sandler", "root", "cause",
}

ALLOWED_LATIN_PHRASES = (
    "launch date",
    "value proposition",
    "discovery call",
    "cold call",
    "follow up",
    "follow-up",
    "objection handling",
    "challenger sale",
)

SOURCES_SECTION_RE = re.compile(r"использованные\s+источники", re.IGNORECASE)
CJK_RE = re.compile(r"[\u4E00-\u9FFF\u3400-\u4DBF\u3040-\u30FF\uAC00-\uD7AF]")

_NUMBERED_LINE_RE = re.compile(r"^\d+[\.)]\s+")
_INLINE_NUMBERED_RE = re.compile(r"(?:^|[\s;])\d+[\.)]\s+")
_SPEECH_PREFIX_RE = re.compile(
    r"^(?:[-*•]\s*)?(?:\d+[\.)]\s*)?(?:\*\*)?(используй|скажи|формулировка|фраза|модуль|можно сказать)(?:\*\*)?\s*:\s*(.+)$",
    re.IGNORECASE,
)
_SPEECH_INSTEAD_RE = re.compile(r"^(?:[-*•]\s*)?(?:\d+[\.)]\s*)?вместо\s+.+\s+используй\s+.+$", re.IGNORECASE)
_SPEECH_QUOTED_RE = re.compile(r'^(?:[-*•]\s*)?(?:\d+[\.)]\s*)?["«“][^"»”]{6,}["»”]\s*$')
_SPEECH_INLINE_PREFIX_RE = re.compile(
    r"(?:используй|скажи|формулировка|фраза|модуль|можно сказать)\s*:\s*[\"«“']?.{4,}?(?=(?:\s+(?:используй|скажи|формулировка|фраза|модуль|можно сказать)\s*:)|$)",
    re.IGNORECASE,
)
_CHECKBOX_BULLET_RE = re.compile(r"^[-*•]\s*\[[ xX]\]\s+")
_HEADING_LINE_RE = re.compile(r"^\s*#{1,4}\s+")


def is_valid_url(value: Any) -> bool:
    return bool(re.match(r"^https?://", clean_text(value), flags=re.IGNORECASE))


def is_valid_url_or_empty(value: Any) -> bool:
    text = clean_text(value)
    return (not text) or is_valid_url(text)


def build_topic_hash(value: str) -> str:
    norm = clean_text(value).lower()
    if not norm:
        return ""
    return hashlib.sha1(norm.encode("utf-8")).hexdigest()[:12]


def is_training_activity(value: str) -> bool:
    probe = clean_text(value).lower()
    if not probe:
        return False
    if probe in {"обучение", "коучинг"}:
        return True
    return "обуч" in probe


def normalize_quotes(text: str) -> str:
    out = str(text or "")
    replacements = {
        "«": '"',
        "»": '"',
        "“": '"',
        "”": '"',
        "„": '"',
        "‟": '"',
        "‘": '"',
        "’": '"',
    }
    for src, dst in replacements.items():
        out = out.replace(src, dst)
    return out


def strip_markdown_fences(text: str) -> str:
    out = str(text or "")
    out = re.sub(r"```[\w-]*", "", out)
    out = out.replace("```", "")
    return out


def _normalize_common(text: str) -> str:
    out = str(text or "")
    out = out.replace("\r\n", "\n").replace("\r", "\n")
    out = normalize_quotes(out)
    out = strip_markdown_fences(out)
    out = re.sub(r"(?i)\broot cause\b", "глубинная причина", out)
    out = re.sub(r"(?i)лучше\s+сказать\s*:", "Используй:", out)
    return out


def _insert_breaks_before_markers(text: str, markers: list[str]) -> str:
    out = str(text or "")
    for marker in markers:
        out = re.sub(rf"\s*{re.escape(marker)}", f"\n{marker}", out)
    out = re.sub(r"\n{3,}", "\n\n", out)
    return out.strip()


def normalize_training_text(text: str) -> str:
    out = _normalize_common(text)
    markers = [
        "## Для кого",
        "## Зачем это обучение",
        "## Что увидели в звонках / дневном контроле",
        "## Теория простыми словами",
        "## Основная модель / алгоритм",
        "## Как применять в звонке",
        "## Речевые модули",
        "## Частые ошибки",
        "## Мини-тренировка",
        "## Чек-лист на следующий рабочий день",
        "## Как руководитель будет проверять внедрение",
    ]
    return _insert_breaks_before_markers(out, markers)


def normalize_task_text(text: str) -> str:
    out = _normalize_common(text)
    markers = [
        "## Цель задания",
        "## Что нужно сделать",
        "## На каких звонках применить",
        "## Что записать после звонка",
        "## Критерии выполнения",
        "## Срок",
        "## Как будет проверяться",
    ]
    return _insert_breaks_before_markers(out, markers)

def _extract_section(text: str, heading_hints: list[str]) -> str:
    lines = str(text or "").splitlines()
    hints = [str(item or "").lower().strip() for item in heading_hints if str(item or "").strip()]
    start_idx = -1
    for idx, raw_line in enumerate(lines):
        line = raw_line.strip().lower()
        if not _HEADING_LINE_RE.match(line):
            continue
        if any(hint in line for hint in hints):
            start_idx = idx
            break
    if start_idx < 0:
        return ""
    end_idx = len(lines)
    for idx in range(start_idx + 1, len(lines)):
        if _HEADING_LINE_RE.match(lines[idx].strip()):
            end_idx = idx
            break
    return "\n".join(lines[start_idx:end_idx])


def _count_bullets(text: str) -> int:
    count = 0
    for raw_line in str(text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith(("- ", "• ", "* ")) or _CHECKBOX_BULLET_RE.match(line):
            count += 1
            inline_numbered = len(_INLINE_NUMBERED_RE.findall(line))
            if inline_numbered > 1:
                count += inline_numbered - 1
            continue
        inline_numbered = len(_INLINE_NUMBERED_RE.findall(line))
        if _NUMBERED_LINE_RE.match(line):
            count += 1
            if inline_numbered > 1:
                count += inline_numbered - 1
            continue
        if inline_numbered > 1:
            count += inline_numbered
    return count


def _count_speech_modules(text: str) -> int:
    count = 0
    for raw_line in str(text or "").splitlines():
        line = raw_line.strip()
        if not line or _HEADING_LINE_RE.match(line):
            continue
        inline_hits = len(_SPEECH_INLINE_PREFIX_RE.findall(line))
        prefixed = _SPEECH_PREFIX_RE.match(line)
        if prefixed:
            phrase = clean_text(prefixed.group(2))
            if phrase and phrase not in {'""', "''"}:
                count += max(1, inline_hits)
            continue
        if _SPEECH_INSTEAD_RE.match(line):
            count += 1
            continue
        if _SPEECH_QUOTED_RE.match(line):
            count += 1
            continue
        if _NUMBERED_LINE_RE.match(line) and len(clean_text(line)) >= 12:
            count += 1
            if inline_hits > 1:
                count += inline_hits - 1
            continue
        if inline_hits > 0:
            count += inline_hits
    return count


def _is_not_single_paragraph(*, text: str, sections_count: int, min_sections_threshold: int, min_non_empty_lines: int) -> bool:
    raw = str(text or "")
    non_empty_lines = [ln for ln in raw.splitlines() if ln.strip()]
    if len(non_empty_lines) >= int(min_non_empty_lines or 0):
        return True
    if int(sections_count or 0) >= int(min_sections_threshold or 0) and bool(HEADING_RE.search(raw)):
        return True
    paragraph_blocks = [blk for blk in re.split(r"\n\s*\n", raw) if blk.strip()]
    if len(paragraph_blocks) >= 3:
        return True
    return False


def _contains_forbidden_phrases(text: str) -> bool:
    probe = str(text or "").lower()
    return any(item in probe for item in FORBIDDEN_PHRASES)


def _contains_sources_section(text: str) -> bool:
    return bool(SOURCES_SECTION_RE.search(str(text or "")))


def _contains_external_urls(text: str) -> bool:
    return bool(URL_RE.search(str(text or "")))


def _detect_foreign_garbage(text: str) -> tuple[bool, list[str]]:
    probe = str(text or "")
    reasons: list[str] = []
    if CJK_RE.search(probe):
        reasons.append("cjk_text_detected")
    if "�" in probe:
        reasons.append("replacement_char_detected")
    if re.search(r"[ÐÑ][\w]{2,}", probe):
        reasons.append("mojibake_sequence_detected")
    return len(reasons) > 0, reasons


def _classify_latin_words(text: str) -> tuple[list[str], list[str]]:
    probe = str(text or "")
    for phrase in ALLOWED_LATIN_PHRASES:
        probe = re.sub(re.escape(phrase), " ", probe, flags=re.IGNORECASE)
    probe = re.sub(r"https?://\S+", " ", probe, flags=re.IGNORECASE)
    probe = re.sub(r"\b\S+@\S+\b", " ", probe)
    words = re.findall(r"\b[A-Za-z][A-Za-z0-9+._/-]{1,}\b", probe)
    blocked: list[str] = []
    warnings: list[str] = []
    for word in words:
        low = clean_text(word).lower().strip(".,;:!?()[]{}<>\"'-")
        if not low:
            continue
        if low in ALLOWED_LATIN_TERMS:
            continue
        if low.startswith("http") or low.startswith("www"):
            continue
        if re.search(r"\.(ru|com|net|org|io|ai)\b", low):
            continue
        if low.endswith((".md", ".txt", ".json", ".csv", ".xlsx", ".xls")):
            warnings.append(low)
            continue
        if "/" in low or "\\" in low or "_" in low or "." in low:
            warnings.append(low)
            continue
        if "-" in low:
            parts = [part for part in low.split("-") if part]
            if parts and all(part in ALLOWED_LATIN_TERMS for part in parts):
                continue
            warnings.append(low)
            continue
        if len(low) >= 18 and low.isalpha():
            blocked.append(low)
            continue
        warnings.append(low)

    unique_blocked: list[str] = []
    unique_warnings: list[str] = []
    seen: set[str] = set()
    for item in blocked:
        if item in seen:
            continue
        seen.add(item)
        unique_blocked.append(item)
    seen.clear()
    for item in warnings:
        if item in seen:
            continue
        seen.add(item)
        unique_warnings.append(item)
    return unique_blocked, unique_warnings

def review_training_quality(training_text: str) -> dict[str, Any]:
    text = normalize_training_text(training_text)
    chars = len(text)
    section_count = len(HEADING_RE.findall(text))
    speech_section = _extract_section(text, ["речевые модули", "речевые", "модули"])
    checklist_section = _extract_section(text, ["чек-лист", "чеклист", "контрольный список"])
    speech_modules_count = _count_speech_modules(speech_section)
    checklist_items_count = _count_bullets(checklist_section)
    no_single_paragraph = _is_not_single_paragraph(
        text=text,
        sections_count=section_count,
        min_sections_threshold=TRAINING_MIN_SECTIONS,
        min_non_empty_lines=8,
    )
    no_empty_quotes = not bool(EMPTY_QUOTES_RE.search(text)) and 'Используй: ""' not in text and 'Используй:""' not in text
    no_forbidden_phrases = not _contains_forbidden_phrases(text)
    contains_sources_section = _contains_sources_section(text)
    contains_external_urls = _contains_external_urls(text)

    foreign_words_examples, foreign_words_warning_examples = _classify_latin_words(text)
    foreign_words_count = len(foreign_words_examples)
    foreign_garbage_detected, foreign_garbage_reasons = _detect_foreign_garbage(text)

    fail_reasons: list[str] = []
    if chars < TRAINING_DOC_MIN_CHARS:
        fail_reasons.append(f"training_doc_too_short:{chars}")
    if section_count < TRAINING_MIN_SECTIONS:
        fail_reasons.append(f"sections_count_below_min:{section_count}")
    if speech_modules_count < TRAINING_MIN_SPEECH_MODULES:
        fail_reasons.append(f"speech_modules_count_below_min:{speech_modules_count}")
    if checklist_items_count < TRAINING_MIN_CHECKLIST_ITEMS:
        fail_reasons.append(f"checklist_items_count_below_min:{checklist_items_count}")
    if not no_single_paragraph:
        fail_reasons.append("single_paragraph_doc")
    if not no_empty_quotes:
        fail_reasons.append("empty_quotes_detected")
    if not no_forbidden_phrases:
        fail_reasons.append("forbidden_phrases_detected")
    if contains_sources_section:
        fail_reasons.append("sources_section_not_allowed")
    if contains_external_urls:
        fail_reasons.append("external_urls_not_allowed")
    if foreign_garbage_detected:
        fail_reasons.append("foreign_garbage_detected")

    return {
        "text": text,
        "training_chars": chars,
        "sections_count": section_count,
        "speech_modules_count": speech_modules_count,
        "checklist_items_count": checklist_items_count,
        "no_single_paragraph_doc": no_single_paragraph,
        "no_empty_quotes": no_empty_quotes,
        "no_forbidden_phrases": no_forbidden_phrases,
        "contains_sources_section": contains_sources_section,
        "contains_external_urls": contains_external_urls,
        "foreign_words_count": foreign_words_count,
        "foreign_words_examples": foreign_words_examples[:20],
        "foreign_words_warning_examples": foreign_words_warning_examples[:20],
        "foreign_garbage_detected": foreign_garbage_detected,
        "foreign_garbage_reasons": foreign_garbage_reasons,
        "quality_passed": len(fail_reasons) == 0,
        "quality_fail_reasons": fail_reasons,
    }


def review_task_quality(task_text: str) -> dict[str, Any]:
    text = normalize_task_text(task_text)
    chars = len(text)
    section_count = len(HEADING_RE.findall(text))
    no_single_paragraph = _is_not_single_paragraph(
        text=text,
        sections_count=section_count,
        min_sections_threshold=6,
        min_non_empty_lines=6,
    )
    no_empty_quotes = not bool(EMPTY_QUOTES_RE.search(text)) and 'Используй: ""' not in text and 'Используй:""' not in text
    no_forbidden_phrases = not _contains_forbidden_phrases(text)
    contains_sources_section = _contains_sources_section(text)
    contains_external_urls = _contains_external_urls(text)

    task_foreign_words_examples, task_foreign_words_warning_examples = _classify_latin_words(text)
    foreign_words_count = len(task_foreign_words_examples)
    foreign_garbage_detected, foreign_garbage_reasons = _detect_foreign_garbage(text)

    fail_reasons: list[str] = []
    if chars < POST_TASK_DOC_MIN_CHARS:
        fail_reasons.append(f"post_task_doc_too_short:{chars}")
    if section_count < 6:
        fail_reasons.append(f"task_sections_count_below_min:{section_count}")
    if not no_single_paragraph:
        fail_reasons.append("task_single_paragraph_doc")
    if not no_empty_quotes:
        fail_reasons.append("task_empty_quotes_detected")
    if not no_forbidden_phrases:
        fail_reasons.append("task_forbidden_phrases_detected")
    if contains_sources_section:
        fail_reasons.append("task_sources_section_not_allowed")
    if contains_external_urls:
        fail_reasons.append("task_external_urls_not_allowed")
    if foreign_garbage_detected:
        fail_reasons.append("task_foreign_garbage_detected")

    return {
        "text": text,
        "task_chars": chars,
        "task_sections_count": section_count,
        "task_no_single_paragraph_doc": no_single_paragraph,
        "task_no_empty_quotes": no_empty_quotes,
        "task_no_forbidden_phrases": no_forbidden_phrases,
        "contains_sources_section": contains_sources_section,
        "contains_external_urls": contains_external_urls,
        "task_foreign_words_count": foreign_words_count,
        "task_foreign_words_examples": task_foreign_words_examples[:20],
        "task_foreign_words_warning_examples": task_foreign_words_warning_examples[:20],
        "task_foreign_garbage_detected": foreign_garbage_detected,
        "task_foreign_garbage_reasons": foreign_garbage_reasons,
        "quality_passed": len(fail_reasons) == 0,
        "quality_fail_reasons": fail_reasons,
    }


def validate_candidate_row(row: dict[str, Any]) -> tuple[bool, list[str]]:
    errors: list[str] = []
    required = ("plan_week_start", "plan_week_end", "plan_date", "recipient", "activity_type", "what_i_do")
    for field in required:
        if not clean_text(row.get(field, "")):
            errors.append(f"missing_field:{field}")
    if not is_training_activity(str(row.get("activity_type", ""))):
        errors.append("not_training_activity")
    if not is_valid_url_or_empty(row.get("training_link")):
        errors.append("training_link_invalid")
    if not is_valid_url_or_empty(row.get("post_training_task_link")):
        errors.append("post_training_task_link_invalid")
    return len(errors) == 0, errors


def validate_draft_row(row: dict[str, Any]) -> tuple[bool, list[str]]:
    errors: list[str] = []
    training_title = clean_text(row.get("training_title", ""))
    training_text_raw = str(row.get("training_material", "") or "")
    task_title = clean_text(row.get("task_title", ""))
    task_text_raw = str(row.get("task_material", "") or "")

    if not training_title:
        errors.append("training_title_empty")
    if not clean_text(training_text_raw):
        errors.append("training_material_empty")
    if not task_title:
        errors.append("task_title_empty")
    if not clean_text(task_text_raw):
        errors.append("task_material_empty")
    if errors:
        return False, errors

    training_q = review_training_quality(training_text_raw)
    task_q = review_task_quality(task_text_raw)
    if not bool(training_q.get("quality_passed", False)):
        errors.extend([f"training_quality:{item}" for item in training_q.get("quality_fail_reasons", [])])
    if not bool(task_q.get("quality_passed", False)):
        errors.extend([f"task_quality:{item}" for item in task_q.get("quality_fail_reasons", [])])
    return len(errors) == 0, errors
