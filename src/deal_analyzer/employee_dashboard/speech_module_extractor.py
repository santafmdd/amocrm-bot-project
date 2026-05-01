from __future__ import annotations

import re
from collections import Counter
from typing import Any

from .models import EvidenceItem, SpeechModuleItem

_DIRECTIVE_RE = re.compile(
    r"^(?:\d+[.)]\s*)?(?:используй|скажи|формулировка)\s*:\s*(.+)$",
    flags=re.IGNORECASE,
)
_QUOTE_RE = re.compile(r"[\"«“](.{8,260}?)[\"»”]")
_NUMBERED_RE = re.compile(r"^\d+[.)]\s+(.{8,260})$")
_INSTEAD_RE = re.compile(r"вместо\s+(.{3,120}?)\s+используй\s+(.{3,180})", flags=re.IGNORECASE)


def _clean(value: Any) -> str:
    return " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split()).strip()


def _is_phrase_like(text: str) -> bool:
    probe = _clean(text)
    if len(probe) < 8:
        return False
    if probe.count(" ") < 1:
        return False
    return True


def _extract_section(text: str, heading: str) -> str:
    pattern = re.compile(rf"(?is)(?:^|\n)\s*#*\s*{re.escape(heading)}\s*(?:\n|:)(.*?)(?=\n\s*#|\Z)")
    match = pattern.search(text)
    if not match:
        return ""
    return match.group(1)


def extract_speech_modules_from_text(text: str) -> list[str]:
    body = str(text or "")
    found: list[str] = []

    for raw_line in body.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        directive_match = _DIRECTIVE_RE.match(line)
        if directive_match:
            phrase = _clean(directive_match.group(1))
            if _is_phrase_like(phrase):
                found.append(phrase)
            continue

        instead_match = _INSTEAD_RE.search(line)
        if instead_match:
            phrase = _clean(f'Вместо "{instead_match.group(1)}" используй "{instead_match.group(2)}"')
            if _is_phrase_like(phrase):
                found.append(phrase)

    section = _extract_section(body, "Речевые модули")
    if section:
        for raw_line in section.splitlines():
            line = raw_line.strip(" -\t")
            if not line:
                continue
            directive_match = _DIRECTIVE_RE.match(line)
            if directive_match:
                phrase = _clean(directive_match.group(1))
                if _is_phrase_like(phrase):
                    found.append(phrase)
                continue

            numbered = _NUMBERED_RE.match(line)
            if numbered:
                phrase = _clean(numbered.group(1))
                if _is_phrase_like(phrase):
                    found.append(phrase)
                continue

            quoted = _QUOTE_RE.findall(line)
            for chunk in quoted:
                phrase = _clean(chunk)
                if _is_phrase_like(phrase):
                    found.append(phrase)

    for match in _QUOTE_RE.findall(body):
        phrase = _clean(match)
        if _is_phrase_like(phrase):
            found.append(phrase)

    unique: list[str] = []
    seen: set[str] = set()
    for phrase in found:
        key = phrase.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(phrase)
    return unique


def collect_speech_module_items(evidence_items: list[EvidenceItem]) -> list[SpeechModuleItem]:
    modules: list[SpeechModuleItem] = []
    for item in evidence_items:
        phrases = extract_speech_modules_from_text(item.text)
        for phrase in phrases:
            modules.append(
                SpeechModuleItem(
                    phrase=phrase,
                    outcome=item.outcome,
                    source=item.source,
                    evidence_link=item.evidence_link,
                    evidence_date=item.evidence_date,
                )
            )
    return modules


def summarize_speech_modules(
    modules: list[SpeechModuleItem],
    *,
    top_n: int = 12,
) -> tuple[list[str], list[str], dict[str, Any]]:
    success_counter: Counter[str] = Counter()
    failure_counter: Counter[str] = Counter()

    for item in modules:
        key = _clean(item.phrase)
        if not key:
            continue
        if item.outcome == "success":
            success_counter[key] += 1
        elif item.outcome == "failure":
            failure_counter[key] += 1

    successful = [phrase for phrase, _count in success_counter.most_common(max(1, int(top_n or 12)))]
    failed = [phrase for phrase, _count in failure_counter.most_common(max(1, int(top_n or 12)))]

    debug = {
        "rows_total": len(modules),
        "successful_rows": sum(1 for item in modules if item.outcome == "success"),
        "failed_rows": sum(1 for item in modules if item.outcome == "failure"),
        "successful_examples": successful[:5],
        "failed_examples": failed[:5],
    }
    return successful, failed, debug
