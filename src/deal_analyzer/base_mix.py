from __future__ import annotations

import re
from collections import Counter
from typing import Any

from src.deal_analyzer.tag_normalization import normalize_tag_value, sanitize_raw_tag

def normalize_tag_values(values: list[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in values:
        raw_tag, normalized_tag, _mapped = normalize_tag_value(raw)
        if not raw_tag:
            continue
        key = normalized_tag.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(normalized_tag)
    return out


def collect_raw_tag_values(values: list[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in values:
        text = sanitize_raw_tag(raw)
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def build_tag_entries(values: list[Any], *, source_of_tag: str) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for raw in values:
        raw_tag, normalized_tag, _mapped = normalize_tag_value(raw)
        if not raw_tag:
            continue
        pair = (raw_tag.lower(), normalized_tag.lower())
        if pair in seen:
            continue
        seen.add(pair)
        out.append(
            {
                "raw_tag": raw_tag,
                "normalized_tag": normalized_tag,
                "source_of_tag": source_of_tag,
            }
        )
    return out


def resolve_base_mix(records: list[dict[str, Any]]) -> dict[str, Any]:
    # 1) deal tags
    use_raw = any(isinstance(item.get("deal_tags_raw"), list) for item in records)
    deal_tags = _collect_values(records, keys=("deal_tags_raw",) if use_raw else ("tags",))
    raw_tags_deal = _collect_raw_values(records, keys=("deal_tags_raw", "tags"))
    raw_tags_company = _collect_raw_values(records, keys=("company_tags",))
    deal_tag_entries = _collect_tag_entries(records, keys=("deal_tags_raw", "tags"), source_of_tag="deal")
    company_tag_entries = _collect_tag_entries(records, keys=("company_tags",), source_of_tag="company")
    if deal_tags:
        return {
            "selected_source": "deal_tags",
            "selected_value": _top_joined(deal_tags),
            "fallback_used": False,
            "raw_tags_deal": raw_tags_deal,
            "raw_tags_company": raw_tags_company,
            "deal_tag_entries": deal_tag_entries,
            "company_tag_entries": company_tag_entries,
        }

    # 2) company tags
    company_tags = _collect_values(records, keys=("company_tags",))
    if company_tags:
        return {
            "selected_source": "company_tags",
            "selected_value": _top_joined(company_tags),
            "fallback_used": False,
            "raw_tags_deal": raw_tags_deal,
            "raw_tags_company": raw_tags_company,
            "deal_tag_entries": deal_tag_entries,
            "company_tag_entries": company_tag_entries,
        }

    # 3) source/form/url/title hints
    source_hints = _collect_source_hints(records)
    if source_hints:
        return {
            "selected_source": "source_hints",
            "selected_value": _top_joined(source_hints),
            "fallback_used": False,
            "raw_tags_deal": raw_tags_deal,
            "raw_tags_company": raw_tags_company,
            "deal_tag_entries": deal_tag_entries,
            "company_tag_entries": company_tag_entries,
        }

    # 4) company meaning / OKVED / comments hints
    semantic_hints = _collect_semantic_hints(records)
    if semantic_hints:
        return {
            "selected_source": "semantic_hints",
            "selected_value": _top_joined(semantic_hints),
            "fallback_used": False,
            "raw_tags_deal": raw_tags_deal,
            "raw_tags_company": raw_tags_company,
            "deal_tag_entries": deal_tag_entries,
            "company_tag_entries": company_tag_entries,
        }

    return {
        "selected_source": "fallback",
        "selected_value": "солянка",
        "fallback_used": True,
        "raw_tags_deal": raw_tags_deal,
        "raw_tags_company": raw_tags_company,
        "deal_tag_entries": deal_tag_entries,
        "company_tag_entries": company_tag_entries,
    }


def build_base_mix_text(records: list[dict[str, Any]]) -> str:
    return str(resolve_base_mix(records).get("selected_value") or "солянка")


def _collect_values(records: list[dict[str, Any]], *, keys: tuple[str, ...]) -> list[str]:
    counter: Counter[str] = Counter()
    for item in records:
        for key in keys:
            values = item.get(key)
            if not isinstance(values, list):
                continue
            for value in normalize_tag_values(values):
                counter[value] += 1
    return [name for name, _ in counter.most_common(6)]


def _collect_raw_values(records: list[dict[str, Any]], *, keys: tuple[str, ...]) -> list[str]:
    counter: Counter[str] = Counter()
    for item in records:
        for key in keys:
            values = item.get(key)
            if not isinstance(values, list):
                continue
            for value in collect_raw_tag_values(values):
                counter[value] += 1
    return [name for name, _ in counter.most_common(12)]


def _collect_tag_entries(
    records: list[dict[str, Any]],
    *,
    keys: tuple[str, ...],
    source_of_tag: str,
) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in records:
        for key in keys:
            values = item.get(key)
            if not isinstance(values, list):
                continue
            for entry in build_tag_entries(values, source_of_tag=source_of_tag):
                marker = (
                    entry.get("raw_tag", "").lower(),
                    entry.get("normalized_tag", "").lower(),
                    entry.get("source_of_tag", "").lower(),
                )
                if marker in seen:
                    continue
                seen.add(marker)
                entries.append(entry)
    return entries[:20]


def _collect_source_hints(records: list[dict[str, Any]]) -> list[str]:
    counter: Counter[str] = Counter()
    for item in records:
        raw_chunks: list[str] = []
        for key in ("source_values",):
            values = item.get(key)
            if isinstance(values, list):
                raw_chunks.extend(str(v or "") for v in values)
        for key in ("deal_name", "source_name", "lead_title", "form_name", "form_title", "source_url"):
            value = item.get(key)
            if value:
                raw_chunks.append(str(value))
        for chunk in raw_chunks:
            compact = " ".join(str(chunk or "").split()).strip()
            if not compact:
                continue
            for token in _extract_source_tokens(compact):
                counter[token] += 1
    return [name for name, _ in counter.most_common(6)]


def _collect_semantic_hints(records: list[dict[str, Any]]) -> list[str]:
    counter: Counter[str] = Counter()
    for item in records:
        company_name = " ".join(str(item.get("company_name") or "").split()).strip().lower()
        if company_name:
            if "завод" in company_name or "производ" in company_name:
                counter["производство"] += 1
            if "снабж" in company_name or "закуп" in company_name:
                counter["закупки"] += 1

        notes = item.get("notes_summary_raw") if isinstance(item.get("notes_summary_raw"), list) else []
        for note in notes:
            text = ""
            if isinstance(note, dict):
                text = str(note.get("text") or "")
            else:
                text = str(note or "")
            low = text.lower()
            if "оквэд" in low:
                counter["ОКВЭД/сегмент"] += 1
            if "тендер" in low:
                counter["тендерные"] += 1
            if "закуп" in low:
                counter["закупки"] += 1

        for key in ("company_comment", "contact_comment"):
            low = str(item.get(key) or "").lower()
            if "оквэд" in low:
                counter["ОКВЭД/сегмент"] += 1
            if "тендер" in low:
                counter["тендерные"] += 1
            if "закуп" in low:
                counter["закупки"] += 1
    return [name for name, _ in counter.most_common(6)]


def _extract_source_tokens(text: str) -> list[str]:
    low = text.lower()
    out: list[str] = []
    if "istock.link" in low:
        out.append("istock.link")
    if "исток" in low:
        out.append("istock")
    if "линк" in low or "link" in low:
        out.append("линк")
    if "инфо" in low or "info" in low:
        out.append("инфо")
    if "тендер" in low:
        out.append("тендерные")
    if "закуп" in low:
        out.append("закупки")
    if "форма" in low or "заявк" in low:
        out.append("входящие формы")

    for host in re.findall(r"https?://([a-z0-9.-]+)", low):
        clean = host.strip(".")
        if clean:
            out.append(clean)
    return normalize_tag_values(out)


def _top_joined(values: list[str]) -> str:
    if not values:
        return "солянка"
    return "; ".join(values[:3])
