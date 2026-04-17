from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from .config import AmoCollectorConfig


_URL_RE = re.compile(r"https?://[^\s<>'\"]+", re.IGNORECASE)
_MEETING_TASK_RE = re.compile(r"(встреч|демо|презент|созвон)", re.IGNORECASE)

_DEFAULT_FIELD_NAME_HINTS: dict[str, list[str]] = {
    "product": ["продукт", "товар", "услуг"],
    "source": ["источник", "канал", "utm"],
    "pain": ["боль", "проблем"],
    "business_tasks": ["бизнес", "задач", "business"],
    "brief": ["бриф", "brief"],
    "demo_result": ["демо", "встреч", "презентац"],
    "test_result": ["тест"],
    "probability": ["вероятност", "probab"],
}


@dataclass(frozen=True)
class DealScopeDecision:
    allowed: bool
    reason: str


@dataclass(frozen=True)
class LinkCollectionResult:
    candidates: list[str]
    from_company_comment: bool
    from_contact_comment: bool


class AmoDealNormalizer:
    def __init__(self, config: AmoCollectorConfig) -> None:
        self.config = config
        self._link_patterns = [re.compile(x, re.IGNORECASE) for x in config.presentation_link_search.regexes if str(x).strip()]
        self._manager_names_exclude_norm = {_norm_name(x) for x in config.manager_names_exclude if _norm_name(x)}

    def is_manager_allowed(self, responsible_user_id: int | None, responsible_user_name: str = "") -> DealScopeDecision:
        if not isinstance(responsible_user_id, int):
            return DealScopeDecision(allowed=False, reason="responsible_user_missing")

        include = self.config.manager_ids_include
        exclude = self.config.manager_ids_exclude

        if include and responsible_user_id not in include:
            return DealScopeDecision(allowed=False, reason="not_in_include")
        if responsible_user_id in exclude:
            return DealScopeDecision(allowed=False, reason="in_exclude")

        name_norm = _norm_name(responsible_user_name)
        if name_norm and name_norm in self._manager_names_exclude_norm:
            return DealScopeDecision(allowed=False, reason="name_in_exclude")

        return DealScopeDecision(allowed=True, reason="allowed")

    def normalize_bundle(self, bundle: dict[str, Any]) -> dict[str, Any]:
        lead = bundle.get("lead", {}) if isinstance(bundle, dict) else {}
        contacts = _as_list(bundle.get("contacts"))
        companies = _as_list(bundle.get("companies"))
        notes = _as_list(bundle.get("notes"))
        tasks = _as_list(bundle.get("tasks"))
        users_cache = _to_int_keyed_map(bundle.get("users_cache"))
        pipeline_name, status_name = self._resolve_pipeline_status_names(lead, bundle)

        responsible_user_id = _as_opt_int(lead.get("responsible_user_id"))
        responsible_user_name = ""
        if isinstance(responsible_user_id, int):
            user = users_cache.get(responsible_user_id, {})
            responsible_user_name = _clean_text(user.get("name"))
        scope = self.is_manager_allowed(responsible_user_id, responsible_user_name)

        deal_cfv = _as_list(lead.get("custom_fields_values"))
        contact = contacts[0] if contacts else {}
        company = companies[0] if companies else {}
        contact_cfv = _as_list(contact.get("custom_fields_values"))
        company_cfv = _as_list(company.get("custom_fields_values"))

        product_values = self._read_values_from_field(deal_cfv, self.config.product_field_id, _DEFAULT_FIELD_NAME_HINTS["product"])
        source_values = self._read_values_from_field(deal_cfv, self.config.source_field_id, _DEFAULT_FIELD_NAME_HINTS["source"])
        pain_text = self._read_text_from_field(deal_cfv, self.config.pain_field_id, _DEFAULT_FIELD_NAME_HINTS["pain"])
        business_tasks_text = self._read_text_from_field(deal_cfv, self.config.tasks_field_id, _DEFAULT_FIELD_NAME_HINTS["business_tasks"])
        demo_result_text = self._read_text_from_field(deal_cfv, self.config.demo_result_field_id, _DEFAULT_FIELD_NAME_HINTS["demo_result"])
        test_result_text = self._read_text_from_field(deal_cfv, self.config.test_result_field_id, _DEFAULT_FIELD_NAME_HINTS["test_result"])
        probability_value = self._read_scalar_from_field(deal_cfv, self.config.probability_field_id, _DEFAULT_FIELD_NAME_HINTS["probability"])

        brief_url_candidates = self._read_values_from_field(deal_cfv, self.config.brief_field_id, _DEFAULT_FIELD_NAME_HINTS["brief"])
        brief_url = brief_url_candidates[0] if brief_url_candidates else ""

        company_comment = self._read_text_from_field(company_cfv, self.config.company_comment_field_id, ["коммент", "comment"])
        contact_comment = self._read_text_from_field(contact_cfv, self.config.contact_comment_field_id, ["коммент", "comment"])

        company_name = _clean_text(company.get("name"))
        company_inn = self._extract_company_inn(company_cfv)
        contact_name = _clean_text(contact.get("name"))
        contact_phone = self._extract_contact_phone(contact_cfv)
        contact_email = self._extract_contact_email(contact_cfv)

        tags = self._extract_tags(lead)
        notes_summary_raw = self._summarize_notes(notes)
        tasks_summary_raw = self._summarize_tasks(tasks)

        links = self._collect_presentation_links(
            deal_cfv=deal_cfv,
            notes=notes,
            company_cfv=company_cfv,
            contact_cfv=contact_cfv,
            company_comment=company_comment,
            contact_comment=contact_comment,
        )

        longest_call_duration_seconds = self._find_longest_call_duration_seconds(notes)
        long_call_detected = longest_call_duration_seconds >= self.config.presentation_detection.min_call_duration_seconds

        completed_meeting_task = self._has_completed_meeting_task(tasks)
        comment_link_present = links.from_company_comment or links.from_contact_comment

        flags: dict[str, bool] = {
            "demo_result_present": bool(demo_result_text),
            "brief_present": bool(brief_url),
            "completed_meeting_task": completed_meeting_task,
            "long_call": long_call_detected,
            "comment_link_present": comment_link_present,
        }

        reasons: list[str] = []
        if flags["demo_result_present"]:
            reasons.append("demo_result_present")
        if flags["brief_present"]:
            reasons.append("brief_present")
        if flags["completed_meeting_task"]:
            reasons.append("completed_meeting_task")
        if flags["long_call"]:
            reasons.append(f"long_call_{longest_call_duration_seconds}s")
        if links.from_company_comment:
            reasons.append("company_comment_link_present")
        if links.from_contact_comment:
            reasons.append("contact_comment_link_present")

        required = self.config.presentation_detection.require_any_of
        presentation_detected = any(flags.get(key, False) for key in required) or bool(links.candidates)

        amo_lead_id = _as_opt_int(lead.get("id"))

        return {
            "deal_id": amo_lead_id,
            "amo_lead_id": amo_lead_id,
            "deal_name": _clean_text(lead.get("name")),
            "created_at": _as_opt_int(lead.get("created_at")),
            "updated_at": _as_opt_int(lead.get("updated_at")),
            "responsible_user_id": responsible_user_id,
            "responsible_user_name": responsible_user_name,
            "pipeline_id": _as_opt_int(lead.get("pipeline_id")),
            "pipeline_name": pipeline_name,
            "status_id": _as_opt_int(lead.get("status_id")),
            "status_name": status_name,
            "product_values": product_values,
            "source_values": source_values,
            "pain_text": pain_text,
            "business_tasks_text": business_tasks_text,
            "brief_url": brief_url,
            "demo_result_text": demo_result_text,
            "test_result_text": test_result_text,
            "probability_value": probability_value,
            "company_name": company_name,
            "company_inn": company_inn,
            "company_comment": company_comment,
            "contact_name": contact_name,
            "contact_phone": contact_phone,
            "contact_email": contact_email,
            "contact_comment": contact_comment,
            "tags": tags,
            "notes_summary_raw": notes_summary_raw,
            "tasks_summary_raw": tasks_summary_raw,
            "presentation_link_candidates": links.candidates,
            "presentation_detected": presentation_detected,
            "presentation_detect_reason": reasons,
            "long_call_detected": long_call_detected,
            "longest_call_duration_seconds": longest_call_duration_seconds,
            "manager_scope_allowed": scope.allowed,
            "training_candidate_text": "",
        }

    def _read_values_from_field(self, custom_fields_values: list[dict[str, Any]], field_id: int | None, fallback_name_hints: list[str]) -> list[str]:
        by_id = self._read_values_by_id(custom_fields_values, field_id)
        if by_id:
            return by_id
        return self._read_values_by_name_hints(custom_fields_values, fallback_name_hints)

    def _read_text_from_field(self, custom_fields_values: list[dict[str, Any]], field_id: int | None, fallback_name_hints: list[str]) -> str:
        values = self._read_values_from_field(custom_fields_values, field_id, fallback_name_hints)
        return "\n".join(values) if values else ""

    def _read_scalar_from_field(self, custom_fields_values: list[dict[str, Any]], field_id: int | None, fallback_name_hints: list[str]) -> int | float | str | None:
        values = self._read_values_from_field(custom_fields_values, field_id, fallback_name_hints)
        if not values:
            return None
        raw = values[0]
        try:
            if "." in raw:
                return float(raw)
            return int(raw)
        except ValueError:
            return raw

    def _read_values_by_id(self, custom_fields_values: list[dict[str, Any]], field_id: int | None) -> list[str]:
        if field_id is None:
            return []
        for field in custom_fields_values:
            if _as_opt_int(field.get("field_id")) != field_id:
                continue
            values = _as_list(field.get("values"))
            return _dedupe([_clean_text(item.get("value")) for item in values if _clean_text(item.get("value"))])
        return []

    def _read_values_by_name_hints(self, custom_fields_values: list[dict[str, Any]], hints: list[str]) -> list[str]:
        if not hints:
            return []
        norm_hints = [_norm_name(x) for x in hints if _norm_name(x)]
        for field in custom_fields_values:
            field_name = _norm_name(field.get("field_name") or field.get("name"))
            if not field_name:
                continue
            if not any(h in field_name for h in norm_hints):
                continue
            values = _as_list(field.get("values"))
            cleaned = _dedupe([_clean_text(item.get("value")) for item in values if _clean_text(item.get("value"))])
            if cleaned:
                return cleaned
        return []

    def _resolve_pipeline_status_names(self, lead: dict[str, Any], bundle: dict[str, Any]) -> tuple[str, str]:
        pipeline_id = _as_opt_int(lead.get("pipeline_id"))
        status_id = _as_opt_int(lead.get("status_id"))
        if pipeline_id is None or status_id is None:
            return "", ""

        pipelines = _as_list(bundle.get("pipelines_cache"))
        pipeline_name = ""
        status_name = ""
        for pipeline in pipelines:
            if _as_opt_int(pipeline.get("id")) != pipeline_id:
                continue
            pipeline_name = _clean_text(pipeline.get("name"))
            for status in _as_list((pipeline.get("_embedded") or {}).get("statuses")):
                if _as_opt_int(status.get("id")) == status_id:
                    status_name = _clean_text(status.get("name"))
                    break
            break

        if not status_name:
            status_entries = _as_list(bundle.get("status_cache"))
            for row in status_entries:
                if _as_opt_int(row.get("pipeline_id")) == pipeline_id and _as_opt_int(row.get("status_id")) == status_id:
                    status_name = _clean_text((row.get("status") or {}).get("name"))
                    break
        return pipeline_name, status_name

    @staticmethod
    def _extract_tags(lead: dict[str, Any]) -> list[str]:
        embedded = lead.get("_embedded", {}) if isinstance(lead, dict) else {}
        tags = _as_list((embedded if isinstance(embedded, dict) else {}).get("tags"))
        out: list[str] = []
        for tag in tags:
            name = _clean_text(tag.get("name"))
            if name:
                out.append(name)
        return _dedupe(out)

    @staticmethod
    def _summarize_notes(notes: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for note in notes:
            params = note.get("params", {}) if isinstance(note.get("params"), dict) else {}
            out.append(
                {
                    "id": _as_opt_int(note.get("id")),
                    "note_type": _clean_text(note.get("note_type")),
                    "text": _clean_text(note.get("text") or params.get("text")),
                    "duration": _as_opt_int(params.get("duration") or params.get("call_duration")),
                }
            )
        return out

    @staticmethod
    def _summarize_tasks(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for task in tasks:
            result = task.get("result", {}) if isinstance(task.get("result"), dict) else {}
            out.append(
                {
                    "id": _as_opt_int(task.get("id")),
                    "text": _clean_text(task.get("text")),
                    "is_completed": bool(task.get("is_completed", False)),
                    "result_text": _clean_text(result.get("text")),
                    "complete_till": _as_opt_int(task.get("complete_till")),
                }
            )
        return out

    def _collect_presentation_links(
        self,
        *,
        deal_cfv: list[dict[str, Any]],
        notes: list[dict[str, Any]],
        company_cfv: list[dict[str, Any]],
        contact_cfv: list[dict[str, Any]],
        company_comment: str,
        contact_comment: str,
    ) -> LinkCollectionResult:
        candidates: list[str] = []
        from_company_comment = False
        from_contact_comment = False

        if self.config.presentation_link_search.scan_deal_custom_fields_url:
            for field in deal_cfv:
                for item in _as_list(field.get("values")):
                    candidates.extend(self._extract_links_from_text(_clean_text(item.get("value"))))

        if self.config.presentation_link_search.scan_notes_common_text:
            for note in notes:
                text = _clean_text(note.get("text"))
                if not text and isinstance(note.get("params"), dict):
                    text = _clean_text((note.get("params") or {}).get("text"))
                candidates.extend(self._extract_links_from_text(text))

        if self.config.presentation_link_search.scan_company_comment:
            if company_comment:
                links = self._extract_links_from_text(company_comment)
                if links:
                    from_company_comment = True
                candidates.extend(links)
            else:
                for field in company_cfv:
                    name = _norm_name(field.get("field_name"))
                    if "коммент" in name or "comment" in name:
                        for item in _as_list(field.get("values")):
                            links = self._extract_links_from_text(_clean_text(item.get("value")))
                            if links:
                                from_company_comment = True
                            candidates.extend(links)

        if self.config.presentation_link_search.scan_contact_comment:
            if contact_comment:
                links = self._extract_links_from_text(contact_comment)
                if links:
                    from_contact_comment = True
                candidates.extend(links)
            else:
                for field in contact_cfv:
                    name = _norm_name(field.get("field_name"))
                    if "коммент" in name or "comment" in name:
                        for item in _as_list(field.get("values")):
                            links = self._extract_links_from_text(_clean_text(item.get("value")))
                            if links:
                                from_contact_comment = True
                            candidates.extend(links)

        return LinkCollectionResult(
            candidates=_dedupe(candidates),
            from_company_comment=from_company_comment,
            from_contact_comment=from_contact_comment,
        )

    def _extract_links_from_text(self, text: str) -> list[str]:
        if not text:
            return []
        links = [x.strip(".,);]\"") for x in _URL_RE.findall(text)]
        out: list[str] = []
        for link in links:
            if link and self._is_link_allowed(link):
                out.append(link)
        return out

    def _is_link_allowed(self, link: str) -> bool:
        if not self._link_patterns:
            return True
        for pattern in self._link_patterns:
            if pattern.search(link):
                return True
        host = urlparse(link).netloc
        return any(pattern.search(host) for pattern in self._link_patterns)

    @staticmethod
    def _find_longest_call_duration_seconds(notes: list[dict[str, Any]]) -> int:
        longest = 0
        for note in notes:
            note_type = _clean_text(note.get("note_type")).lower()
            if note_type not in {"call_in", "call_out"}:
                continue
            params = note.get("params", {}) if isinstance(note.get("params"), dict) else {}
            duration = _as_opt_int(params.get("duration") or params.get("call_duration") or note.get("duration"))
            if isinstance(duration, int):
                longest = max(longest, duration)
        return longest

    @staticmethod
    def _has_completed_meeting_task(tasks: list[dict[str, Any]]) -> bool:
        for task in tasks:
            if not bool(task.get("is_completed", False)):
                continue
            text = _clean_text(task.get("text"))
            result = task.get("result", {}) if isinstance(task.get("result"), dict) else {}
            result_text = _clean_text(result.get("text"))
            if _MEETING_TASK_RE.search(text) and result_text:
                return True
        return False

    @staticmethod
    def _extract_company_inn(company_cfv: list[dict[str, Any]]) -> str:
        for field in company_cfv:
            name = _norm_name(field.get("field_name"))
            if "инн" in name:
                for item in _as_list(field.get("values")):
                    value = _clean_text(item.get("value"))
                    if value:
                        return value
        return ""

    @staticmethod
    def _extract_contact_phone(contact_cfv: list[dict[str, Any]]) -> str:
        for field in contact_cfv:
            code = _clean_text(field.get("field_code")).upper()
            if code == "PHONE":
                values = [_clean_text(item.get("value")) for item in _as_list(field.get("values"))]
                return "; ".join(_dedupe([v for v in values if v]))
        return ""

    @staticmethod
    def _extract_contact_email(contact_cfv: list[dict[str, Any]]) -> str:
        for field in contact_cfv:
            code = _clean_text(field.get("field_code")).upper()
            if code == "EMAIL":
                values = [_clean_text(item.get("value")).lower() for item in _as_list(field.get("values"))]
                return "; ".join(_dedupe([v for v in values if v]))
        return ""


# Backward-compatible alias for existing imports.
AmoLeadNormalizer = AmoDealNormalizer


def _as_list(value: Any) -> list[dict[str, Any]]:
    return [x for x in value if isinstance(x, dict)] if isinstance(value, list) else []


def _to_int_keyed_map(value: Any) -> dict[int, dict[str, Any]]:
    if not isinstance(value, dict):
        return {}
    out: dict[int, dict[str, Any]] = {}
    for key, item in value.items():
        if not isinstance(item, dict):
            continue
        try:
            out[int(key)] = item
        except (TypeError, ValueError):
            continue
    return out


def _as_opt_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clean_text(value: Any) -> str:
    text = str(value or "").strip()
    return re.sub(r"\s+", " ", text)


def _norm_name(value: Any) -> str:
    text = _clean_text(value).lower().replace("ё", "е")
    text = re.sub(r"\s+", " ", text)
    return text


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        token = value.strip().lower()
        if token and token not in seen:
            seen.add(token)
            out.append(value.strip())
    return out
