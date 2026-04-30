
from __future__ import annotations

import html
import json
import os
import re
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote_plus, unquote, urlparse
from urllib.request import Request, urlopen

from src.integrations.google_sheets_api_client import AUTH_MODE_AUTO, GoogleSheetsApiClient

from ..daily_control.source_reader import clean_text, detect_header_row, map_headers, parse_date
from ..weekly_shared.sheets_discovery import resolve_spreadsheet_id
from ..weekly_shared.week_plan_reader import WEEK_PLAN_ALIASES
from .docs_writer import training_materials_required_scopes
from .models import SourceCoverage, SourceSnippet, TrainingCandidate
from .validation import build_topic_hash, is_training_activity, validate_candidate_row


PLAN_EXTRA_ALIASES: dict[str, tuple[str, ...]] = {
    "manager_role_profile": ("Роль менеджера", "manager_role_profile"),
    "what_to_check": ("Что проверяю", "what_to_check"),
    "daily_meeting_thesis": ("Общий тезис на дейлик", "daily_meeting_thesis"),
    "expected_quantity_effect": ("Ожидаемый эффект - количество", "expected_quantity_effect"),
    "expected_quality_effect": ("Ожидаемый эффект - качество", "expected_quality_effect"),
}

RUS_PRODUCTION_NON_WORKING_DAYS: set[str] = {
    "2026-05-01",
}


class ExternalMethodSourceProvider:
    def __init__(
        self,
        *,
        cfg: Any,
        provider: str = "auto",
        timeout_seconds: int = 10,
        logger: Any = None,
    ) -> None:
        self.cfg = cfg
        self.provider = str(provider or "auto").strip().lower() or "auto"
        self.timeout_seconds = max(3, int(timeout_seconds or 10))
        self.logger = logger

    def search(self, *, query: str, limit: int = 3) -> dict[str, Any]:
        q = clean_text(query)
        top_k = max(1, int(limit or 3))
        if not q:
            return self._empty("no_query", provider=self.provider)
        if self.provider == "disabled":
            return self._empty("disabled", provider="disabled")

        if self.provider == "auto":
            by_http = self._search_http_json(query=q, limit=top_k)
            if bool(by_http.get("used", False)):
                return by_http
            by_ddg = self._search_duckduckgo_html(query=q, limit=top_k)
            if bool(by_ddg.get("used", False)):
                return by_ddg
            by_manual = self._search_manual_curated(query=q, limit=top_k)
            if bool(by_manual.get("used", False)):
                return by_manual
            merged_errors = [
                *list(by_http.get("fetch_errors", []) if isinstance(by_http.get("fetch_errors"), list) else []),
                *list(by_ddg.get("fetch_errors", []) if isinstance(by_ddg.get("fetch_errors"), list) else []),
                *list(by_manual.get("fetch_errors", []) if isinstance(by_manual.get("fetch_errors"), list) else []),
            ]
            if merged_errors:
                return self._empty("provider_error", provider="auto", fetch_errors=merged_errors)
            return self._empty("no_results", provider="auto")

        if self.provider == "http_json":
            return self._search_http_json(query=q, limit=top_k)
        if self.provider == "duckduckgo_html":
            return self._search_duckduckgo_html(query=q, limit=top_k)
        if self.provider == "manual_curated_urls":
            return self._search_manual_curated(query=q, limit=top_k)
        return self._empty("provider_error", provider=self.provider, fetch_errors=[f"unsupported_provider:{self.provider}"])

    def _empty(self, status: str, *, provider: str, fetch_errors: list[str] | None = None) -> dict[str, Any]:
        errors = list(fetch_errors or [])
        return {
            "status": status,
            "provider": provider,
            "used": False,
            "sources": [],
            "snippets": [],
            "external_source_titles": [],
            "external_source_urls": [],
            "external_source_fetch_errors": errors,
            "fetch_errors": errors,
            "count": 0,
        }

    def _pack_results(
        self,
        *,
        provider: str,
        status: str,
        rows: list[dict[str, str]],
        fetch_errors: list[str] | None = None,
    ) -> dict[str, Any]:
        normalized: list[dict[str, str]] = []
        titles: list[str] = []
        urls: list[str] = []
        snippets: list[dict[str, str]] = []
        for item in rows:
            title = clean_text(item.get("title", ""))
            url = clean_text(item.get("url", ""))
            snippet = clean_text(item.get("snippet", ""))
            if not title and not snippet:
                continue
            if url and url not in urls:
                urls.append(url)
            if title:
                titles.append(title)
            source_ref = url or provider
            snippets.append({"source": source_ref, "text": snippet or title})
            normalized.append({"title": title, "url": url, "snippet": snippet, "provider": provider})
        errors = list(fetch_errors or [])
        return {
            "status": status if normalized else ("no_results" if not errors else "provider_error"),
            "provider": provider,
            "used": bool(normalized),
            "sources": normalized,
            "snippets": snippets,
            "external_source_titles": titles[:25],
            "external_source_urls": urls[:25],
            "external_source_fetch_errors": errors,
            "fetch_errors": errors,
            "count": len(normalized),
        }
    def _search_http_json(self, *, query: str, limit: int) -> dict[str, Any]:
        if not bool(getattr(self.cfg, "external_retrieval_enabled", False)):
            return self._empty("disabled", provider="http_json")
        adapter = str(getattr(self.cfg, "external_retrieval_adapter", "none") or "none").strip().lower()
        if adapter != "http_json":
            return self._empty("provider_error", provider="http_json", fetch_errors=[f"unsupported_adapter:{adapter}"])
        endpoint = str(getattr(self.cfg, "external_retrieval_endpoint", "") or "").strip()
        if not endpoint:
            return self._empty("unavailable", provider="http_json", fetch_errors=["missing_endpoint"])

        top_k = max(1, int(getattr(self.cfg, "external_retrieval_top_k", limit) or limit))
        timeout_s = max(3, int(getattr(self.cfg, "external_retrieval_timeout_seconds", self.timeout_seconds) or self.timeout_seconds))
        api_key = str(getattr(self.cfg, "external_retrieval_api_key", "") or "").strip()
        payload = json.dumps({"query": query, "top_k": min(top_k, limit)}, ensure_ascii=False).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        req = Request(endpoint, data=payload, headers=headers, method="POST")
        try:
            with urlopen(req, timeout=timeout_s) as response:
                raw = response.read().decode("utf-8", errors="replace")
        except HTTPError as exc:
            return self._empty("provider_error", provider="http_json", fetch_errors=[f"http_{exc.code}"])
        except (URLError, TimeoutError) as exc:
            return self._empty("provider_error", provider="http_json", fetch_errors=[str(getattr(exc, "reason", exc))])

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return self._empty("provider_error", provider="http_json", fetch_errors=["invalid_json"])

        items = parsed.get("items") if isinstance(parsed, dict) else parsed
        rows: list[dict[str, str]] = []
        if isinstance(items, list):
            for item in items[: max(1, int(limit or 1))]:
                if isinstance(item, dict):
                    rows.append(
                        {
                            "title": clean_text(item.get("title") or ""),
                            "url": clean_text(item.get("url") or item.get("source") or ""),
                            "snippet": clean_text(item.get("snippet") or item.get("text") or ""),
                        }
                    )
                else:
                    rows.append({"title": "", "url": endpoint, "snippet": clean_text(item)})
        return self._pack_results(provider="http_json", status="ok", rows=rows)

    @staticmethod
    def _decode_ddg_href(href: str) -> str:
        raw = clean_text(href)
        if not raw:
            return ""
        if raw.startswith("//"):
            raw = "https:" + raw
        if raw.startswith("/"):
            raw = "https://duckduckgo.com" + raw
        parsed = urlparse(raw)
        if "duckduckgo.com" in parsed.netloc and parsed.path.startswith("/l/"):
            qs = parse_qs(parsed.query)
            uddg = qs.get("uddg", [])
            if uddg:
                return clean_text(unquote(uddg[0]))
        return raw

    def _search_duckduckgo_html(self, *, query: str, limit: int) -> dict[str, Any]:
        ddg_url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
        req = Request(
            ddg_url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
                ),
                "Accept-Language": "ru,en-US;q=0.8,en;q=0.6",
            },
            method="GET",
        )
        try:
            with urlopen(req, timeout=self.timeout_seconds) as response:
                raw = response.read().decode("utf-8", errors="replace")
        except HTTPError as exc:
            return self._empty("provider_error", provider="duckduckgo_html", fetch_errors=[f"http_{exc.code}"])
        except (URLError, TimeoutError) as exc:
            return self._empty("provider_error", provider="duckduckgo_html", fetch_errors=[str(getattr(exc, "reason", exc))])

        anchors = re.findall(
            r'<a[^>]*class="result__a"[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
            raw,
            flags=re.IGNORECASE | re.DOTALL,
        )
        snippets = re.findall(
            r'<a[^>]*class="result__snippet"[^>]*>(.*?)</a>|<div[^>]*class="result__snippet"[^>]*>(.*?)</div>',
            raw,
            flags=re.IGNORECASE | re.DOTALL,
        )
        rows: list[dict[str, str]] = []
        for idx, item in enumerate(anchors[: max(1, int(limit or 1))]):
            href_raw, title_html = item
            title = clean_text(html.unescape(re.sub(r"<[^>]+>", " ", title_html)))
            url = self._decode_ddg_href(href_raw)
            snippet = ""
            if idx < len(snippets):
                one = snippets[idx]
                snippet_html = (one[0] or one[1] or "") if isinstance(one, tuple) else str(one or "")
                snippet = clean_text(html.unescape(re.sub(r"<[^>]+>", " ", snippet_html)))
            rows.append({"title": title, "url": url, "snippet": snippet})
        return self._pack_results(provider="duckduckgo_html", status="ok", rows=rows)

    def _search_manual_curated(self, *, query: str, limit: int) -> dict[str, Any]:
        _ = query
        configured = getattr(self.cfg, "training_materials_external_curated_urls", None)
        urls: list[str] = []
        if isinstance(configured, list):
            urls = [clean_text(item) for item in configured if clean_text(item)]
        if not urls:
            env_raw = str(os.environ.get("TRAINING_EXTERNAL_CURATED_URLS", "") or "").strip()
            if env_raw:
                urls = [clean_text(item) for item in env_raw.split(",") if clean_text(item)]
        if not urls:
            return self._empty("unavailable", provider="manual_curated_urls", fetch_errors=["curated_urls_missing"])

        rows: list[dict[str, str]] = []
        for url in urls[: max(1, int(limit or 1))]:
            rows.append(
                {
                    "title": f"Curated external methodology: {url}",
                    "url": url,
                    "snippet": "Внешний источник методики продаж/переговоров (curated fallback).",
                }
            )
        return self._pack_results(provider="manual_curated_urls", status="ok", rows=rows)

def _read_plan_matrix(
    *,
    cfg: Any,
    spreadsheet_id: str,
    plan_sheet_name: str,
    logger: Any,
    scopes: list[str] | None = None,
    auth_mode: str | None = None,
) -> tuple[list[str], list[list[str]], int]:
    app_root = Path(cfg.config_path).resolve().parents[1]
    client = GoogleSheetsApiClient(
        project_root=app_root,
        logger=logger,
        scopes=list(scopes or training_materials_required_scopes()),
        auth_mode=str(auth_mode or AUTH_MODE_AUTO),
    )
    matrix = client.get_values(spreadsheet_id, f"'{plan_sheet_name}'!A1:AZ")
    if not matrix:
        return [], [], 1
    header_row_number = detect_header_row(matrix, start_row=1, min_nonempty=3)
    header_idx = max(0, header_row_number - 1)
    headers = [clean_text(item) for item in matrix[header_idx]]
    rows = [list(map(clean_text, row)) for row in matrix[header_idx + 1 :]]
    return headers, rows, header_row_number


def _parse_week_bounds(row: list[str], mapping: dict[str, int]) -> tuple[str, str]:
    start_idx = mapping.get("plan_week_start")
    end_idx = mapping.get("plan_week_end")
    week_start = ""
    week_end = ""
    if isinstance(start_idx, int) and 0 <= start_idx < len(row):
        week_start = parse_date(row[start_idx])
    if isinstance(end_idx, int) and 0 <= end_idx < len(row):
        week_end = parse_date(row[end_idx])
    return week_start, week_end


def _parse_iso_date(value: str) -> date | None:
    parsed = parse_date(value)
    if not parsed:
        return None
    try:
        return date.fromisoformat(parsed)
    except ValueError:
        return None


def _is_non_working_day(day_iso: str) -> tuple[bool, str]:
    parsed = _parse_iso_date(day_iso)
    if parsed is None:
        return False, ""
    if day_iso in RUS_PRODUCTION_NON_WORKING_DAYS:
        return True, "holiday_calendar"
    if parsed.weekday() >= 5:
        return True, "weekend"
    return False, ""


def collect_training_candidates(
    *,
    cfg: Any,
    plan_sheet_name: str,
    week_start: str,
    week_end: str,
    manager: str = "",
    plan_date: str = "",
    limit: int = 0,
    logger: Any,
    scopes: list[str] | None = None,
    auth_mode: str | None = None,
) -> tuple[list[TrainingCandidate], dict[str, Any]]:
    spreadsheet_id = resolve_spreadsheet_id(cfg)
    headers, rows, header_row_number = _read_plan_matrix(
        cfg=cfg,
        spreadsheet_id=spreadsheet_id,
        plan_sheet_name=plan_sheet_name,
        logger=logger,
        scopes=scopes,
        auth_mode=auth_mode,
    )
    aliases = dict(WEEK_PLAN_ALIASES)
    aliases.update(PLAN_EXTRA_ALIASES)
    mapped = map_headers(headers, aliases).mapped

    candidates: list[TrainingCandidate] = []
    rejected_rows: list[dict[str, Any]] = []
    rows_skipped_existing_links = 0
    seen_idempotency_keys: set[str] = set()
    available_week_ranges: dict[str, int] = {}
    available_activity_types: dict[str, int] = {}
    plan_rows_in_week_by_exact_key = 0
    plan_rows_in_week_by_start_only = 0
    plan_rows_training_activity_total = 0
    plan_rows_training_activity_in_period = 0

    manager_norm = clean_text(manager).lower()
    plan_date_norm = parse_date(plan_date)
    requested_week_start = parse_date(week_start)
    requested_week_end = parse_date(week_end)
    requested_week_start_date = _parse_iso_date(requested_week_start)
    requested_week_end_date = _parse_iso_date(requested_week_end)
    if requested_week_start_date is not None and requested_week_end_date is not None and requested_week_end_date < requested_week_start_date:
        requested_week_start_date, requested_week_end_date = requested_week_end_date, requested_week_start_date

    def _pick(row: list[str], field: str) -> str:
        idx = mapped.get(field)
        if idx is None:
            return ""
        if idx < 0 or idx >= len(row):
            return ""
        return clean_text(row[idx])

    for idx, row in enumerate(rows):
        if not isinstance(row, list):
            continue
        row_number = int(header_row_number + idx + 1)
        row_week_start, row_week_end = _parse_week_bounds(row, mapped)
        row_plan_date = parse_date(_pick(row, "plan_date"))
        activity_type = _pick(row, "activity_type")
        recipient = _pick(row, "recipient")
        if row_week_start or row_week_end:
            week_key = f"{row_week_start or '?'}..{row_week_end or '?'}"
            available_week_ranges[week_key] = int(available_week_ranges.get(week_key, 0) or 0) + 1
        activity_key = clean_text(activity_type)
        if activity_key:
            available_activity_types[activity_key] = int(available_activity_types.get(activity_key, 0) or 0) + 1

        is_training = is_training_activity(str(activity_type or ""))
        if is_training:
            plan_rows_training_activity_total += 1

        period_matches = True
        if requested_week_start and requested_week_end:
            matches_exact = row_week_start == requested_week_start and row_week_end == requested_week_end
            matches_start_only = False
            matches_plan_date_only = False
            if (
                not matches_exact
                and row_week_start == requested_week_start
                and requested_week_start_date is not None
                and requested_week_end_date is not None
            ):
                row_plan_day = _parse_iso_date(row_plan_date)
                if row_plan_day is not None and requested_week_start_date <= row_plan_day <= requested_week_end_date:
                    matches_start_only = True

            if (
                not matches_exact
                and not matches_start_only
                and requested_week_start_date is not None
                and requested_week_end_date is not None
            ):
                row_plan_day = _parse_iso_date(row_plan_date)
                if row_plan_day is not None and requested_week_start_date <= row_plan_day <= requested_week_end_date:
                    matches_plan_date_only = True

            if matches_exact:
                plan_rows_in_week_by_exact_key += 1
            elif matches_start_only:
                plan_rows_in_week_by_start_only += 1
            elif matches_plan_date_only:
                plan_rows_in_week_by_start_only += 1
            else:
                period_matches = False
                if row_week_start == requested_week_start and row_week_end != requested_week_end:
                    rejected_rows.append(
                        {
                            "row_number": row_number,
                            "recipient": recipient,
                            "plan_date": row_plan_date,
                            "activity_type": activity_type,
                            "reason": "week_end_mismatch",
                            "requested_week_start": requested_week_start,
                            "requested_week_end": requested_week_end,
                            "found_week_start": row_week_start,
                            "found_week_end": row_week_end,
                        }
                    )
                elif not row_week_start and not row_week_end:
                    rejected_rows.append(
                        {
                            "row_number": row_number,
                            "recipient": recipient,
                            "plan_date": row_plan_date,
                            "activity_type": activity_type,
                            "reason": "week_bounds_missing_and_plan_date_outside",
                            "requested_week_start": requested_week_start,
                            "requested_week_end": requested_week_end,
                        }
                    )
                else:
                    rejected_rows.append(
                        {
                            "row_number": row_number,
                            "recipient": recipient,
                            "plan_date": row_plan_date,
                            "activity_type": activity_type,
                            "reason": "outside_requested_period",
                            "requested_week_start": requested_week_start,
                            "requested_week_end": requested_week_end,
                            "found_week_start": row_week_start,
                            "found_week_end": row_week_end,
                        }
                    )
        if not period_matches:
            continue

        if is_training:
            plan_rows_training_activity_in_period += 1

        if manager_norm and clean_text(recipient).lower() != manager_norm:
            rejected_rows.append(
                {
                    "row_number": row_number,
                    "recipient": recipient,
                    "plan_date": row_plan_date,
                    "activity_type": activity_type,
                    "reason": "manager_filter_mismatch",
                    "requested_manager": manager,
                }
            )
            continue

        if plan_date_norm and row_plan_date != plan_date_norm:
            rejected_rows.append(
                {
                    "row_number": row_number,
                    "recipient": recipient,
                    "plan_date": row_plan_date,
                    "activity_type": activity_type,
                    "reason": "plan_date_filter_mismatch",
                    "requested_plan_date": plan_date_norm,
                }
            )
            continue

        effective_week_start = row_week_start or requested_week_start
        effective_week_end = row_week_end or requested_week_end

        payload = {
            "plan_week_start": effective_week_start,
            "plan_week_end": effective_week_end,
            "plan_date": row_plan_date,
            "recipient": recipient,
            "manager_role_profile": _pick(row, "manager_role_profile"),
            "activity_type": activity_type,
            "status": _pick(row, "status"),
            "what_i_do": _pick(row, "what_i_do"),
            "task_to_assign": _pick(row, "task_to_assign"),
            "what_to_check": _pick(row, "what_to_check"),
            "daily_meeting_thesis": _pick(row, "daily_meeting_thesis"),
            "expected_quantity_effect": _pick(row, "expected_quantity_effect"),
            "expected_quality_effect": _pick(row, "expected_quality_effect"),
            "training_link": _pick(row, "training_link"),
            "post_training_task_link": _pick(row, "post_training_task_link"),
        }

        if not is_training:
            rejected_rows.append(
                {
                    "row_number": row_number,
                    "recipient": recipient,
                    "plan_date": row_plan_date,
                    "activity_type": activity_type,
                    "reason": "not_training_activity",
                }
            )
            continue

        non_working, non_working_reason = _is_non_working_day(row_plan_date)
        if non_working:
            rejected_rows.append(
                {
                    "row_number": row_number,
                    "recipient": recipient,
                    "plan_date": row_plan_date,
                    "activity_type": activity_type,
                    "reason": "non_working_day",
                    "non_working_reason": non_working_reason,
                }
            )
            continue

        ok, errors = validate_candidate_row(payload)
        if not ok:
            rejected_rows.append(
                {
                    "row_number": row_number,
                    "recipient": recipient,
                    "plan_date": row_plan_date,
                    "activity_type": activity_type,
                    "reason": "candidate_validation_failed",
                    "errors": errors,
                    "week_start_inferred_from_request": bool(not row_week_start and bool(effective_week_start)),
                    "week_end_inferred_from_request": bool(not row_week_end and bool(effective_week_end)),
                }
            )
            continue

        if clean_text(payload.get("training_link")) and clean_text(payload.get("post_training_task_link")):
            rows_skipped_existing_links += 1
            rejected_rows.append(
                {
                    "row_number": row_number,
                    "recipient": recipient,
                    "reason": "existing_links_present",
                }
            )
            continue

        topic_hash = build_topic_hash(str(payload.get("what_i_do") or ""))
        idempotency_key = "|".join(
            [
                str(payload.get("plan_week_start") or ""),
                str(payload.get("plan_week_end") or ""),
                str(payload.get("recipient") or "").lower(),
                str(payload.get("plan_date") or ""),
                str(payload.get("activity_type") or "").lower(),
                topic_hash,
            ]
        )
        if idempotency_key in seen_idempotency_keys:
            rejected_rows.append(
                {
                    "row_number": row_number,
                    "recipient": recipient,
                    "reason": "duplicate_idempotency_key",
                    "idempotency_key": idempotency_key,
                }
            )
            continue
        seen_idempotency_keys.add(idempotency_key)
        candidates.append(
            TrainingCandidate(
                row_number=row_number,
                plan_week_start=str(payload.get("plan_week_start") or ""),
                plan_week_end=str(payload.get("plan_week_end") or ""),
                plan_date=str(payload.get("plan_date") or ""),
                recipient=str(payload.get("recipient") or ""),
                manager_role_profile=str(payload.get("manager_role_profile") or ""),
                activity_type=str(payload.get("activity_type") or ""),
                status=str(payload.get("status") or ""),
                what_i_do=str(payload.get("what_i_do") or ""),
                task_to_assign=str(payload.get("task_to_assign") or ""),
                what_to_check=str(payload.get("what_to_check") or ""),
                daily_meeting_thesis=str(payload.get("daily_meeting_thesis") or ""),
                expected_quantity_effect=str(payload.get("expected_quantity_effect") or ""),
                expected_quality_effect=str(payload.get("expected_quality_effect") or ""),
                training_link=str(payload.get("training_link") or ""),
                post_training_task_link=str(payload.get("post_training_task_link") or ""),
                topic_hash=topic_hash,
                idempotency_key=idempotency_key,
            )
        )
        if int(limit or 0) > 0 and len(candidates) >= int(limit):
            break

    diagnostics = {
        "spreadsheet_id": spreadsheet_id,
        "plan_sheet": plan_sheet_name,
        "requested_week_start": requested_week_start,
        "requested_week_end": requested_week_end,
        "plan_headers": headers,
        "mapped_columns": {field: headers[idx] for field, idx in mapped.items() if idx < len(headers)},
        "row_count_total": len(rows),
        "plan_rows_total": len(rows),
        "plan_rows_in_week_by_exact_key": int(plan_rows_in_week_by_exact_key),
        "plan_rows_in_week_by_start_only": int(plan_rows_in_week_by_start_only),
        "plan_rows_training_activity_total": int(plan_rows_training_activity_total),
        "plan_rows_training_activity_in_period": int(plan_rows_training_activity_in_period),
        "available_week_ranges_in_plan_sheet": [
            {"week_range": key, "rows_count": int(value or 0)}
            for key, value in sorted(available_week_ranges.items(), key=lambda item: item[0])
        ],
        "available_activity_types": [
            {"activity_type": key, "rows_count": int(value or 0)}
            for key, value in sorted(available_activity_types.items(), key=lambda item: item[0].lower())
        ],
        "rows_training_candidates": len(candidates),
        "rows_skipped_existing_links": int(rows_skipped_existing_links),
        "rows_skipped": rejected_rows,
        "rejected_rows_with_reason": rejected_rows,
    }
    return candidates, diagnostics

def _read_text_snippet(path: Path, *, max_chars: int = 1200) -> str:
    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        try:
            text = path.read_text(encoding="utf-8-sig")
        except Exception:
            return ""
    compact = " ".join(str(text).replace("\n", " ").replace("\r", " ").split())
    return compact[: max(200, int(max_chars or 1200))]


def collect_source_snippets(
    *,
    cfg: Any,
    training_topic: str,
    project_root: Path,
    candidate: TrainingCandidate | None = None,
    external_search_provider: str = "auto",
    external_search_limit: int = 5,
) -> tuple[list[SourceSnippet], SourceCoverage]:
    warnings: list[str] = []
    snippets: list[SourceSnippet] = []

    style_paths: list[Path] = []
    pattern_file = project_root / "docs" / "мой паттерн общения.txt"
    if pattern_file.exists():
        style_paths.append(pattern_file)
    style_root = project_root / "docs" / "style_sources"
    if style_root.exists() and style_root.is_dir():
        style_paths.extend(sorted(style_root.rglob("*.txt")))
        style_paths.extend(sorted(style_root.rglob("*.md")))
    style_seen: set[str] = set()
    for path in style_paths:
        key = str(path.resolve())
        if key in style_seen:
            continue
        style_seen.add(key)
        snippet = _read_text_snippet(path, max_chars=1200)
        if snippet:
            snippets.append(SourceSnippet(source_type="style", source=str(path), text=snippet))
    if not style_seen:
        warnings.append("style_sources_missing")

    speech_paths: list[Path] = []
    docs_root = project_root / "docs"
    speech_markers = ("спич", "скрипт", "холодный звонок", "переговор", "возраж", "обуч")
    if docs_root.exists() and docs_root.is_dir():
        for path in sorted(docs_root.rglob("*")):
            if not path.is_file() or path.suffix.lower() not in {".txt", ".md", ".rtf"}:
                continue
            low_name = path.name.lower()
            if any(marker in low_name for marker in speech_markers):
                speech_paths.append(path)
                continue
            snippet = _read_text_snippet(path, max_chars=800).lower()
            if snippet and any(marker in snippet for marker in speech_markers):
                speech_paths.append(path)
    speech_seen: set[str] = set()
    for path in speech_paths[:40]:
        key = str(path.resolve())
        if key in speech_seen:
            continue
        speech_seen.add(key)
        snippet = _read_text_snippet(path, max_chars=1200)
        if snippet:
            snippets.append(SourceSnippet(source_type="speech", source=str(path), text=snippet))
    if not speech_seen:
        warnings.append("speech_sources_missing")

    product_refs = dict(getattr(cfg, "product_reference_urls", {}) or {})
    if not product_refs:
        product_refs = {
            "link": "https://istock.link/",
            "info": "https://istock.info/; https://istock.info/plm",
        }
    product_sources_used = 0
    for name, raw in product_refs.items():
        urls = [clean_text(x) for x in str(raw or "").split(";") if clean_text(x)]
        for url in urls:
            product_sources_used += 1
            snippets.append(
                SourceSnippet(
                    source_type="product",
                    source=url,
                    text=f"Источник продукта ({name}): {url}. Контекст: LINK, INFO, PLM, amoCRM.",
                )
            )
    if product_sources_used == 0:
        warnings.append("product_sources_from_internal_context")

    external_provider = ExternalMethodSourceProvider(
        cfg=cfg,
        provider=str(external_search_provider or "auto"),
        timeout_seconds=max(3, int(getattr(cfg, "external_retrieval_timeout_seconds", 10) or 10)),
    )
    external_result = external_provider.search(query=training_topic, limit=max(1, int(external_search_limit or 5)))
    external_used = bool(external_result.get("used", False))
    external_status = str(external_result.get("status", "external_search_unavailable"))
    external_titles = list(external_result.get("external_source_titles", []) if isinstance(external_result.get("external_source_titles"), list) else [])
    external_urls = list(external_result.get("external_source_urls", []) if isinstance(external_result.get("external_source_urls"), list) else [])
    external_fetch_errors = list(external_result.get("external_source_fetch_errors", []) if isinstance(external_result.get("external_source_fetch_errors"), list) else [])
    external_count = int(external_result.get("count", len(external_urls)) or 0)

    if isinstance(external_result.get("snippets"), list):
        for item in external_result.get("snippets", []):
            if isinstance(item, dict):
                text = clean_text(item.get("text") or item.get("snippet") or "")
                if text:
                    snippets.append(SourceSnippet(source_type="external", source=str(item.get("source") or item.get("url") or "external"), text=text))

    if not external_used:
        warnings.append("external_sources_missing")
    if external_fetch_errors:
        warnings.append("external_source_fetch_errors_present")

    if candidate is not None:
        snippets.append(
            SourceSnippet(
                source_type="plan",
                source="План недели",
                text=(
                    f"План недели: менеджер={clean_text(candidate.recipient)}; дата={clean_text(candidate.plan_date)}; "
                    f"что делаю={clean_text(candidate.what_i_do)}; задача={clean_text(candidate.task_to_assign)}; "
                    f"проверяю={clean_text(candidate.what_to_check)}."
                ),
            )
        )
        snippets.append(
            SourceSnippet(
                source_type="daily_control",
                source="Дневной контроль",
                text=(
                    f"Сигналы из ежедневного контроля: тезис={clean_text(candidate.daily_meeting_thesis)}; "
                    f"ожидаемый эффект количество={clean_text(candidate.expected_quantity_effect)}; "
                    f"ожидаемый эффект качество={clean_text(candidate.expected_quality_effect)}."
                ),
            )
        )
        snippets.append(
            SourceSnippet(
                source_type="call_review",
                source="Разбор звонков",
                text="Сигналы разбора звонков использованы через связанный недельный план и тезисы обучения. Опирайся на факты и не придумывай цитаты, которых нет в источниках.",
            )
        )

    coverage = SourceCoverage(
        style_sources_used=len([x for x in snippets if x.source_type == "style"]),
        speech_sources_used=len([x for x in snippets if x.source_type == "speech"]),
        product_sources_used=len([x for x in snippets if x.source_type == "product"]),
        external_sources_used=external_used,
        external_sources_count=max(external_count, len(external_urls)),
        external_source_titles=external_titles[:25],
        external_source_urls=external_urls[:25],
        external_source_fetch_errors=external_fetch_errors[:25],
        external_search_status=external_status,
        warnings=warnings,
    )
    return snippets, coverage


def serialize_sources(snippets: list[SourceSnippet]) -> list[dict[str, Any]]:
    return [asdict(item) for item in snippets]
