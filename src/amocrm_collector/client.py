from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class ApiRequestError(Exception):
    path: str
    message: str
    status: int | None = None
    content_type: str | None = None
    body_preview: str = ""

    def __str__(self) -> str:
        status_text = f" status={self.status}" if self.status is not None else ""
        ctype_text = f" content_type={self.content_type}" if self.content_type else ""
        return f"{self.message} path={self.path}{status_text}{ctype_text} body={self.body_preview[:200]}"


@dataclass(frozen=True)
class ApiResponseMeta:
    path: str
    status: int | None
    content_type: str | None


class AmoCollectorClient:
    def __init__(self, *, base_domain: str, access_token: str) -> None:
        self.base_domain = str(base_domain or "").strip().strip("/")
        self.access_token = str(access_token or "").strip()
        if not self.base_domain:
            raise RuntimeError("amoCRM collector: base_domain is required")
        if not self.access_token:
            raise RuntimeError("amoCRM collector: access_token is required")

        self._users_cache: dict[int, dict[str, Any]] | None = None
        self._pipelines_cache: list[dict[str, Any]] | None = None
        self._status_cache: dict[tuple[int, int], dict[str, Any]] | None = None

    def get_account(self) -> dict[str, Any]:
        return self._get("/api/v4/account")

    def get_leads_by_period(
        self,
        *,
        date_from_unix: int,
        date_to_unix: int,
        page: int,
        limit: int = 250,
        pipeline_ids: list[int] | None = None,
    ) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 250))
        params: dict[str, Any] = {
            "limit": safe_limit,
            "page": max(1, int(page)),
            "filter[created_at][from]": int(date_from_unix),
            "filter[created_at][to]": int(date_to_unix),
        }
        if pipeline_ids:
            params["filter[pipeline_id]"] = [int(x) for x in pipeline_ids]
        payload = self._get("/api/v4/leads", params=params)
        return _embedded_list(payload, "leads")

    def get_lead(self, lead_id: int) -> dict[str, Any]:
        return self._get(f"/api/v4/leads/{int(lead_id)}")

    def get_lead_links(self, lead_id: int) -> list[dict[str, Any]]:
        payload = self._get(f"/api/v4/leads/{int(lead_id)}/links")
        return _embedded_list(payload, "links")

    def get_notes_by_lead(self, lead_id: int, limit: int = 250) -> list[dict[str, Any]]:
        path = f"/api/v4/leads/{int(lead_id)}/notes"
        return self._collect_embedded_items_with_retries(
            path=path,
            embedded_key="notes",
            limit=limit,
            primary_params=None,
            fallback_params=[{"limit": max(1, min(int(limit), 100))}, {}],
        )

    def get_tasks_by_lead(self, lead_id: int, limit: int = 250) -> list[dict[str, Any]]:
        path = "/api/v4/tasks"
        return self._collect_embedded_items_with_retries(
            path=path,
            embedded_key="tasks",
            limit=limit,
            primary_params={"filter[entity_type]": "leads", "filter[entity_id]": int(lead_id)},
            fallback_params=[
                {"filter[entity_type]": "lead", "filter[entity_id]": int(lead_id)},
                {"filter[entity_id]": int(lead_id)},
            ],
        )

    def get_contacts_by_ids(self, ids: list[int]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for cid in sorted(set(int(x) for x in ids if isinstance(x, int) or str(x).isdigit())):
            out.append(self._get(f"/api/v4/contacts/{cid}"))
        return out

    def get_companies_by_ids(self, ids: list[int]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for cid in sorted(set(int(x) for x in ids if isinstance(x, int) or str(x).isdigit())):
            out.append(self._get(f"/api/v4/companies/{cid}"))
        return out

    def get_users_cache(self) -> dict[int, dict[str, Any]]:
        if self._users_cache is None:
            users = self._collect_embedded_items(path="/api/v4/users", embedded_key="users", limit=250)
            self._users_cache = {int(x.get("id")): x for x in users if isinstance(x.get("id"), int)}
        return self._users_cache

    def get_pipelines_cache(self) -> list[dict[str, Any]]:
        if self._pipelines_cache is None:
            self._pipelines_cache = self._collect_embedded_items(
                path="/api/v4/leads/pipelines",
                embedded_key="pipelines",
                limit=250,
            )
        return self._pipelines_cache

    def get_status_cache(self) -> dict[tuple[int, int], dict[str, Any]]:
        if self._status_cache is None:
            status_map: dict[tuple[int, int], dict[str, Any]] = {}
            for pipeline in self.get_pipelines_cache():
                pipeline_id = pipeline.get("id")
                if not isinstance(pipeline_id, int):
                    continue
                statuses = _embedded_list(pipeline, "statuses")
                for status in statuses:
                    status_id = status.get("id")
                    if isinstance(status_id, int):
                        status_map[(pipeline_id, status_id)] = status
            self._status_cache = status_map
        return self._status_cache

    def get_custom_fields(self, entity: str, limit: int = 250) -> list[dict[str, Any]]:
        endpoint = {
            "leads": "/api/v4/leads/custom_fields",
            "contacts": "/api/v4/contacts/custom_fields",
            "companies": "/api/v4/companies/custom_fields",
        }.get(entity)
        if not endpoint:
            raise RuntimeError(f"Unsupported entity for custom fields: {entity}")
        return self._collect_embedded_items(path=endpoint, embedded_key="custom_fields", limit=limit)

    def collect_lead_bundle(self, lead_id: int) -> dict[str, Any]:
        warnings: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []

        lead = self.get_lead(lead_id)

        links = self._safe_load_section(
            lead_id=lead_id,
            section="links",
            loader=lambda: self.get_lead_links(lead_id),
            warnings=warnings,
        )
        notes = self._safe_load_section(
            lead_id=lead_id,
            section="notes",
            loader=lambda: self.get_notes_by_lead(lead_id),
            warnings=warnings,
        )
        tasks = self._safe_load_section(
            lead_id=lead_id,
            section="tasks",
            loader=lambda: self.get_tasks_by_lead(lead_id),
            warnings=warnings,
        )

        contact_ids: list[int] = []
        company_ids: list[int] = []
        for item in links:
            to_entity_type = str(item.get("to_entity_type", "") or "").lower()
            to_id = item.get("to_entity_id")
            if not isinstance(to_id, int):
                continue
            if to_entity_type == "contacts":
                contact_ids.append(to_id)
            elif to_entity_type == "companies":
                company_ids.append(to_id)

        contacts: list[dict[str, Any]] = []
        for cid in sorted(set(contact_ids)):
            loaded = self._safe_load_section(
                lead_id=lead_id,
                section=f"contact_{cid}",
                loader=lambda contact_id=cid: [self._get(f"/api/v4/contacts/{contact_id}")],
                warnings=warnings,
            )
            contacts.extend(loaded)

        companies: list[dict[str, Any]] = []
        for cid in sorted(set(company_ids)):
            loaded = self._safe_load_section(
                lead_id=lead_id,
                section=f"company_{cid}",
                loader=lambda company_id=cid: [self._get(f"/api/v4/companies/{company_id}")],
                warnings=warnings,
            )
            companies.extend(loaded)

        users_cache = self._safe_dict_section(
            lead_id=lead_id,
            section="users_cache",
            loader=self.get_users_cache,
            warnings=warnings,
        )
        pipelines_cache = self._safe_load_section(
            lead_id=lead_id,
            section="pipelines_cache",
            loader=self.get_pipelines_cache,
            warnings=warnings,
        )

        status_cache_map = self._safe_dict_section(
            lead_id=lead_id,
            section="status_cache",
            loader=self.get_status_cache,
            warnings=warnings,
        )
        status_cache = [
            {"pipeline_id": pid, "status_id": sid, "status": status}
            for (pid, sid), status in status_cache_map.items()
            if isinstance(status, dict)
        ]

        return {
            "lead": lead,
            "links": links,
            "notes": notes,
            "tasks": tasks,
            "contacts": contacts,
            "companies": companies,
            "users_cache": users_cache,
            "pipelines_cache": pipelines_cache,
            "status_cache": status_cache,
            "warnings": warnings,
            "errors": errors,
        }

    def debug_deal_sections(self, lead_id: int) -> dict[str, Any]:
        lead_info = self._probe_object(f"/api/v4/leads/{int(lead_id)}")

        notes_info = self._probe_embedded_with_retries(
            path=f"/api/v4/leads/{int(lead_id)}/notes",
            embedded_key="notes",
            primary_params=None,
            fallback_params=[{"limit": 100}, {}],
        )
        tasks_info = self._probe_embedded_with_retries(
            path="/api/v4/tasks",
            embedded_key="tasks",
            primary_params={"filter[entity_type]": "leads", "filter[entity_id]": int(lead_id), "limit": 250, "page": 1},
            fallback_params=[
                {"filter[entity_type]": "lead", "filter[entity_id]": int(lead_id), "limit": 100, "page": 1},
                {"filter[entity_id]": int(lead_id), "limit": 100, "page": 1},
                {"filter[entity_id]": int(lead_id)},
            ],
        )

        links = self.get_lead_links(lead_id)
        contact_ids = sorted({int(x.get("to_entity_id")) for x in links if str(x.get("to_entity_type", "")).lower() == "contacts" and isinstance(x.get("to_entity_id"), int)})
        company_ids = sorted({int(x.get("to_entity_id")) for x in links if str(x.get("to_entity_type", "")).lower() == "companies" and isinstance(x.get("to_entity_id"), int)})

        contacts_info = self._probe_related_entities(entity="contacts", ids=contact_ids)
        companies_info = self._probe_related_entities(entity="companies", ids=company_ids)

        return {
            "deal_id": int(lead_id),
            "sections": {
                "lead": lead_info,
                "notes": notes_info,
                "tasks": tasks_info,
                "contacts": contacts_info,
                "companies": companies_info,
            },
        }

    def _safe_load_section(
        self,
        *,
        lead_id: int,
        section: str,
        loader,
        warnings: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        try:
            payload = loader()
            if isinstance(payload, list):
                return [x for x in payload if isinstance(x, dict)]
            return []
        except ApiRequestError as exc:
            warnings.append(
                {
                    "deal_id": lead_id,
                    "section": section,
                    "endpoint_path": exc.path,
                    "http_status": exc.status,
                    "content_type": exc.content_type,
                    "body_preview": exc.body_preview[:400],
                    "error": exc.message,
                }
            )
            return []
        except Exception as exc:
            warnings.append(
                {
                    "deal_id": lead_id,
                    "section": section,
                    "endpoint_path": "",
                    "http_status": None,
                    "content_type": None,
                    "body_preview": "",
                    "error": str(exc),
                }
            )
            return []

    def _safe_dict_section(
        self,
        *,
        lead_id: int,
        section: str,
        loader,
        warnings: list[dict[str, Any]],
    ) -> dict[Any, Any]:
        try:
            payload = loader()
            if isinstance(payload, dict):
                return payload
            return {}
        except ApiRequestError as exc:
            warnings.append(
                {
                    "deal_id": lead_id,
                    "section": section,
                    "endpoint_path": exc.path,
                    "http_status": exc.status,
                    "content_type": exc.content_type,
                    "body_preview": exc.body_preview[:400],
                    "error": exc.message,
                }
            )
            return {}
        except Exception as exc:
            warnings.append(
                {
                    "deal_id": lead_id,
                    "section": section,
                    "endpoint_path": "",
                    "http_status": None,
                    "content_type": None,
                    "body_preview": "",
                    "error": str(exc),
                }
            )
            return {}

    def _collect_embedded_items_with_retries(
        self,
        *,
        path: str,
        embedded_key: str,
        limit: int,
        primary_params: dict[str, Any] | None,
        fallback_params: list[dict[str, Any] | None],
    ) -> list[dict[str, Any]]:
        try:
            return self._collect_embedded_items(path=path, embedded_key=embedded_key, limit=limit, params=primary_params)
        except ApiRequestError as first_error:
            last_error = first_error
            for params in fallback_params:
                try:
                    payload = self._get(path, params=params)
                    return _embedded_list(payload, embedded_key)
                except ApiRequestError as retry_error:
                    last_error = retry_error
                    continue
            raise last_error

    def _collect_embedded_items(
        self,
        *,
        path: str,
        embedded_key: str,
        limit: int,
        params: dict[str, Any] | None = None,
        max_pages: int = 100,
    ) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 250))
        collected: list[dict[str, Any]] = []

        for page in range(1, max_pages + 1):
            merged: dict[str, Any] = {"limit": safe_limit, "page": page}
            if params:
                merged.update(params)
            payload = self._get(path, params=merged)
            batch = _embedded_list(payload, embedded_key)
            if not batch:
                break
            collected.extend(batch)
            if len(batch) < safe_limit:
                break
        return collected

    def _probe_object(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        request_path = self._build_request_path(path, params=params)
        try:
            payload, meta = self._get_with_meta(path, params=params)
            return {
                "endpoint": request_path,
                "status": meta.status,
                "content_type": meta.content_type,
                "item_count": 1 if isinstance(payload, dict) and payload else 0,
                "body_preview": "",
                "ok": True,
            }
        except ApiRequestError as exc:
            return {
                "endpoint": request_path,
                "status": exc.status,
                "content_type": exc.content_type,
                "item_count": 0,
                "body_preview": exc.body_preview[:400],
                "ok": False,
                "error": exc.message,
            }

    def _probe_embedded_with_retries(
        self,
        *,
        path: str,
        embedded_key: str,
        primary_params: dict[str, Any] | None,
        fallback_params: list[dict[str, Any] | None],
    ) -> dict[str, Any]:
        attempts = [primary_params] + list(fallback_params)
        last_error: ApiRequestError | None = None
        used_endpoint = self._build_request_path(path, params=primary_params)

        for params in attempts:
            request_path = self._build_request_path(path, params=params)
            used_endpoint = request_path
            try:
                payload, meta = self._get_with_meta(path, params=params)
                items = _embedded_list(payload, embedded_key)
                return {
                    "endpoint": request_path,
                    "status": meta.status,
                    "content_type": meta.content_type,
                    "item_count": len(items),
                    "body_preview": "",
                    "ok": True,
                }
            except ApiRequestError as exc:
                last_error = exc

        return {
            "endpoint": used_endpoint,
            "status": last_error.status if last_error else None,
            "content_type": last_error.content_type if last_error else None,
            "item_count": 0,
            "body_preview": (last_error.body_preview if last_error else "")[:400],
            "ok": False,
            "error": last_error.message if last_error else "unknown_error",
        }

    def _probe_related_entities(self, *, entity: str, ids: list[int]) -> dict[str, Any]:
        section_result = {
            "endpoint": f"/api/v4/{entity}/<id>",
            "status": 200,
            "content_type": "application/json",
            "item_count": 0,
            "body_preview": "",
            "ok": True,
            "ids": ids,
        }
        count = 0
        for entity_id in ids:
            request_path = f"/api/v4/{entity}/{int(entity_id)}"
            try:
                _, meta = self._get_with_meta(request_path)
                section_result["status"] = meta.status
                section_result["content_type"] = meta.content_type
                count += 1
            except ApiRequestError as exc:
                section_result.update(
                    {
                        "endpoint": request_path,
                        "status": exc.status,
                        "content_type": exc.content_type,
                        "body_preview": exc.body_preview[:400],
                        "ok": False,
                        "error": exc.message,
                    }
                )
                break

        section_result["item_count"] = count
        return section_result

    def _build_request_path(self, path: str, *, params: dict[str, Any] | None = None) -> str:
        query = urlencode(params or {}, doseq=True)
        return f"{path}?{query}" if query else path

    def _get(self, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        payload, _ = self._get_with_meta(path, params=params)
        return payload

    def _get_with_meta(self, path: str, *, params: dict[str, Any] | None = None) -> tuple[dict[str, Any], ApiResponseMeta]:
        query = urlencode(params or {}, doseq=True)
        url = f"https://{self.base_domain}{path}"
        request_path = f"{path}?{query}" if query else path
        if query:
            url += f"?{query}"

        req = Request(
            url,
            method="GET",
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "Accept": "application/json",
            },
        )
        return _open_json(req, path=request_path)


def _embedded_list(payload: dict[str, Any], key: str) -> list[dict[str, Any]]:
    embedded = payload.get("_embedded", {}) if isinstance(payload, dict) else {}
    items = embedded.get(key, []) if isinstance(embedded, dict) else []
    return [x for x in items if isinstance(x, dict)] if isinstance(items, list) else []


def _open_json(req: Request, *, path: str) -> tuple[dict[str, Any], ApiResponseMeta]:
    try:
        with urlopen(req, timeout=45) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            content_type = resp.headers.get("Content-Type", "")
            status = resp.getcode()
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
        raise ApiRequestError(
            path=path,
            message="amoCRM HTTP error",
            status=exc.code,
            content_type=exc.headers.get("Content-Type", "") if exc.headers else None,
            body_preview=body[:400],
        ) from exc
    except URLError as exc:
        raise ApiRequestError(path=path, message=f"amoCRM network error: {exc}") from exc

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ApiRequestError(
            path=path,
            message="amoCRM response is not JSON",
            status=status,
            content_type=content_type,
            body_preview=raw[:400],
        ) from exc

    if not isinstance(payload, dict):
        raise ApiRequestError(
            path=path,
            message="amoCRM JSON payload is not an object",
            status=status,
            content_type=content_type,
            body_preview=str(payload)[:400],
        )

    return payload, ApiResponseMeta(path=path, status=status, content_type=content_type)
