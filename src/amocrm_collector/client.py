from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


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
        return self._collect_embedded_items(path=f"/api/v4/leads/{int(lead_id)}/notes", embedded_key="notes", limit=limit)

    def get_tasks_by_lead(self, lead_id: int, limit: int = 250) -> list[dict[str, Any]]:
        return self._collect_embedded_items(
            path="/api/v4/tasks",
            embedded_key="tasks",
            limit=limit,
            params={"filter[entity_type]": "leads", "filter[entity_id]": int(lead_id)},
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
        lead = self.get_lead(lead_id)
        links = self.get_lead_links(lead_id)
        notes = self.get_notes_by_lead(lead_id)
        tasks = self.get_tasks_by_lead(lead_id)

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

        contacts = self.get_contacts_by_ids(contact_ids)
        companies = self.get_companies_by_ids(company_ids)

        users_cache = self.get_users_cache()
        pipelines_cache = self.get_pipelines_cache()
        status_cache = self.get_status_cache()

        return {
            "lead": lead,
            "links": links,
            "notes": notes,
            "tasks": tasks,
            "contacts": contacts,
            "companies": companies,
            "users_cache": users_cache,
            "pipelines_cache": pipelines_cache,
            "status_cache": [
                {"pipeline_id": pid, "status_id": sid, "status": status}
                for (pid, sid), status in status_cache.items()
            ],
        }

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

    def _get(self, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        query = urlencode(params or {}, doseq=True)
        url = f"https://{self.base_domain}{path}"
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
        return _open_json(req)


def _embedded_list(payload: dict[str, Any], key: str) -> list[dict[str, Any]]:
    embedded = payload.get("_embedded", {}) if isinstance(payload, dict) else {}
    items = embedded.get(key, []) if isinstance(embedded, dict) else []
    return [x for x in items if isinstance(x, dict)] if isinstance(items, list) else []


def _open_json(req: Request) -> dict[str, Any]:
    try:
        with urlopen(req, timeout=45) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
        raise RuntimeError(f"amoCRM HTTP error {exc.code}: {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"amoCRM network error: {exc}") from exc

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"amoCRM response is not JSON: {raw[:400]}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"amoCRM JSON payload is not an object: {payload}")
    return payload


