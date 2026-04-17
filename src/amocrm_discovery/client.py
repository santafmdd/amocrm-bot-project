from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class AmoDiscoveryContext:
    base_domain: str
    access_token: str


class AmoDiscoveryClient:
    def __init__(self, *, base_domain: str, access_token: str) -> None:
        self.base_domain = str(base_domain or "").strip().strip("/")
        self.access_token = str(access_token or "").strip()
        if not self.base_domain:
            raise RuntimeError("amoCRM discovery: base_domain is required")
        if not self.access_token:
            raise RuntimeError("amoCRM discovery: access_token is required")

    def get_account_snapshot(self) -> dict[str, Any]:
        return self._get("/api/v4/account")

    def get_users(self, *, limit: int = 250) -> list[dict[str, Any]]:
        return self._collect_embedded_items(path="/api/v4/users", embedded_key="users", limit=limit)

    def get_pipelines_with_statuses(self, *, limit: int = 250) -> list[dict[str, Any]]:
        return self._collect_embedded_items(path="/api/v4/leads/pipelines", embedded_key="pipelines", limit=limit)

    def get_custom_fields(self, *, entity: str, limit: int = 250) -> list[dict[str, Any]]:
        endpoint = {
            "leads": "/api/v4/leads/custom_fields",
            "contacts": "/api/v4/contacts/custom_fields",
            "companies": "/api/v4/companies/custom_fields",
        }.get(entity)
        if not endpoint:
            raise RuntimeError(f"Unsupported custom fields entity: {entity}")
        return self._collect_embedded_items(path=endpoint, embedded_key="custom_fields", limit=limit)

    def get_lead(self, lead_id: int) -> dict[str, Any]:
        return self._get(f"/api/v4/leads/{int(lead_id)}")

    def get_lead_links(self, lead_id: int) -> list[dict[str, Any]]:
        payload = self._get(f"/api/v4/leads/{int(lead_id)}/links")
        embedded = payload.get("_embedded", {}) if isinstance(payload, dict) else {}
        links = embedded.get("links", []) if isinstance(embedded, dict) else []
        return [x for x in links if isinstance(x, dict)] if isinstance(links, list) else []

    def get_lead_notes(self, lead_id: int, *, limit: int = 250) -> list[dict[str, Any]]:
        return self._collect_embedded_items(
            path=f"/api/v4/leads/{int(lead_id)}/notes",
            embedded_key="notes",
            limit=limit,
        )

    def get_lead_tasks(self, lead_id: int, *, limit: int = 250) -> list[dict[str, Any]]:
        return self._collect_embedded_items(
            path="/api/v4/tasks",
            embedded_key="tasks",
            limit=limit,
            params={"filter[entity_type]": "leads", "filter[entity_id]": int(lead_id)},
        )

    def get_contact(self, contact_id: int) -> dict[str, Any]:
        return self._get(f"/api/v4/contacts/{int(contact_id)}")

    def get_company(self, company_id: int) -> dict[str, Any]:
        return self._get(f"/api/v4/companies/{int(company_id)}")

    def get_user(self, user_id: int) -> dict[str, Any]:
        return self._get(f"/api/v4/users/{int(user_id)}")

    def build_lead_bundle(self, lead_id: int) -> dict[str, Any]:
        lead = self.get_lead(lead_id)
        links = self.get_lead_links(lead_id)
        notes = self.get_lead_notes(lead_id)
        tasks = self.get_lead_tasks(lead_id)

        contact_ids: list[int] = []
        company_ids: list[int] = []
        for link in links:
            to_entity = str(link.get("to_entity_type", "") or "").lower()
            to_id = link.get("to_entity_id")
            if not isinstance(to_id, int):
                continue
            if to_entity == "contacts":
                contact_ids.append(to_id)
            elif to_entity == "companies":
                company_ids.append(to_id)

        contacts = [self.get_contact(cid) for cid in sorted(set(contact_ids))]
        companies = [self.get_company(cid) for cid in sorted(set(company_ids))]

        responsible_user = None
        responsible_id = lead.get("responsible_user_id")
        if isinstance(responsible_id, int):
            try:
                responsible_user = self.get_user(responsible_id)
            except RuntimeError:
                responsible_user = {"id": responsible_id, "error": "failed_to_fetch"}

        pipeline_snapshot = self._resolve_pipeline_status(lead)
        tags = self._extract_tags(lead)

        return {
            "lead": lead,
            "links": links,
            "contacts": contacts,
            "companies": companies,
            "notes": notes,
            "tasks": tasks,
            "tags": tags,
            "pipeline_status": pipeline_snapshot,
            "responsible_user": responsible_user,
            "custom_fields_values": lead.get("custom_fields_values", []),
        }

    def _resolve_pipeline_status(self, lead: dict[str, Any]) -> dict[str, Any]:
        pipeline_id = lead.get("pipeline_id")
        status_id = lead.get("status_id")
        summary: dict[str, Any] = {
            "pipeline_id": pipeline_id,
            "status_id": status_id,
            "pipeline_name": "",
            "status_name": "",
        }
        if not isinstance(pipeline_id, int) or not isinstance(status_id, int):
            return summary

        try:
            pipelines = self.get_pipelines_with_statuses()
        except RuntimeError:
            return summary

        for pipeline in pipelines:
            if pipeline.get("id") != pipeline_id:
                continue
            summary["pipeline_name"] = str(pipeline.get("name", "") or "")
            embedded = pipeline.get("_embedded", {}) if isinstance(pipeline, dict) else {}
            statuses = embedded.get("statuses", []) if isinstance(embedded, dict) else []
            if isinstance(statuses, list):
                for status in statuses:
                    if isinstance(status, dict) and status.get("id") == status_id:
                        summary["status_name"] = str(status.get("name", "") or "")
                        break
            break
        return summary

    @staticmethod
    def _extract_tags(lead: dict[str, Any]) -> list[dict[str, Any]]:
        embedded = lead.get("_embedded", {}) if isinstance(lead, dict) else {}
        tags = embedded.get("tags", []) if isinstance(embedded, dict) else []
        return [x for x in tags if isinstance(x, dict)] if isinstance(tags, list) else []

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
            merged_params: dict[str, Any] = {"limit": safe_limit, "page": page}
            if params:
                merged_params.update(params)
            payload = self._get(path, params=merged_params)
            embedded = payload.get("_embedded", {}) if isinstance(payload, dict) else {}
            items = embedded.get(embedded_key, []) if isinstance(embedded, dict) else []
            if not isinstance(items, list):
                break
            batch = [item for item in items if isinstance(item, dict)]
            if not batch:
                break
            collected.extend(batch)
            if len(batch) < safe_limit:
                break
        return collected

    def _get(self, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        query = ""
        if params:
            query = urlencode(params, doseq=True)
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
