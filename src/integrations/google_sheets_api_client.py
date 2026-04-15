"""Google Sheets API client (read-focused) for layout discovery flows."""

from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path
from typing import Any


SPREADSHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"
AUTH_MODE_AUTO = "auto"
AUTH_MODE_CACHE_ONLY = "cache_only"
AUTH_MODE_INTERACTIVE_BOOTSTRAP = "interactive_bootstrap"
_ALLOWED_AUTH_MODES = {AUTH_MODE_AUTO, AUTH_MODE_CACHE_ONLY, AUTH_MODE_INTERACTIVE_BOOTSTRAP}


def extract_spreadsheet_id(sheet_url: str) -> str:
    value = (sheet_url or "").strip()
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", value)
    if not match:
        raise RuntimeError(f"Could not parse spreadsheetId from sheet_url: {sheet_url}")
    return match.group(1)


def _read_token_scopes(token_file: Path) -> list[str]:
    try:
        payload = json.loads(token_file.read_text(encoding="utf-8"))
    except Exception:
        return []
    scopes = payload.get("scopes", []) if isinstance(payload, dict) else []
    if isinstance(scopes, list):
        return [str(s).strip() for s in scopes if str(s).strip()]
    if isinstance(scopes, str) and scopes.strip():
        return [scopes.strip()]
    return []


def _scopes_match(expected: list[str], actual: list[str]) -> bool:
    return set(expected) == set(actual)


def _normalize_auth_mode(raw: str | None) -> str:
    value = str(raw or "").strip().lower()
    if value in _ALLOWED_AUTH_MODES:
        return value
    return AUTH_MODE_AUTO


def _quote_sheet_title(title: str) -> str:
    safe = str(title or "").replace("'", "''")
    return f"'{safe}'"


def _split_range_tab(range_a1: str) -> tuple[str, str]:
    raw = str(range_a1 or "").strip()
    if "!" not in raw:
        return "", raw
    tab, suffix = raw.split("!", 1)
    tab = tab.strip()
    if tab.startswith("'") and tab.endswith("'") and len(tab) >= 2:
        tab = tab[1:-1].replace("''", "'")
    return tab, suffix.strip()


class GoogleSheetsApiClient:
    """Small read-oriented wrapper around Google Sheets API v4."""

    def __init__(self, project_root: Path, logger: logging.Logger | None = None) -> None:
        self.project_root = project_root
        self.logger = logger or logging.getLogger("project")
        self.scopes = [SPREADSHEETS_SCOPE]

        credentials_path = os.getenv("GOOGLE_API_CREDENTIALS_FILE", "").strip()
        token_path = os.getenv("GOOGLE_API_TOKEN_FILE", "").strip()

        self.credentials_file = Path(credentials_path) if credentials_path else (project_root / "credentials.json")
        self.token_file = Path(token_path) if token_path else (project_root / "token.json")

        mode_env = os.getenv("GOOGLE_API_AUTH_MODE", AUTH_MODE_AUTO)
        self.auth_mode = _normalize_auth_mode(mode_env)
        self._service = None
        self._spreadsheet_meta_cache: dict[str, dict[str, Any]] = {}

    def _load_google_libs(self):
        try:
            from google.auth.transport.requests import Request
            from google.oauth2.credentials import Credentials
            from google_auth_oauthlib.flow import InstalledAppFlow
            from googleapiclient.discovery import build
        except Exception as exc:
            raise RuntimeError(
                "Google API libraries are not installed. Install google-auth, google-auth-oauthlib, and google-api-python-client."
            ) from exc
        return Request, Credentials, InstalledAppFlow, build

    def _allow_interactive_auth(self) -> bool:
        return self.auth_mode in {AUTH_MODE_AUTO, AUTH_MODE_INTERACTIVE_BOOTSTRAP}

    def build_service(self):
        if self._service is not None:
            return self._service

        Request, Credentials, InstalledAppFlow, build = self._load_google_libs()

        self.logger.info("google auth mode selected: %s", self.auth_mode)
        self.logger.info("oauth scopes requested: %s", self.scopes)
        self.logger.info("google auth files: credentials=%s token=%s", self.credentials_file, self.token_file)

        creds = None
        token_scope_mismatch = False
        used_cached_token = False
        refreshed_token = False

        if self.token_file.exists():
            cached_scopes = _read_token_scopes(self.token_file)
            if cached_scopes and not _scopes_match(self.scopes, cached_scopes):
                token_scope_mismatch = True
                self.logger.warning(
                    "Token scopes mismatch detected. token_file=%s cached_scopes=%s requested_scopes=%s. "
                    "Re-issuing OAuth token is required.",
                    self.token_file,
                    cached_scopes,
                    self.scopes,
                )
            else:
                creds = Credentials.from_authorized_user_file(str(self.token_file), self.scopes)
                used_cached_token = True

        if creds and creds.valid:
            self.logger.info("google auth: using cached token")
            self._service = build("sheets", "v4", credentials=creds)
            return self._service

        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                refreshed_token = bool(creds.valid)
                if refreshed_token:
                    self.token_file.write_text(creds.to_json(), encoding="utf-8")
                    self.logger.info("google auth: refresh token updated")
                    self._service = build("sheets", "v4", credentials=creds)
                    return self._service
            except Exception as exc:
                self.logger.warning(
                    "Token refresh failed for %s. OAuth re-auth may be required. error=%s",
                    self.token_file,
                    exc,
                )
                creds = None

        if not self._allow_interactive_auth():
            reason = "token_missing_or_invalid"
            if token_scope_mismatch:
                reason = "token_scope_mismatch"
            elif used_cached_token and not refreshed_token:
                reason = "cached_token_not_usable"
            self.logger.error("google auth: interactive authorization disabled (mode=%s) reason=%s", self.auth_mode, reason)
            if self.auth_mode == AUTH_MODE_CACHE_ONLY:
                raise RuntimeError(
                    "Google auth cache_only: token missing/invalid. Interactive OAuth is forbidden in this mode. "
                    "Run one explicit bootstrap with --google-auth-mode interactive_bootstrap."
                )
            raise RuntimeError(
                "Google Sheets API token is missing/invalid and interactive OAuth is disabled. "
                "Run bootstrap once with GOOGLE_API_AUTH_MODE=interactive_bootstrap (or auto) to create/refresh token.json."
            )

        if not self.credentials_file.exists():
            raise RuntimeError(
                f"Google API credentials file not found: {self.credentials_file}. "
                "Set GOOGLE_API_CREDENTIALS_FILE or place credentials.json in project root."
            )

        self.logger.info("google auth: interactive authorization required (system browser OAuth flow)")
        try:
            flow = InstalledAppFlow.from_client_secrets_file(str(self.credentials_file), self.scopes)
            creds = flow.run_local_server(port=0)
        except Exception as exc:
            hint = (
                f"OAuth authorization failed for scopes={self.scopes}. "
                f"If you recently changed scopes, remove token cache file: {self.token_file}"
            )
            raise RuntimeError(hint) from exc

        self.token_file.write_text(creds.to_json(), encoding="utf-8")
        if token_scope_mismatch:
            self.logger.info("OAuth token cache refreshed due to scope mismatch: %s", self.token_file)
        else:
            self.logger.info("google auth: token cache created/updated: %s", self.token_file)

        self._service = build("sheets", "v4", credentials=creds)
        return self._service

    def get_spreadsheet_metadata(self, spreadsheet_id: str, *, force_refresh: bool = False) -> dict[str, Any]:
        key = str(spreadsheet_id or "").strip()
        if key and not force_refresh and key in self._spreadsheet_meta_cache:
            return self._spreadsheet_meta_cache[key]
        service = self.build_service()
        payload = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        if isinstance(payload, dict) and key:
            self._spreadsheet_meta_cache[key] = payload
        return payload if isinstance(payload, dict) else {}

    def list_sheets(self, spreadsheet_id: str) -> list[dict[str, Any]]:
        meta = self.get_spreadsheet_metadata(spreadsheet_id)
        sheets = meta.get("sheets", []) if isinstance(meta, dict) else []
        result: list[dict[str, Any]] = []
        for item in sheets:
            props = item.get("properties", {}) if isinstance(item, dict) else {}
            result.append(
                {
                    "sheetId": props.get("sheetId"),
                    "title": props.get("title", ""),
                    "index": props.get("index"),
                    "rowCount": props.get("gridProperties", {}).get("rowCount"),
                    "columnCount": props.get("gridProperties", {}).get("columnCount"),
                }
            )
        return result

    def resolve_sheet_title(self, spreadsheet_id: str, requested_tab_name: str) -> str:
        requested = str(requested_tab_name or "").strip()
        if not requested:
            raise RuntimeError("Requested sheet tab name is empty")
        sheets = self.list_sheets(spreadsheet_id)
        titles = [str(item.get("title", "")).strip() for item in sheets if str(item.get("title", "")).strip()]
        for title in titles:
            if title == requested:
                return title
        requested_norm = requested.lower()
        for title in titles:
            if title.lower() == requested_norm:
                return title
        raise RuntimeError(
            f"Google Sheets tab not found: requested='{requested}'. Available tabs={titles}"
        )

    def resolve_sheet(self, spreadsheet_id: str, requested_tab_name: str) -> dict[str, Any]:
        requested = str(requested_tab_name or "").strip()
        if not requested:
            raise RuntimeError("Requested sheet tab name is empty")
        sheets = self.list_sheets(spreadsheet_id)
        exact = None
        ci = None
        for item in sheets:
            title = str(item.get("title", "") or "").strip()
            if not title:
                continue
            if title == requested:
                exact = item
                break
            if ci is None and title.lower() == requested.lower():
                ci = item
        selected = exact or ci
        if not selected:
            titles = [str(item.get("title", "")).strip() for item in sheets if str(item.get("title", "")).strip()]
            raise RuntimeError(f"Google Sheets tab not found: requested='{requested}'. Available tabs={titles}")
        return selected

    def insert_rows(self, *, spreadsheet_id: str, tab_name: str, start_index: int, row_count: int) -> dict[str, Any]:
        count = int(row_count or 0)
        if count <= 0:
            return {"replies": [], "insertedRows": 0}
        sheet = self.resolve_sheet(spreadsheet_id, tab_name)
        sheet_id = sheet.get("sheetId")
        if sheet_id is None:
            raise RuntimeError(f"Sheet id not found for tab: {tab_name}")
        service = self.build_service()
        body = {
            "requests": [
                {
                    "insertDimension": {
                        "range": {
                            "sheetId": int(sheet_id),
                            "dimension": "ROWS",
                            "startIndex": int(max(0, start_index)),
                            "endIndex": int(max(0, start_index)) + count,
                        },
                        "inheritFromBefore": True,
                    }
                }
            ]
        }
        payload = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
        key = str(spreadsheet_id or "").strip()
        if key:
            self._spreadsheet_meta_cache.pop(key, None)
        return payload if isinstance(payload, dict) else {"raw": payload}
    def build_tab_a1_range(self, *, tab_title: str, range_suffix: str) -> str:
        suffix = str(range_suffix or "").strip()
        if not suffix:
            raise RuntimeError("Google Sheets range suffix is empty")
        return f"{_quote_sheet_title(tab_title)}!{suffix}"

    def _normalize_range_for_tab(self, spreadsheet_id: str, range_a1: str) -> str:
        tab, suffix = _split_range_tab(range_a1)
        if not tab:
            return range_a1
        resolved = self.resolve_sheet_title(spreadsheet_id, tab)
        return self.build_tab_a1_range(tab_title=resolved, range_suffix=suffix)

    def _raise_range_error_with_context(self, *, spreadsheet_id: str, range_a1: str, exc: Exception) -> None:
        message = str(exc)
        requested_tab, _ = _split_range_tab(range_a1)
        if "Unable to parse range" in message or "Invalid range" in message:
            available = [item.get("title", "") for item in self.list_sheets(spreadsheet_id)]
            raise RuntimeError(
                "Google Sheets range request failed. "
                f"requested_range={range_a1} requested_tab={requested_tab!r} available_tabs={available}. "
                "Check tab_name/table_mappings configuration."
            ) from exc
        raise exc

    def get_values(self, spreadsheet_id: str, range_a1: str) -> list[list[str]]:
        service = self.build_service()
        normalized_range = self._normalize_range_for_tab(spreadsheet_id, range_a1)
        try:
            payload = (
                service.spreadsheets()
                .values()
                .get(spreadsheetId=spreadsheet_id, range=normalized_range)
                .execute()
            )
        except Exception as exc:
            self._raise_range_error_with_context(spreadsheet_id=spreadsheet_id, range_a1=range_a1, exc=exc)
            raise
        raw_values = payload.get("values", []) if isinstance(payload, dict) else []
        values: list[list[str]] = []
        for row in raw_values:
            if isinstance(row, list):
                values.append([str(cell) for cell in row])
            else:
                values.append([str(row)])
        return values

    def batch_get_values(self, spreadsheet_id: str, ranges: list[str]) -> dict[str, list[list[str]]]:
        if not ranges:
            return {}
        service = self.build_service()
        normalized_ranges = [self._normalize_range_for_tab(spreadsheet_id, r) for r in ranges]
        try:
            payload = (
                service.spreadsheets()
                .values()
                .batchGet(spreadsheetId=spreadsheet_id, ranges=normalized_ranges)
                .execute()
            )
        except Exception as exc:
            bad_range = ranges[0] if ranges else ""
            self._raise_range_error_with_context(spreadsheet_id=spreadsheet_id, range_a1=bad_range, exc=exc)
            raise
        value_ranges = payload.get("valueRanges", []) if isinstance(payload, dict) else []
        result: dict[str, list[list[str]]] = {}
        for item in value_ranges:
            if not isinstance(item, dict):
                continue
            key = str(item.get("range", "")).strip()
            rows = item.get("values", [])
            matrix: list[list[str]] = []
            for row in rows if isinstance(rows, list) else []:
                if isinstance(row, list):
                    matrix.append([str(cell) for cell in row])
                else:
                    matrix.append([str(row)])
            if key:
                result[key] = matrix
        return result

    def batch_update_values(self, spreadsheet_id: str, data: list[dict[str, Any]]) -> dict[str, Any]:
        """Write values via spreadsheets.values.batchUpdate."""
        if not data:
            return {"totalUpdatedRows": 0, "totalUpdatedColumns": 0, "totalUpdatedCells": 0, "responses": []}

        service = self.build_service()
        normalized_data: list[dict[str, Any]] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            raw_range = str(item.get("range", "")).strip()
            if not raw_range:
                continue
            normalized = dict(item)
            normalized["range"] = self._normalize_range_for_tab(spreadsheet_id, raw_range)
            normalized_data.append(normalized)

        body = {
            "valueInputOption": "RAW",
            "data": normalized_data,
        }
        try:
            payload = (
                service.spreadsheets()
                .values()
                .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
                .execute()
            )
        except Exception as exc:
            bad_range = str(normalized_data[0].get("range", "")) if normalized_data else ""
            self._raise_range_error_with_context(spreadsheet_id=spreadsheet_id, range_a1=bad_range, exc=exc)
            raise
        return payload if isinstance(payload, dict) else {"raw": payload}

    @staticmethod
    def normalize_matrix(matrix: list[list[str]], rows: int, cols: int) -> list[list[str]]:
        out: list[list[str]] = []
        for r in range(rows):
            row = matrix[r] if r < len(matrix) else []
            out.append([str(row[c]).strip() if c < len(row) else "" for c in range(cols)])
        return out







