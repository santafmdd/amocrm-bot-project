"""Google Sheets API client (read-focused) for layout discovery flows."""

from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path
from typing import Any


SPREADSHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"


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



class GoogleSheetsApiClient:
    """Small read-oriented wrapper around Google Sheets API v4."""

    def __init__(self, project_root: Path, logger: logging.Logger | None = None) -> None:
        self.project_root = project_root
        self.logger = logger or logging.getLogger("project")
        # Keep scope exactly aligned with the validated local test script (test_google_sheets_api.py).
        self.scopes = [SPREADSHEETS_SCOPE]

        credentials_path = os.getenv("GOOGLE_API_CREDENTIALS_FILE", "").strip()
        token_path = os.getenv("GOOGLE_API_TOKEN_FILE", "").strip()

        self.credentials_file = Path(credentials_path) if credentials_path else (project_root / "credentials.json")
        self.token_file = Path(token_path) if token_path else (project_root / "token.json")

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

    def build_service(self):
        Request, Credentials, InstalledAppFlow, build = self._load_google_libs()

        self.logger.info("oauth scopes requested: %s", self.scopes)

        creds = None
        token_scope_mismatch = False
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

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception as exc:
                    self.logger.warning(
                        "Token refresh failed for %s. OAuth re-auth may be required. error=%s",
                        self.token_file,
                        exc,
                    )
                    creds = None

            if not creds or not creds.valid:
                if not self.credentials_file.exists():
                    raise RuntimeError(
                        f"Google API credentials file not found: {self.credentials_file}. "
                        "Set GOOGLE_API_CREDENTIALS_FILE or place credentials.json in project root."
                    )
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

        return build("sheets", "v4", credentials=creds)

    def get_spreadsheet_metadata(self, spreadsheet_id: str) -> dict[str, Any]:
        service = self.build_service()
        return service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

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

    def get_values(self, spreadsheet_id: str, range_a1: str) -> list[list[str]]:
        service = self.build_service()
        payload = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=range_a1)
            .execute()
        )
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
        payload = (
            service.spreadsheets()
            .values()
            .batchGet(spreadsheetId=spreadsheet_id, ranges=ranges)
            .execute()
        )
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
        body = {
            "valueInputOption": "RAW",
            "data": data,
        }
        payload = (
            service.spreadsheets()
            .values()
            .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
            .execute()
        )
        return payload if isinstance(payload, dict) else {"raw": payload}

    @staticmethod
    def normalize_matrix(matrix: list[list[str]], rows: int, cols: int) -> list[list[str]]:
        out: list[list[str]] = []
        for r in range(rows):
            row = matrix[r] if r < len(matrix) else []
            out.append([str(row[c]).strip() if c < len(row) else "" for c in range(cols)])
        return out
