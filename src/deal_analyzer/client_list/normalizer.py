from __future__ import annotations

import re
from typing import Any

from ..daily_control.source_reader import clean_text, parse_date
from .models import ClientListRow


CLIENT_LIST_ALIASES: dict[str, tuple[str, ...]] = {
    "manager_name": ("Менеджер", "Ответственный", "Сотрудник", "Owner", "Кто ведет"),
    "client_name": ("Клиент", "Название клиента", "Контрагент", "Проект"),
    "deal_name": ("Сделка", "Название сделки", "Lead", "Лид"),
    "contact_name": ("Контакт", "Имя контакта", "ФИО"),
    "company_name": ("Компания", "Компания клиента", "Юр лицо"),
    "amocrm_link": ("amoCRM link", "amoCRM", "Ссылка amoCRM", "amo link", "Ссылка"),
    "deal_link": ("Deal link", "Ссылка на сделку", "Сделка ссылка"),
    "contact_link": ("Contact link", "Ссылка на контакт", "Контакт ссылка"),
    "company_link": ("Company link", "Ссылка на компанию", "Компания ссылка"),
    "status_text": ("Статус", "Стадия", "Этап", "Pipeline status"),
    "comment_text": ("Комментарий", "Комментарии", "Примечание", "Заметки"),
    "value_text": ("Сумма", "Потенциал", "Value", "Бюджет"),
    "next_step_text": ("Следующий шаг", "Next step", "План действия"),
    "next_step_date": ("Дата следующего шага", "Next step date", "Дата контакта"),
    "risk_stalled": ("Риск зависания", "Завис", "Просрочено", "Stalled"),
}

_DEAL_PATTERNS = (
    re.compile(r"/leads/detail/(\d+)", flags=re.IGNORECASE),
    re.compile(r"[?&](?:lead_id|deal_id)=(\d+)", flags=re.IGNORECASE),
)
_CONTACT_PATTERNS = (
    re.compile(r"/contacts/detail/(\d+)", flags=re.IGNORECASE),
    re.compile(r"[?&](?:contact_id)=(\d+)", flags=re.IGNORECASE),
)
_COMPANY_PATTERNS = (
    re.compile(r"/companies/detail/(\d+)", flags=re.IGNORECASE),
    re.compile(r"[?&](?:company_id)=(\d+)", flags=re.IGNORECASE),
)


def _norm(value: Any) -> str:
    return clean_text(value).lower()


def _index_by_headers(headers: list[str]) -> dict[str, int]:
    index: dict[str, int] = {}
    for idx, header in enumerate(headers):
        key = _norm(header)
        if key and key not in index:
            index[key] = idx
    return index


def _find_header_index(headers: list[str], probes: list[str]) -> int:
    by_norm = _index_by_headers(headers)
    for probe in probes:
        key = _norm(probe)
        if key in by_norm:
            return by_norm[key]
    for probe in probes:
        key = _norm(probe)
        for idx, header in enumerate(headers):
            if key and key in _norm(header):
                return idx
    return -1


def _merge_aliases(
    *,
    base: tuple[str, ...],
    cfg_values: list[str] | tuple[str, ...] | None,
) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in [*(cfg_values or []), *list(base)]:
        text = clean_text(value)
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def build_header_mapping(headers: list[str], *, cfg: Any | None = None) -> dict[str, int]:
    aliases = dict(CLIENT_LIST_ALIASES)
    if cfg is not None:
        aliases["amocrm_link"] = tuple(
            _merge_aliases(
                base=aliases["amocrm_link"],
                cfg_values=getattr(cfg, "client_list_link_columns", ()),
            )
        )
        aliases["status_text"] = tuple(
            _merge_aliases(
                base=aliases["status_text"],
                cfg_values=getattr(cfg, "client_list_status_columns", ()),
            )
        )
        aliases["comment_text"] = tuple(
            _merge_aliases(
                base=aliases["comment_text"],
                cfg_values=getattr(cfg, "client_list_comment_columns", ()),
            )
        )
        aliases["value_text"] = tuple(
            _merge_aliases(
                base=aliases["value_text"],
                cfg_values=getattr(cfg, "client_list_value_columns", ()),
            )
        )
        aliases["next_step_text"] = tuple(
            _merge_aliases(
                base=aliases["next_step_text"],
                cfg_values=getattr(cfg, "client_list_next_step_columns", ()),
            )
        )
    mapping: dict[str, int] = {}
    for field, probes in aliases.items():
        idx = _find_header_index(headers, list(probes))
        if idx >= 0:
            mapping[field] = idx
    return mapping


def _extract_first(text: str, patterns: tuple[re.Pattern[str], ...]) -> str:
    for pattern in patterns:
        match = pattern.search(text)
        if match:
            return clean_text(match.group(1))
    return ""


def extract_amocrm_ids(*, links: list[str]) -> dict[str, str]:
    joined = " ".join(clean_text(item) for item in links if clean_text(item))
    return {
        "deal_id": _extract_first(joined, _DEAL_PATTERNS),
        "contact_id": _extract_first(joined, _CONTACT_PATTERNS),
        "company_id": _extract_first(joined, _COMPANY_PATTERNS),
    }


def _parse_amount(value: str) -> float | None:
    text = clean_text(value)
    if not text:
        return None
    compact = text.replace(" ", "").replace("\u00a0", "").replace(",", ".")
    match = re.search(r"-?\d+(?:\.\d+)?", compact)
    if not match:
        return None
    try:
        return float(match.group(0))
    except (TypeError, ValueError):
        return None


def _is_stalled(status_text: str, comment_text: str, next_step_text: str) -> bool:
    probe = " ".join([_norm(status_text), _norm(comment_text), _norm(next_step_text)])
    markers = (
        "завис",
        "тишина",
        "нет ответа",
        "не отвечает",
        "просроч",
        "без даты",
        "перенес",
        "stalled",
        "paused",
    )
    return any(marker in probe for marker in markers)


def normalize_client_rows(
    *,
    headers: list[str],
    rows: list[list[str]],
    mapping: dict[str, int],
    header_row_number: int,
) -> tuple[list[ClientListRow], list[dict[str, Any]]]:
    normalized: list[ClientListRow] = []
    rejected: list[dict[str, Any]] = []

    def _pick(row: list[str], field: str) -> str:
        idx = mapping.get(field, -1)
        if idx < 0 or idx >= len(row):
            return ""
        return clean_text(row[idx])

    for idx, raw in enumerate(rows):
        if not isinstance(raw, list):
            continue
        row_number = int(header_row_number + idx + 1)
        manager_name = _pick(raw, "manager_name")
        client_name = _pick(raw, "client_name")
        deal_name = _pick(raw, "deal_name")
        contact_name = _pick(raw, "contact_name")
        company_name = _pick(raw, "company_name")
        status_text = _pick(raw, "status_text")
        comment_text = _pick(raw, "comment_text")
        value_text = _pick(raw, "value_text")
        next_step_text = _pick(raw, "next_step_text")
        next_step_date = parse_date(_pick(raw, "next_step_date"))
        risk_raw = _pick(raw, "risk_stalled")
        amocrm_link = _pick(raw, "amocrm_link")
        deal_link = _pick(raw, "deal_link")
        contact_link = _pick(raw, "contact_link")
        company_link = _pick(raw, "company_link")
        links = [amocrm_link, deal_link, contact_link, company_link]
        ids = extract_amocrm_ids(links=links)
        if not deal_link and ids.get("deal_id"):
            deal_link = f"https://officeistockinfo.amocrm.ru/leads/detail/{ids.get('deal_id')}"
        if not contact_link and ids.get("contact_id"):
            contact_link = f"https://officeistockinfo.amocrm.ru/contacts/detail/{ids.get('contact_id')}"
        if not company_link and ids.get("company_id"):
            company_link = f"https://officeistockinfo.amocrm.ru/companies/detail/{ids.get('company_id')}"

        if not any(
            [
                client_name,
                deal_name,
                contact_name,
                company_name,
                amocrm_link,
                deal_link,
                contact_link,
                company_link,
                status_text,
                comment_text,
                next_step_text,
            ]
        ):
            rejected.append({"row_number": row_number, "reason": "row_empty"})
            continue

        risk_stalled = _is_stalled(status_text, comment_text, next_step_text) or _norm(risk_raw) in {"да", "yes", "true", "1"}
        normalized.append(
            ClientListRow(
                row_number=row_number,
                manager_name=manager_name,
                client_name=client_name,
                deal_name=deal_name,
                contact_name=contact_name,
                company_name=company_name,
                status_text=status_text,
                comment_text=comment_text,
                value_text=value_text,
                value_amount=_parse_amount(value_text),
                next_step_text=next_step_text,
                next_step_date=next_step_date,
                risk_stalled=risk_stalled,
                amocrm_link=amocrm_link,
                deal_link=deal_link,
                contact_link=contact_link,
                company_link=company_link,
                deal_id=ids.get("deal_id", ""),
                contact_id=ids.get("contact_id", ""),
                company_id=ids.get("company_id", ""),
            )
        )
    return normalized, rejected

