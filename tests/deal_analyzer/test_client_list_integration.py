from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from src.deal_analyzer.client_list.normalizer import (
    build_header_mapping,
    extract_amocrm_ids,
    normalize_client_rows,
)
from src.deal_analyzer.client_list.prioritizer import (
    build_manager_client_context,
    build_priority_summary,
    classify_priority_category,
)
from src.deal_analyzer.client_list.models import ClientListRow
from src.deal_analyzer.week_plan.cli import _inject_client_context_into_sales_manager_rows
from src.deal_analyzer.config import DealAnalyzerConfig


def _cfg() -> DealAnalyzerConfig:
    return DealAnalyzerConfig(
        config_path=Path("config/deal_analyzer.local.json"),
        output_dir=Path("workspace/deal_analyzer"),
        score_weights={},
        analyzer_backend="hybrid",
        ollama_base_url="http://127.0.0.1:11434",
        ollama_model="qwen3.5:397b-cloud",
        ollama_timeout_seconds=60,
        client_list_enabled=True,
        client_list_sheet_name="Клиентский список",
        manager_role_registry={"Илья Бочков": "sales_manager"},
    )


def test_client_list_header_mapping() -> None:
    headers = [
        "Менеджер",
        "Клиент",
        "Ссылка на сделку",
        "Статус",
        "Комментарий",
        "Потенциал",
        "Следующий шаг",
        "Дата следующего шага",
    ]
    cfg = SimpleNamespace(
        client_list_link_columns=("Ссылка на сделку",),
        client_list_status_columns=("Статус",),
        client_list_comment_columns=("Комментарий",),
        client_list_value_columns=("Потенциал",),
        client_list_next_step_columns=("Следующий шаг",),
    )
    mapped = build_header_mapping(headers, cfg=cfg)
    assert mapped["manager_name"] == 0
    assert mapped["client_name"] == 1
    assert mapped["deal_link"] == 2 or mapped["amocrm_link"] == 2
    assert mapped["status_text"] == 3
    assert mapped["comment_text"] == 4
    assert mapped["value_text"] == 5


def test_client_list_extract_amocrm_ids() -> None:
    ids = extract_amocrm_ids(
        links=[
            "https://officeistockinfo.amocrm.ru/leads/detail/12345",
            "https://officeistockinfo.amocrm.ru/contacts/detail/54321",
            "https://officeistockinfo.amocrm.ru/companies/detail/777",
        ]
    )
    assert ids["deal_id"] == "12345"
    assert ids["contact_id"] == "54321"
    assert ids["company_id"] == "777"


def test_client_list_priority_categories() -> None:
    invoice = ClientListRow(
        row_number=2,
        manager_name="Илья Бочков",
        client_name="Клиент A",
        deal_name="Сделка A",
        contact_name="",
        company_name="",
        status_text="Ожидание оплаты по счету",
        comment_text="КП отправлено",
        value_text="500000",
        value_amount=500000.0,
        next_step_text="Позвонить по оплате",
        next_step_date="2026-04-15",
        risk_stalled=False,
        amocrm_link="",
        deal_link="https://officeistockinfo.amocrm.ru/leads/detail/100",
        contact_link="",
        company_link="",
        deal_id="100",
        contact_id="",
        company_id="",
    )
    category, _reason = classify_priority_category(invoice)
    assert category == "invoice_to_payment"

    demo = ClientListRow(
        row_number=3,
        manager_name="Илья Бочков",
        client_name="Клиент B",
        deal_name="Сделка B",
        contact_name="",
        company_name="",
        status_text="Демо проведено",
        comment_text="Нужно перевести в тест",
        value_text="120000",
        value_amount=120000.0,
        next_step_text="Согласовать тест",
        next_step_date="2026-04-16",
        risk_stalled=False,
        amocrm_link="",
        deal_link="https://officeistockinfo.amocrm.ru/leads/detail/101",
        contact_link="",
        company_link="",
        deal_id="101",
        contact_id="",
        company_id="",
    )
    category2, _reason2 = classify_priority_category(demo)
    assert category2 == "demo_to_test"


def test_week_plan_sales_manager_uses_client_list_context() -> None:
    rows = [
        {
            "recipient": "Илья Бочков",
            "manager_role_profile": "менеджер по продажам",
            "plan_date": "2026-04-14",
            "what_i_do": "Проверяю CRM и заполняю поля.",
            "task_to_assign": "1. Развитие: провести разбор. 2. Коммерческий результат: проверить сделки. 3. Контроль: заполнить CRM.",
        }
    ]
    context = {
        "илья бочков": {
            "top_priority_items": [
                {
                    "priority_category": "invoice_to_payment",
                    "deal_id": "31228579",
                    "deal_link": "https://officeistockinfo.amocrm.ru/leads/detail/31228579",
                    "next_step_text": "Согласовать дату оплаты",
                }
            ]
        }
    }
    updated, debug = _inject_client_context_into_sales_manager_rows(rows=rows, client_context_by_manager=context, cfg=_cfg())
    assert debug["rows_touched"] == 1
    task = updated[0]["task_to_assign"]
    assert "invoice_to_payment" in task
    assert "https://officeistockinfo.amocrm.ru/leads/detail/31228579" in task
    assert "массовый обзвон" not in updated[0]["what_i_do"].lower()


def test_week_plan_sales_manager_tasks_target_commercial_stages() -> None:
    headers = ["Менеджер", "Клиент", "Ссылка на сделку", "Статус", "Комментарий", "Потенциал", "Следующий шаг", "Дата следующего шага"]
    rows = [
        ["Илья Бочков", "Клиент A", "https://officeistockinfo.amocrm.ru/leads/detail/500", "Тест открыт", "", "100000", "Довести до счета", "2026-04-14"],
        ["Илья Бочков", "Клиент B", "https://officeistockinfo.amocrm.ru/leads/detail/501", "Счет отправлен", "", "250000", "Дожать оплату", "2026-04-15"],
    ]
    mapped = build_header_mapping(headers, cfg=SimpleNamespace())
    normalized, rejected = normalize_client_rows(headers=headers, rows=rows, mapping=mapped, header_row_number=1)
    assert not rejected
    summary = build_priority_summary(normalized)
    assert summary["categories"].get("test_to_invoice", 0) >= 1
    assert summary["categories"].get("invoice_to_payment", 0) >= 1
    ctx = build_manager_client_context(
        rows=normalized,
        manager_name="Илья Бочков",
        period_start="2026-04-13",
        period_end="2026-04-17",
        manager_role_registry={"Илья Бочков": "sales_manager"},
    )
    joined = " ".join(ctx.summary_lines).lower()
    assert "test_to_invoice" in joined or "invoice_to_payment" in joined
