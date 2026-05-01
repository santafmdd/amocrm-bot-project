from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.config import load_config
from src.deal_analyzer.config import DealAnalyzerConfig
from src.deal_analyzer.evidence_sources.google_drive_links import (
    classify_google_drive_link_kind,
    extract_google_drive_links_from_text,
)
from src.deal_analyzer.evidence_sources.models import PresentationDiscoveryOptions
from src.deal_analyzer.evidence_sources.presentation_discovery import discover_presentation_evidence
from src.deal_analyzer.evidence_sources.presentation_transcriber import transcribe_presentation_link


class _Logger:
    def info(self, *_args, **_kwargs):
        return None

    def warning(self, *_args, **_kwargs):
        return None


def _cfg() -> DealAnalyzerConfig:
    return DealAnalyzerConfig(
        config_path=Path("config/deal_analyzer.local.json"),
        output_dir=Path("workspace/deal_analyzer"),
        score_weights={},
        analyzer_backend="rules",
        ollama_base_url="http://127.0.0.1:11434",
        ollama_model="qwen3.5:397b-cloud",
        ollama_timeout_seconds=60,
        style_profile_name="manager_ru_v1",
        transcription_backend="mock",
        period_live_refresh_enabled=False,
    )


def _tmp_dir(name: str) -> Path:
    root = load_config().project_root / "workspace" / "tmp_tests" / "evidence_sources" / name
    if root.exists():
        for child in sorted(root.rglob("*"), reverse=True):
            if child.is_file():
                child.unlink(missing_ok=True)
            elif child.is_dir():
                try:
                    child.rmdir()
                except Exception:
                    pass
    root.mkdir(parents=True, exist_ok=True)
    return root


def test_extract_google_drive_links_from_comment() -> None:
    text = (
        "Ссылка на демо: https://drive.google.com/file/d/1AbCdEfGhIJkLmNoP/view?usp=sharing "
        "и документ https://docs.google.com/document/d/1QwertyuiopASDFghjklzxcvbnm/edit"
    )
    links = extract_google_drive_links_from_text(text=text, source_field="comment")
    assert len(links) == 2
    kinds = {x.kind for x in links}
    assert "unknown" in kinds or "video" in kinds
    assert "doc" in kinds


def test_presentation_discovery_for_interest_stage_upgrade() -> None:
    run_dir = _tmp_dir("run_interest")
    rows = [
        {
            "deal_id": "101",
            "status_name": "Есть интерес",
            "company_comment": "Запись демо: https://drive.google.com/file/d/1AbCdEfGhIJkLmNoP/view",
            "notes_summary_raw": ["Клиент попросил демо и тест"],
        }
    ]
    raw = {
        "101": {
            "notes": [
                {
                    "text": "Видео встречи https://drive.google.com/file/d/1ZxCvBnMaSdFgHjKl/view",
                }
            ]
        }
    }
    call_ledger = [
        {
            "deal_id": "101",
            "call_id": "c1",
            "source_location": "amocrm_api:global_notes:notes_call_out",
            "entity_type": "lead",
        }
    ]
    logger = _Logger()
    with patch("src.deal_analyzer.evidence_sources.presentation_discovery.CallDownloader._make_api_client", return_value=(None, "", "", "missing")):
        out = discover_presentation_evidence(
            cfg=_cfg(),
            logger=logger,
            rows=rows,
            raw_bundles_by_deal=raw,
            call_ledger_all=call_ledger,
            period_start=date(2026, 4, 28),
            period_end=date(2026, 4, 28),
            options=PresentationDiscoveryOptions(
                include_presentations=True,
                presentation_link_fields="auto",
                presentation_transcribe_missing=False,
                max_presentation_files_per_run=5,
            ),
            run_dir=run_dir,
        )
    assert int(out["summary"]["presentation_links_found_count"]) >= 1
    assert int(out["summary"]["presentation_evidence_items_count"]) >= 2
    assert run_dir.joinpath("presentation_discovery_debug.json").exists()
    assert run_dir.joinpath("evidence_items.json").exists()


def test_presentation_evidence_added_to_call_review_contract() -> None:
    run_dir = _tmp_dir("run_contract")
    rows = [
        {
            "deal_id": "202",
            "status_name": "Демо",
            "company_comment": "Провели демонстрацию https://drive.google.com/file/d/2AbCdEfGhIJkLmNoP/view",
        }
    ]
    call_ledger = [{"deal_id": "202", "call_id": "c2", "source_location": "amocrm_api:global_notes:notes_call_out"}]
    with patch("src.deal_analyzer.evidence_sources.presentation_discovery.CallDownloader._make_api_client", return_value=(None, "", "", "missing")):
        out = discover_presentation_evidence(
            cfg=_cfg(),
            logger=_Logger(),
            rows=rows,
            raw_bundles_by_deal={},
            call_ledger_all=call_ledger,
            period_start=date(2026, 4, 29),
            period_end=date(2026, 4, 29),
            options=PresentationDiscoveryOptions(
                include_presentations=True,
                presentation_link_fields="auto",
                presentation_transcribe_missing=False,
                max_presentation_files_per_run=3,
            ),
            run_dir=run_dir,
        )
    update = out["deal_updates"]["202"]
    assert isinstance(update.get("evidence_items"), list)
    types = {str(x.get("evidence_type") or "") for x in update["evidence_items"] if isinstance(x, dict)}
    assert "phone_call" in types
    assert "comment_link" in types


def test_presentation_transcript_fallback() -> None:
    tmp_root = _tmp_dir("run_transcript")
    link_text = "https://drive.google.com/file/d/3AbCdEfGhIJkLmNoP/view"
    link = extract_google_drive_links_from_text(text=link_text, source_field="comment")[0]

    def _fake_transcribe(*, calls, config, logger):
        return [
            {
                "transcript_status": "ok",
                "transcript_error": "",
                "transcript_backend": "mock",
                "transcript_text": "Провели обучающую демонстрацию и зафиксировали следующий шаг.",
                "transcript_chars": 74,
            }
        ]

    with patch("src.deal_analyzer.evidence_sources.presentation_transcriber.transcribe_call_evidence", side_effect=_fake_transcribe), patch(
        "src.deal_analyzer.evidence_sources.presentation_transcriber.load_config",
        return_value=SimpleNamespace(project_root=tmp_root),
    ):
        payload = transcribe_presentation_link(link=link, config=_cfg(), logger=_Logger())
    assert payload["status"] == "ok"
    assert int(payload["transcript_chars"]) > 20


def test_demo_quality_fields_present() -> None:
    run_dir = _tmp_dir("run_quality")
    rows = [
        {
            "deal_id": "303",
            "status_name": "Есть интерес",
            "company_comment": (
                "Давайте вместе: откройте сервис, нажмите на сценарий и покажите как у вас сейчас. "
                "После блока фиксируем следующий шаг и дату теста. "
                "Ссылка https://drive.google.com/file/d/9AbCdEfGhIJkLmNoP/view"
            ),
        }
    ]
    with patch("src.deal_analyzer.evidence_sources.presentation_discovery.CallDownloader._make_api_client", return_value=(None, "", "", "missing")):
        out = discover_presentation_evidence(
            cfg=_cfg(),
            logger=_Logger(),
            rows=rows,
            raw_bundles_by_deal={},
            call_ledger_all=[],
            period_start=date(2026, 4, 30),
            period_end=date(2026, 4, 30),
            options=PresentationDiscoveryOptions(
                include_presentations=True,
                presentation_link_fields="auto",
                presentation_transcribe_missing=False,
                max_presentation_files_per_run=1,
            ),
            run_dir=run_dir,
        )
    items = out["evidence_items"]
    assert len(items) >= 1
    first = items[0]
    assert first["demo_format_detected"] in {"aggressive_pitch", "educational_demo", "unclear"}
    assert first["client_hands_on_detected"] in {"yes", "no", "unknown"}
    assert first["problem_discovery_before_demo"] in {"yes", "no", "unknown"}
    assert first["next_step_fixed_after_demo"] in {"yes", "no", "unknown"}
    assert isinstance(first["demo_quality_score_0_100"], int)
    assert isinstance(first["demo_coaching_hint"], str)
