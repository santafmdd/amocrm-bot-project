import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.domain.refusal_status_normalizer import (
    canonicalize_after_status,
    canonicalize_before_status,
    canonicalize_refusal_reason,
    format_grouped_status_display,
    normalize_basic_text,
    parse_group_and_reason,
)


def test_normalize_basic_text_unifies_spaces_yo_and_punctuation() -> None:
    assert normalize_basic_text("  Перестал   выходить  на связь!!! ") == "перестал выходить на связь"
    assert normalize_basic_text("Ёжик") == "ежик"


def test_alias_mapping_merges_known_refusal_duplicates() -> None:
    variants = [
        "перестал выходить на свя",
        "перестал выходить на связ",
        "перестал выходит",
        "перестал выходить на связь",
    ]
    canonical = {canonicalize_refusal_reason(v) for v in variants}
    assert canonical == {"перестал выходить на связь"}


def test_format_grouped_status_display_uses_canonical_full_reason() -> None:
    group = "Проведена демонстрация"
    reason = "Перестал выходить на связ"
    label = format_grouped_status_display(group, reason)
    assert label == "(Проведена демонстрация) Перестал выходить на связь"


def test_canonicalize_after_status_preserves_group_and_canonical_reason() -> None:
    value = "(Верификация) Перестал выходить на связ"
    assert canonicalize_after_status(value) == "(верификация) перестал выходить на связь"


def test_parse_group_and_reason_handles_slash_format() -> None:
    group, reason = parse_group_and_reason("Верификация / Перестал выходить на свя")
    assert group == "верификация"
    assert reason == "перестал выходить на связь"


def test_canonicalize_before_status_stays_deterministic() -> None:
    assert canonicalize_before_status("  Первичный контакт ") == "первичный контакт"
