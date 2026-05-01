from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class GoogleDriveLink:
    url: str
    file_id: str
    kind: str
    source_field: str
    source_excerpt: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "file_id": self.file_id,
            "kind": self.kind,
            "source_field": self.source_field,
            "source_excerpt": self.source_excerpt,
        }


@dataclass(frozen=True)
class PresentationEvidenceItem:
    deal_id: str
    evidence_type: str
    source_location: str
    entity_type: str = "lead"
    call_id: str = ""
    contact_id: str = ""
    contact_url: str = ""
    link_url: str = ""
    link_kind: str = ""
    note_excerpt: str = ""
    transcript_text: str = ""
    transcript_status: str = ""
    transcript_error: str = ""
    transcript_chars: int = 0
    demo_format_detected: str = "unclear"
    client_hands_on_detected: str = "unknown"
    problem_discovery_before_demo: str = "unknown"
    next_step_fixed_after_demo: str = "unknown"
    demo_quality_score_0_100: int = 0
    demo_coaching_hint: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "deal_id": self.deal_id,
            "evidence_type": self.evidence_type,
            "source_location": self.source_location,
            "entity_type": self.entity_type,
            "call_id": self.call_id,
            "contact_id": self.contact_id,
            "contact_url": self.contact_url,
            "link_url": self.link_url,
            "link_kind": self.link_kind,
            "note_excerpt": self.note_excerpt,
            "transcript_text": self.transcript_text,
            "transcript_status": self.transcript_status,
            "transcript_error": self.transcript_error,
            "transcript_chars": int(self.transcript_chars),
            "demo_format_detected": self.demo_format_detected,
            "client_hands_on_detected": self.client_hands_on_detected,
            "problem_discovery_before_demo": self.problem_discovery_before_demo,
            "next_step_fixed_after_demo": self.next_step_fixed_after_demo,
            "demo_quality_score_0_100": int(self.demo_quality_score_0_100),
            "demo_coaching_hint": self.demo_coaching_hint,
        }


@dataclass(frozen=True)
class PresentationDiscoveryOptions:
    include_presentations: bool = False
    presentation_link_fields: str = "auto"
    presentation_transcribe_missing: bool = True
    max_presentation_files_per_run: int = 20

