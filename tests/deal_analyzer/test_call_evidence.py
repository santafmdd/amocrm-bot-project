from src.deal_analyzer.call_evidence import deduplicate_calls, extract_calls_from_notes


def test_call_evidence_extract_and_normalize_from_notes():
    notes = [
        {
            "id": 1,
            "note_type": "call_out",
            "created_at": 1700000000,
            "responsible_user_id": 10,
            "params": {
                "uniq": "call-abc",
                "duration": 45,
                "link": "https://rec.example/1",
            },
        }
    ]
    users = {10: {"name": "????"}}
    calls = extract_calls_from_notes(notes=notes, deal_id=777, users_cache=users)

    assert len(calls) == 1
    call = calls[0]
    assert call.call_id == "call-abc"
    assert call.deal_id == "777"
    assert call.direction == "outbound"
    assert call.duration_seconds == 45
    assert call.recording_url == "https://rec.example/1"
    assert call.missing_recording is False


def test_call_evidence_dedup_prefers_recording_variant():
    notes = [
        {
            "id": 1,
            "note_type": "call_out",
            "created_at": 1700000000,
            "params": {"uniq": "same-id", "duration": 10},
        },
        {
            "id": 2,
            "note_type": "call_out",
            "created_at": 1700000010,
            "params": {"uniq": "same-id", "duration": 12, "link": "https://rec.example/2"},
        },
    ]
    calls = extract_calls_from_notes(notes=notes, deal_id=777)
    deduped = deduplicate_calls(calls)

    assert len(deduped) == 1
    assert deduped[0].recording_url == "https://rec.example/2"
