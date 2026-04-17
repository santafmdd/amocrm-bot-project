from pathlib import Path

from src.deal_analyzer.transcript_cache import TranscriptCache, build_transcript_cache_key


def test_transcript_cache_key_is_deterministic():
    call = {
        "call_id": "x1",
        "deal_id": "42",
        "recording_url": "https://REC.example/abc",
        "recording_ref": "z",
        "duration_seconds": 120,
        "timestamp": "2026-04-18T10:00:00+00:00",
    }
    key1 = build_transcript_cache_key(call)
    key2 = build_transcript_cache_key(dict(call))
    assert key1 == key2


def test_transcript_cache_roundtrip():
    cache_dir = Path(r"d:\AI_Automation\_tmp_transcript_cache_test")
    cache = TranscriptCache(cache_dir=cache_dir)
    key = "abc123"
    payload = {"transcript_text": "ok", "transcript_status": "ok"}
    cache.set(key, payload)
    loaded = cache.get(key)
    assert loaded == payload
