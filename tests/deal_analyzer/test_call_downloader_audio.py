from __future__ import annotations

import shutil
from pathlib import Path
from types import SimpleNamespace
from urllib.error import URLError
from unittest.mock import patch

from src.deal_analyzer.call_downloader import CallDownloader
from src.deal_analyzer.call_evidence import CallEvidence
from src.config import load_config


class _Logger:
    def info(self, *_args, **_kwargs):
        return None

    def warning(self, *_args, **_kwargs):
        return None


def _config(tmp_path: Path):
    return SimpleNamespace(
        call_collection_mode="raw_only",
        audio_cache_dir=str(tmp_path),
        amocrm_auth_config_path="",
        call_base_domain="",
    )


def _test_dir(name: str) -> Path:
    app = load_config()
    root = app.project_root / "workspace" / "tmp_tests" / "deal_analyzer" / name
    if root.exists():
        shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True, exist_ok=True)
    return root


def _call(*, url: str = "") -> CallEvidence:
    return CallEvidence(
        call_id="c1",
        deal_id="d1",
        manager_id="1",
        manager_name="manager",
        timestamp="",
        duration_seconds=12,
        direction="outbound",
        source_location="test",
        recording_url=url,
        recording_ref="ref1",
        quality_flags=[],
        missing_recording=not bool(url),
    )


def test_resolve_call_audio_download_success():
    tmp_path = _test_dir("call_audio_download_ok")
    downloader = CallDownloader(config=_config(tmp_path), logger=_Logger())
    call = _call(url="https://rec.example/c1.mp3")

    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self):
            return b"audio-bytes"

    with patch("src.deal_analyzer.call_downloader.urlopen", return_value=_Resp()):
        resolved = downloader._resolve_call_audio(call)

    assert resolved.audio_download_status == "downloaded"
    assert resolved.audio_source_url == "https://rec.example/c1.mp3"
    assert resolved.audio_path
    assert Path(resolved.audio_path).exists()


def test_resolve_call_audio_uses_cached_file_without_redownload():
    tmp_path = _test_dir("call_audio_cached")
    downloader = CallDownloader(config=_config(tmp_path), logger=_Logger())
    call = _call(url="https://rec.example/c1.mp3")
    target = downloader._build_target_audio_path(call=call, source_url=call.recording_url)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"cached-bytes")

    with patch("src.deal_analyzer.call_downloader.urlopen", side_effect=AssertionError("should not download")):
        resolved = downloader._resolve_call_audio(call)

    assert resolved.audio_download_status == "cached"
    assert resolved.audio_path == str(target)


def test_resolve_call_audio_broken_url_does_not_raise():
    tmp_path = _test_dir("call_audio_broken")
    downloader = CallDownloader(config=_config(tmp_path), logger=_Logger())
    call = _call(url="https://rec.example/c1.mp3")

    with patch("src.deal_analyzer.call_downloader.urlopen", side_effect=URLError("boom")):
        resolved = downloader._resolve_call_audio(call)

    assert resolved.audio_download_status == "failed"
    assert "download_request_failed" in resolved.audio_download_error
    assert resolved.audio_path == ""


def test_resolve_call_audio_missing_url_skips_cleanly():
    tmp_path = _test_dir("call_audio_missing")
    downloader = CallDownloader(config=_config(tmp_path), logger=_Logger())
    resolved = downloader._resolve_call_audio(_call(url=""))

    assert resolved.audio_download_status == "missing_url"
    assert resolved.audio_path == ""
