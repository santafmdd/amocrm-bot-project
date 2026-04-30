# Call Review Diarization Spike

## Goal
- Evaluate whether we can split call recordings by speakers and map roles (`manager` / `client`) without touching battle write-path.
- Keep this as R&D only. No integration into `real-write` in this step.

## Data Paths
- Existing run artifacts: `workspace/deal_analyzer/period_runs/<run>/deals/deal_*.json`
- Audio cache from snapshot call evidence (`audio_path`, `recording_url`)
- Existing faster-whisper timestamps from transcript artifacts

## Candidate Approaches
1. Two-channel audio
- If recording is stereo with agent/client separated by channel:
  - split channels (`ffmpeg -map_channel`)
  - run STT per channel
  - assign role directly by channel metadata or by greeting phrases
- Lowest-risk path when channel separation exists.

2. Mono audio + diarization model
- Use one of:
  - `pyannote.audio`
  - `whisperX` diarization pipeline
  - speechbrain diarization
- Pipeline:
  - diarization segments -> `SPEAKER_00/SPEAKER_01`
  - align with STT timestamps
  - merge into role-attributed transcript

3. Hybrid with existing faster-whisper segments
- Keep current STT as is.
- Add diarization timestamps and join on overlap.
- If overlap confidence is low, keep `speaker_unknown` instead of forced mapping.

## Role Mapping Heuristics (LLM post-step)
- Inputs:
  - diarized chunks with timestamps
  - transcript text
  - known manager name from call metadata
- Role assignment hints:
  - manager often opens with greeting + company intro
  - manager asks qualification questions
  - client gives objections/constraints
- Output contract:
  - `speaker_role_map`
  - `confidence`
  - `unresolved_segments`

## Risks
- Mono call quality/noise reduces diarization confidence.
- Speaker switches in short overlap fragments can create false role labels.
- Heavy diarization models may increase run time and GPU/CPU load.

## Minimal Safe PoC (not wired to active pipeline)
1. Take 5-10 recordings from one run.
2. Run diarization offline into separate artifact.
3. Build `diarization_eval.json` with:
  - speaker turns count
  - unresolved overlap ratio
  - manual spot-check notes
4. Decide go/no-go for production integration.

## Acceptance For Future Integration
- At least 80% segments get stable role mapping on sampled calls.
- No degradation of current call_review runtime safety gates.
- Feature flag only, default OFF.
