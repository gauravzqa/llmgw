"""Voice over HTTP: the surfaces PLAN-2 Phase D adds.

`VOICE_SURFACES` is the tuple the route registry (`llmgw.surfaces.REGISTRY`)
appends: one instance per dialect NAME, because `name` is the closed metric
label and the registry is keyed by it. A surface with several client routes
answers `upstream_path_for(route)` so the server can map each to the
provider's path; a surface whose framing differs per route is the exception
the design does not have, so:

* `audio_speech` is registered with its binary framing (what every SDK and
  the LiveKit plugin send). Its SSE form (`stream_format: "sse"`) is
  `AUDIO_SPEECH_SSE`, the same route with the other framer, selected by
  `AudioSpeechSurface.framing_for(body)` once the server consults it.
* `elevenlabs_tts` is registered for the buffered and `/stream` routes (raw
  audio); `/stream/with-timestamps` (JSONL) is `ElevenLabsTimestampsSurface`,
  importable, unregistered.

Names are the closed metric vocabulary in `metrics.SURFACES`; adding a
surface here means adding its name there, in the same change.
"""

from __future__ import annotations

from llmgw.surfaces.base import Surface
from llmgw.surfaces.voice._base import VoiceRequestFacts, VoiceSurface
from llmgw.surfaces.voice.assemblyai_sync import AssemblyAISyncSurface
from llmgw.surfaces.voice.audio_speech import AudioSpeechSurface
from llmgw.surfaces.voice.audio_transcription import AudioTranscriptionSurface
from llmgw.surfaces.voice.elevenlabs_tts import (
    ElevenLabsTimestampsSurface,
    ElevenLabsTTSSurface,
)
from llmgw.surfaces.voice.inworld_tts import InworldTTSSurface

AUDIO_SPEECH: Surface = AudioSpeechSurface()                  # binary by default
AUDIO_SPEECH_SSE: Surface = AudioSpeechSurface(framing="sse")  # same route, per-request
AUDIO_TRANSCRIPTION: Surface = AudioTranscriptionSurface()      # + /translations
INWORLD_TTS: Surface = InworldTTSSurface()                      # /voice and /voice:stream
ELEVENLABS_TTS: Surface = ElevenLabsTTSSurface()                # buffered and /stream
ELEVENLABS_TTS_TIMESTAMPS: Surface = ElevenLabsTimestampsSurface()  # unregistered
ASSEMBLYAI_SYNC: Surface = AssemblyAISyncSurface()

VOICE_SURFACES: tuple[Surface, ...] = (
    AUDIO_SPEECH,
    AUDIO_TRANSCRIPTION,
    INWORLD_TTS,
    ELEVENLABS_TTS,
    ELEVENLABS_TTS_TIMESTAMPS,
    ASSEMBLYAI_SYNC,
)
"""What the registry mounts: five names, nine routes."""

VOICE_ROUTES: dict[str, Surface] = {
    route: surface for surface in VOICE_SURFACES for route in surface.routes
}
"""Client route template -> surface, the `build_app(extra_surfaces=)` shape."""

__all__ = [
    "ASSEMBLYAI_SYNC",
    "AUDIO_SPEECH",
    "AUDIO_SPEECH_SSE",
    "AUDIO_TRANSCRIPTION",
    "ELEVENLABS_TTS",
    "ELEVENLABS_TTS_TIMESTAMPS",
    "INWORLD_TTS",
    "VOICE_ROUTES",
    "VOICE_SURFACES",
    "AssemblyAISyncSurface",
    "AudioSpeechSurface",
    "AudioTranscriptionSurface",
    "ElevenLabsTTSSurface",
    "ElevenLabsTimestampsSurface",
    "InworldTTSSurface",
    "VoiceRequestFacts",
    "VoiceSurface",
]
