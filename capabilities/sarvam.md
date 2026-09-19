# Sarvam sweep: Sarvam AI vs llmgw (2026-09-19)

Repo: this worktree, on top of the Phase D/E/G voice work. Every row below
was measured live on 18 Sep 2026 (`capabilities/captures-sarvam-assemblyai.md`
§2) or re-measured on 19 Sep 2026 while the surfaces were built. Where the
two disagree, the 19 Sep reading wins and the row says so.

Status key, as in the other sweeps: **Shipped** (a route, a catalog row and a
test exist), **Proxyable today** (works through an existing path unchanged),
**Needs new transport**, **Not a gateway concern**.

## Verdict in one paragraph

Sarvam is the cheapest provider in the capabilities set to add and the most
awkward to bill. Cheap because `Authorization: Bearer <key>` is accepted
identically to the documented `api-subscription-key` header, so the existing
credential path carries it, and because its text model's
`/v1/chat/completions` is OpenAI-compatible down to the `data: [DONE]` --
one catalog row and no new code. Awkward because **not one of its four HTTP
speech responses reports a meter**, while the price list bills per character
and per audio second. The gateway therefore estimates both, from the request
text and from the uploaded WAV's own header, and every Sarvam speech record
carries `basis=estimated` plus a `cost_notes` line saying which estimate was
used. The streaming STT WebSocket *does* report `metrics.audio_duration`
exactly, so the meter exists inside Sarvam -- it is simply absent from the
HTTP products.

## What changed against the captures file

| Item | Captures said (18 Sep) | Live on 19 Sep | Consequence |
|---|---|---|---|
| Text model id | `sarvam-m` | **Gone.** `Model 'sarvam-m' has been deprecated. Please use one of the available models instead: sarvam-105b, sarvam-105b-conversations.` | The catalog row is `sarvam.sarvam-105b`. Planning around a model id is planning around something that can be retired between the probe and the build |
| `GET /v1/models` | not probed | **200, OpenAI-shaped**: `{"object":"list","data":[{"id":"sarvam-105b",…},{"id":"sarvam-105b-conversations",…}]}` | Sarvam is one of the few voice-adjacent providers `live/probe.py` can reconcile the catalog against |
| Chat streaming | open question | **SSE, OpenAI-identical**: `chat.completion.chunk` deltas, a usage-only final chunk (`choices: []`), then `data: [DONE]` | `OpenAIChatSurface` serves it with no new surface, no new metric name |
| Chat usage | open question | Present, and the final usage chunk arrives **whether or not `stream_options.include_usage` is sent** | Sarvam bills exactly on the text path, unlike every speech path |
| `saaras:v4` on `/speech-to-text-translate` | implied interchangeable | **400.** The translate route's model set is `'saaras:v2.5', 'saaras:v3', 'saaras:v1', 'saaras:v2', 'saaras:flash' or 'saaras:turbo'` -- narrower, and not a subset spelling of the transcription route's | A caller who assumes one model set finds out at the provider. The catalog cannot say "this model on that route"; the constraint is a comment on the `sarvam.saaras-v4` row |
| `bulbul:v4-flash` speakers | not probed | A **different speaker set** (`aayan_hi_conversational`, `amit_hi_conversational`, …); `shubh` is a `bulbul:v3` speaker and 400s on v4-flash | `speaker` is forwarded untouched; this is the caller's business and the gateway does not translate it |
| TTS stream chunking | 11 chunks, steady 11,000 B | 7 to 23 chunks, a ramp (12,576 / 2,796 / 28 / 11,000 …) | 11,000 B is typical, not a contract. The fake uses it as a default and says so |
| Unknown-model message | "enumerates the model set" | Three different spellings across three products -- see §4 | A single-substring classifier rule would have matched one of the three |

Everything else in the captures held: the base URL with no version prefix,
the 403 for a bad or missing credential, `'text' cannot be empty` as a 400,
the chunked `audio/pcm` TTS stream with no terminal frame, the one error
envelope, and the absence of any usage field anywhere on the speech
responses.

## 1. Products and endpoints

| capability | Sarvam | protocol/unit | llmgw fit | note |
|---|---|---|---|---|
| TTS buffered `POST /text-to-speech` | live | JSON in, JSON out: `{"request_id", "audios": [<base64 WAV>]}`. 16 kHz mono s16le for `speech_sample_rate: 16000`. 70 chars -> ~100-130 KB WAV, ~1.1-1.3 s wall from Chennai | **Shipped**: `sarvam_tts`, `POST /sarvam/text-to-speech`, buffered, model in the body key `model` | No usage field, no usage header |
| TTS chunked `POST /text-to-speech/stream` | live | Chunked `audio/pcm` (or `audio/mpeg` per `output_audio_codec`), no framing, **no terminal frame** -- the body ends when the connection does. TTFB ~0.34-0.38 s | **Shipped**: `sarvam_tts_stream`, `framing="raw"` | The gateway's "terminal event" concept has nothing to bind to, which `raw` framing already expresses |
| STT `POST /speech-to-text` | live | **multipart**, audio in a part named `file`, model in a form field `model`; JSON out: `{"request_id", "transcript", "language_code", "language_probability"}` | **Shipped**: `sarvam_stt`, `body="multipart"`, rewrite via `apply_api_model_multipart` | No duration, no usage. The model field is optional at Sarvam; the gateway requires it, because a request it cannot route is one it should not forward and let the provider bill |
| STT translate `POST /speech-to-text-translate` | live | Same request shape; response adds `diarized_transcript` (null unless diarization was asked for) | **Shipped**: `sarvam_stt_translate` | Its own metric name: its own line on Sarvam's price list, today at the same rate |
| Chat `POST /v1/chat/completions` | live | OpenAI-compatible: buffered and SSE, `reasoning_content` deltas, usage-only final chunk, `data: [DONE]` | **Proxyable today**, and shipped as a catalog row only (`sarvam.sarvam-105b`) | See §6 for the one accounting detail the shared surface misses |
| Models `GET /v1/models` | live | OpenAI-shaped list | **Proxyable today**; not routed (the gateway serves `/v1/models` from its own catalog) | Usable by `live/probe.py` |
| TTS WebSocket `wss://…/text-to-speech/ws` | captures 9a | 5 × `{"type":"audio","data":{…}}`; **the server never closes** | **Needs new transport** (PLAN-G plane) | Auth is rejected at the upgrade with a real HTTP 403 and a JSON body, so AssemblyAI's 1008-ambiguity problem does not arise here |
| STT WebSocket `wss://…/speech-to-text/ws` | captures 9b | `events/START_SPEECH`, `events/END_SPEECH`, then `type:"data"` with `metrics.audio_duration` | **Needs new transport** | The only Sarvam surface that reports its own meter. The final transcript is emitted on the VAD's END_SPEECH, not on the last audio frame (9c: speech with no trailing silence got no transcript at all) |
| Translate, transliterate, language id, document digitization, dubbing | price list | REST JSON | **Not a gateway concern** today | Priced per 10k characters / per page / per minute of media |

## 2. Framing

Nothing new was needed. The buffered routes are one JSON object; the stream
is `raw`, the framer Phase D added for ElevenLabs. The one thing worth
stating is what `raw` means here: the chunked TTS body has **no terminator at
all**, so `native_ending()` returns empty and the gateway appends nothing
after the last real byte (C20). A client reading the stream learns it is over
by the connection closing, exactly as it would talking to Sarvam directly.

## 3. Auth and headers

| item | finding |
|---|---|
| Documented header | `api-subscription-key: <key>` |
| Bearer | **Accepted identically** (captures probe 5; re-confirmed 19 Sep on `/v1/chat/completions`, both forms 200). `auth_scheme="bearer"`, the existing path |
| Bad key | **403** `invalid_api_key_error`, body `{"error":{"message":"Invalid or missing authentication credentials","code":"invalid_api_key_error","request_id":…}}` |
| Missing key | The same 403, byte for byte. Sarvam does not distinguish the two, so neither does the gateway |
| Credential reflection | **None** in twelve captured error bodies, key and 4-/8-character prefixes both checked. Hence `scrub_error_bodies="auth"` (the default) rather than Inworld's `"all"` |
| Request id | `x-request-id` on every response and `error.request_id` in every error body, both `<yyyymmdd>_<uuid>` |
| Rate-limit headers | None on any response |
| Server | `uvicorn` on everything |

## 4. Errors

One envelope for every product and every status:

```json
{"error": {"message": "<human string>", "code": "<machine code>", "request_id": "<id>"}}
```

Codes observed: `invalid_request_error`, `invalid_api_key_error`,
`not_found_error`. Documented but not observed:
`unprocessable_entity_error`, `insufficient_quota_error`,
`authentication_error`, `rate_limit_exceeded_error`,
`internal_server_error`.

The envelope's uniformity is the problem, not the convenience: **one code,
`invalid_request_error`, covers a malformed body, an unknown model and a
retired model**, so only the message separates the caller's fault from our
stale catalog. And the message is spelled three ways:

| product | message |
|---|---|
| TTS | `Validation Error(s):\n- model: Input should be 'bulbul:v2', 'bulbul:v3-beta', 'bulbul:v3' or 'bulbul:v4-flash'` |
| STT | `body.model : Input should be 'saarika:v2.5', 'saaras:v3', 'saaras:v3-realtime', 'saaras:v4', 'saaras:v4-multispk', 'saarika:v1', 'saarika:v2' or 'saarika:flash'` |
| chat | `body.model : Value error, Input 'sarvam-nope' should be one of sarvam-105b, sarvam-105b-conversations` |
| retired | `Model 'bulbul:v2' has been deprecated. Please use 'bulbul:v3' instead.` |

`errors._UNKNOWN_MODEL_HINTS` gained four entries for these, and
`tests/unit/test_real_error_bodies.py` carries all of them as literal
captured bytes plus the counterweight (`'text' cannot be empty` must stay
`InvalidRequest`).

One more rule came out of Sarvam: **a 404 whose `code` is `not_found_error`
is a routing fault, not a missing model.** `POST /text-to-speech:stream` (the
colon form) is a 404 and `POST /text-to-speech/stream` is a 200; the
difference is a typo in our surface, and the old rule -- "any API error
object at 404 means the model is not here" -- would have sent the executor
shopping the request around every fallback for a path none of them has, and
pointed the operator at a model id that was never wrong. The rule keys on
`code`, not `type`, because Anthropic spells its genuine unknown-model 404
with `error.type == "not_found_error"`.

## 5. Prices

Published in **INR only**, and for speech **by service, not by model**.
Sources: https://www.sarvam.ai/api-pricing and
https://docs.sarvam.ai/api/getting-started/pricing, both read 2026-09-19.

| service / model | INR | unit | USD at Rs 88.5/USD | catalog |
|---|---|---|---|---|
| Text to Speech (real-time and streaming) | Rs 3.00 per 1,000 characters | characters, "charged per character, rounded up" | **33.90 per 1M characters** | `sarvam.bulbul-v3`, `sarvam.bulbul-v4-flash` (`input_per_m`) |
| Speech to Text (real-time, streaming, batch) | Rs 30.00 per hour, "billed per second" | audio seconds | **0.00565 per minute** | `sarvam.saaras-v3`, `sarvam.saaras-v4` (`per_minute`) |
| Speech to Text & Translate | Rs 30.00 per hour | audio seconds | same | same rows |
| …with diarization | Rs 45.00 per hour | audio seconds | not encoded | no diarizing route is exposed |
| Sarvam 105B / 105B Conversations | Rs 29.28 / Rs 10.98 / Rs 73.20 per 1M tokens (in / cached in / out) | tokens | **0.3308 / 0.1241 / 0.8271** | `sarvam.sarvam-105b` |

Two caveats live in the catalog comment because the catalog has no field for
them:

1. **No per-model speech rate is published.** `bulbul:v3` and
   `bulbul:v4-flash` carry the same number, and so do `saaras:v3` and
   `saaras:v4`, because Sarvam prices the *service*. Two rows that agree are
   not a copy-paste slip and must not be "fixed" in isolation.
2. **The USD figures are a conversion.** They are only as current as
   Rs 88.5/USD (2026-09-18). `priced_at` records when the RUPEE figure was
   checked, so a move in INR/USD makes the stored dollars wrong while the
   date still looks fresh -- the "fresh and wrong" failure `catalog.py`'s own
   header describes. Re-derive on every price sweep, not only when Sarvam
   changes a number.

## 6. Usage and billing

| surface | what Sarvam reports | what the gateway records | basis |
|---|---|---|---|
| `sarvam_tts` | **nothing** | `characters = len(request text)` (or the joined `inputs` list) | `estimated`, note: "sarvam text-to-speech reports no usage on the wire; characters counted from the request text" |
| `sarvam_tts_stream` | **nothing** | the same | the same |
| `sarvam_stt` | **nothing** | `seconds` read from the uploaded RIFF/WAVE header, clamped to the bytes actually uploaded | `estimated`, note: "…seconds estimated from the uploaded WAV header" |
| `sarvam_stt_translate` | **nothing** | the same | the same |
| `sarvam_stt` on a compressed upload | nothing | **zero seconds** | `estimated`, note: "…the upload carried no readable WAV header; billed seconds are zero" |
| chat (`openai_chat`) | `usage` exactly, on a final streamed chunk | tokens, exact | `exact` |

### The speech-to-text hole, and the three ways not to fill it

Sarvam bills per audio second and returns no duration. Three options existed:

1. **Force `with_timestamps=true`** and bill from
   `max(timestamps.end_time_seconds)`. Rejected: it changes the request the
   caller made and the response they get. An exact number bought by sending
   something other than what was asked for is not exact, it is a different
   request's number.
2. **Record `seconds=0`, basis estimated, and move on.** Honest, and what
   happens for any upload the gateway cannot read.
3. **Read the duration out of the client's own container header.** Correct
   for WAV, impossible for compressed audio, and always an estimate: it is
   what the uploader *declared*, not what Sarvam metered.

The gateway does (3) where it can and (2) otherwise, and labels both. The
reader (`surfaces/voice/sarvam.py:wav_seconds`) runs on attacker-supplied
bytes on the request path, so it is a bounded walk over chunk headers that
cannot raise, is capped at 32 chunks and 64 KiB, refuses a zero byte rate,
and clamps a declared `data` size to the bytes actually present -- a header
claiming four hours inside a 10 KB upload bills as 10 KB of audio.
`tests/unit/test_sarvam_surfaces.py` fires nine hostile bodies at it.

The mechanism is two small hooks rather than special-casing Sarvam in the
server: `Surface.facts_from_body(facts, body, content_type)`, which may add
request-side billing detail to a non-JSON body's routing facts and cannot
change the routing key, and `Surface.cost_notes(facts, usage)`, whose lines
are prepended to the accounting record's own. Both are total; a bug in either
produces a less informative record, never a failed request.

### One thing the shared chat surface misses

Sarvam puts `reasoning_tokens` at the TOP of its `usage` object, not under
`completion_tokens_details` (which it sends as `null`). `surfaces/openai.py`
reads the nested spelling only, so a Sarvam chat record counts input and
output correctly and reports zero reasoning tokens -- for a model that spent
24 of 24 completion tokens thinking in the smoke run. Informational only
(reasoning is priced at the output rate and is already inside `output_tokens`,
so no bill is wrong), and deliberately not fixed here: changing a shared
surface's usage reader for one provider's spelling is a change that belongs
with its own evidence and its own test.

## 7. What llmgw needed, and what it did not

Needed nothing new for transport, framing, credentials or units: `raw`
framing, multipart forwarding, `characters`, `seconds`, and
`auth_scheme="bearer"` all predate Sarvam. What it did need:

- four surfaces and four `metrics.SURFACES` names;
- four `_UNKNOWN_MODEL_HINTS` entries and the 404 routing-fault rule;
- `VoiceRequestFacts.seconds`, and the two hooks above;
- five catalog rows and a provider row.

## 8. Known gaps

- **Both streaming products are WebSockets** (`/text-to-speech/ws`,
  `/speech-to-text/ws`) and wait on the PLAN-G plane. Neither socket closes
  itself; a relay must own the close and the drain must not wait for one.
- **No diarization route** is exposed, so the Rs 45/hour rate is not encoded.
- **No rate limit was provoked**, no 429 or 5xx body was observed, and there
  are no rate-limit headers to read. `max_concurrency=16` is a guess sized
  like the other voice rows.
- **`sarvam-105b-conversations`** is served and has no catalog row; only the
  model actually exercised was added.
- **Route-scoped model sets** (`saaras:v4` on transcription but not on
  translate) cannot be expressed in the catalog and are documented instead.

## Appendix: the live smoke, 19 Sep 2026

`LLMGW_ENV_FILE=… .venv/bin/python -m live.smoke_sarvam`, seven cases through
the gateway, speech-to-text fed by Sarvam's own text-to-speech output:

```
sarvam gateway on http://127.0.0.1:42083
  SARVAM_API_KEY: set
  sarvam-tts-sync: 200 wav=131116B riff=True t=1.280s served_by=sarvam/sarvam.bulbul-v3 provider_meter=none billed_characters=70 (estimated from 70 request chars) -> PASS
  sarvam-tts-stream: 200 audio/pcm chunks=21 bytes=103766 ttfb=0.335s served_by=sarvam/sarvam.bulbul-v3 billed_characters=70 (no terminal frame; body ends on close) -> PASS
  sarvam-stt: 200 t=0.559s transcript='The quick brown fox jumps over the lazy dog near the riverbank today.' served_by=sarvam/sarvam.saaras-v4 provider_meter=none billed_seconds=4 (estimated from the WAV header's 4.10s) -> PASS
  sarvam-stt-translate: 200 t=0.360s transcript='The quick brown fox jumps over the lazy dog near the riverbank today.' served_by=sarvam/sarvam.saaras-v3 provider_meter=none billed_seconds=4 (estimated from the WAV header's 4.10s) diarized=None -> PASS
  sarvam-chat: 200 text/event-stream; charset=utf-8 bytes=10263 done=True t=0.535s served_by=sarvam/sarvam.sarvam-105b provider_usage={'completion_tokens': 24, 'prompt_tokens': 16, 'total_tokens': 40, 'completion_tokens_details': None, 'prompt_tokens_details': None, 'reasoning_tokens': 24} billed_input=16 (openai_chat surface, no Sarvam-specific code) -> PASS
  sarvam-unrouted-model: 400 refused before any socket body='{"error": {"type": "policy_error", "message": "unknown model \'bulbul:v99\'"}}' -> PASS
  sarvam-model-not-found: 400 model_not_found_delta=1 body='{"error":{"message":"body.model : Input should be \'saaras:v2.5\', \'saaras:v3\', \'saaras:v1\', \'saaras:v2\', \'saaras:flash\' or \'saaras:turbo\'","code":"inva' -> PASS

FAILURES: 0
ESTIMATED SPEND: $0.005543
```

Total live spend across the probes and three smoke runs on 19 Sep 2026: about
USD 0.03.
