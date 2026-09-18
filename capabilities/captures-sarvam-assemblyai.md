# Live captures: Sarvam and AssemblyAI (2026-09-18)

Every row below is a real call made on 18 Sep 2026 from Chennai with the keys
in the repo's gitignored `.env`, loaded by variable reference only. Scripts and
raw captures are in the session scratchpad under `sarvam-aai/` (`common.py`,
`a1_async.py` … `a5_ws.py`, `s1_tts.py` … `s4_ws.py`, `out/*.json`); every saved
file and every body quoted here was passed through a scrubber that replaces both
keys and their 4- and 8-character prefixes. Neither provider echoed a credential
fragment in any error body.

Audio: one Sarvam TTS sentence, WAV/PCM16 mono 16 kHz — `probe16k.wav`
("Hello from the gateway probe.", 1.28 s, 41,004 B) and `long16k.wav`
("The quick brown fox jumps over the lazy dog near the river bank today.",
3.584 s, 114,732 B). Sarvam's own TTS generated both, so no third provider
(and no `OPENAI_API_KEY`) was used anywhere in this sweep.

Total spend: **about USD 0.03** (see §3).

This file settles PLAN-G G4's open questions on AssemblyAI streaming, and it
contradicts `capabilities/voice-assemblyai.md` and the `assemblyai-sync`
catalog row on three points — see §4.

---

## 1. AssemblyAI

Three hosts, one key, one credential style: **`authorization: <raw key>`, no
`Bearer`** — confirmed on all three hosts. `auth_scheme="raw"` is correct.

### 1.1 Probe table

| # | probe | request | status | key response fields | latency |
|---|---|---|---|---|---|
| A1a | upload | `POST api.assemblyai.com/v2/upload`, binary WAV 41,004 B, `content-type: application/octet-stream` | 200 | `upload_url` (a `cdn.assemblyai.com/upload/…` URL) | 1,271 ms |
| A1b | submit | `POST /v2/transcript` JSON `{"audio_url": …}` | 200 | `id`, `status:"processing"`, `audio_duration:null`, `speech_models:["universal-3-5-pro","universal-2"]`, `speech_model_used:null` | 756 ms |
| A1c | poll ×2 | `GET /v2/transcript/{id}` every 1 s | 200, 200 | poll 1 @757 ms `processing`; poll 2 @2,580 ms `completed`, `text`, `audio_duration:2`, `speech_model_used:"universal-3-5-pro"` | 817 ms each; **2.58 s wall from submit to completed** |
| A2a | sync, raw PCM to `/transcribe` (what the catalog surface sends) | `POST sync.assemblyai.com/transcribe`, `application/octet-stream`, no `X-AAI-Model` | **404** | `Not found` (text/plain, `server: awselb/2.0`) | 627 ms |
| A3a | **sync, documented form** | `POST sync.assemblyai.com/v1/transcribe`, `X-AAI-Model: universal-3-5-pro`, multipart part `audio` (WAV) | **200** | `text`, `words[]` (`text`+`confidence`, no timings), `confidence`, **`audio_duration_ms:1280`**, `session_id`, `request_time_ms:435.6` | 1,002 ms |
| A3b | sync at `/transcribe` (no `/v1`) | same, path `/transcribe` | 200 | identical body | 1,019 ms |
| A3c | sync, raw PCM + `config` part | `audio=@s.pcm;type=audio/pcm` + `config={"sample_rate":16000,"channels":1}` | 200 | identical body | 1,021 ms |
| A4a | sync, multipart, **no** `X-AAI-Model` | as A3a minus the header | **404** | `Not found` (text/plain, `awselb/2.0`) | 545 ms |
| A4d | sync, `X-AAI-Model: no-such-model` | | **404** | `Not found` (text/plain, `awselb/2.0`) | 565 ms |
| A4e | sync, `X-AAI-Model: universal-2` | | **404** | `Not found` (text/plain, `awselb/2.0`) | 514 ms |
| A4b/c | sync, raw body + valid `X-AAI-Model` | `application/octet-stream` / `audio/wav` | **415** | `application/problem+json`: `{"status","title","detail"}` | 555 ms |
| A4f | sync, multipart part named `file` | | 400 | `{"status":400,"title":"Bad Request","detail":"request must include an \`audio\` file part"}` | 1,021 ms |
| A4g | sync, **bad key** | 32 zeros in `authorization` | **404** | `application/problem+json` `{"status":404,"title":"Not Found","detail":"Invalid API key"}` | 562 ms |
| A4h | sync, empty audio part | | 400 | `{"status":400,"title":"Bad Audio","detail":"truncated WAV: "}` | 546 ms |
| A7a | sync `/v1/warm` POST | | 405 | `{"status":405,"title":"Method Not Allowed",…}` — the route exists, POST is not its verb | 656 ms |
| A6a | mint token | `GET streaming.assemblyai.com/v3/token?expires_in_seconds=600` | 200 | `{"token":"<2642 chars>","expires_in_seconds":600}` | 1,224 ms |
| A6b | legacy token | `POST api.assemblyai.com/v2/realtime/token` | **404** | `Not found` — the v2 realtime token endpoint is gone | 730 ms |
| A6c | mint capped token | `…/v3/token?expires_in_seconds=120&max_session_duration_seconds=300` | 200 | `{"token":…,"expires_in_seconds":120}` | 1,205 ms |
| A6d/e | TTS hunt | `POST api.assemblyai.com/v2/tts`, `/v1/speech` | 404, 404 | `Not found` | — |
| A7b | REST bad key | `GET /v2/transcript?limit=1`, 32 zeros | **401** | `{"error":"Authentication error, API token missing/invalid"}` | 802 ms |
| A7c | REST empty submit | `POST /v2/transcript` `{}` | 400 | `{"error":"Transcript creation error, audio_url not found"}` | 719 ms |
| A7d | REST unknown model | `{"audio_url":…, "speech_model":"no-such-model"}` | 400 | `{"error":"speech_model is deprecated. Use \"speech_models\" instead. …"}` | 774 ms |

### 1.2 Streaming WebSocket

`wss://streaming.assemblyai.com/v3/ws?sample_rate=16000`. 36 × 100 ms binary
frames (3,200 B) of 16 kHz mono PCM16, paced in real time, then
`{"type":"Terminate"}`. Times are ms since `connect()`.

| # | probe | upgrade | server frames | close |
|---|---|---|---|---|
| A5a | header auth, happy | **101 @600 ms** | `Begin` @1,264; 17 partial `Turn` (`end_of_turn:false`); 1 final `Turn` (`end_of_turn:true`) @7,207; an empty `turn_order:1` `Turn` @7,635; `Termination` @7,945 | server 1000 `Session Ended` |
| A5b | **bad key** (32 zeros) in `authorization` | **101 @589 ms** | `Error` @602 (13 ms after 101): `{"type":"Error","error_code":1008,"error":"Unauthorized Connection: Invalid API key"}` | **server 1008** `See Error message for details` @784 |
| A5c | **no `authorization` header** | **101 @605 ms** | `Error` @609: `…"error":"Unauthorized Connection: Missing Authorization header"` | server 1008, same reason |
| A5d | **bad `?token=notatoken`** | **101 @559 ms** | `Error` @571: `…"error":"Unauthorized Connection: Invalid API key"` | server 1008, same reason |
| A5e | valid `?token=` (from A6c) | 101 @578 | identical sequence to A5a; `Termination` @7,864 | server 1000 `Session Ended` |
| A5f | **the same token a second time** | 101 @632 | `Begin` @1,321 — **accepted** | none in 8 s; client closed |

**Auth is checked after the 101, never before.** Every bad-credential case
upgraded successfully and then got a text `Error` frame followed by close
**1008**. There is no HTTP status to read: a gateway must classify on the
`Error` frame body, because 1008 is also the too-many-sessions code. The error
text contains no key fragment.

**Session cap, measured.** `Begin.expires_at` on the header-auth session
(A5a, 14:31:22Z) was `1789752682` = **17:31:22Z — exactly 3 h**. On the
token session minted with `max_session_duration_seconds=300` (A5e, 14:32:53Z)
it was `1789742273` = **14:37:53Z — 5 min**. So `max_session_duration_seconds`
really is the lever that reconciles a 3-hour session against
`LLMGW_DRAIN_GRACE`, and it is now verified rather than assumed.

**Tokens are not one-time.** `voice-assemblyai.md` §4 says "One-time-use
token". A5f reused A5e's token and got a clean second `Begin`.

**`Begin.configuration.model` is `universal-streaming-english`** with no model
in the query — that is the streaming default, not `universal-3-5-pro`.

### 1.3 Verbatim bodies

`Begin` (A5a):

```json
{"type":"Begin","id":"aa006a1d-d478-4827-948a-4067d1030530","expires_at":1789752682,
 "configuration":{"model":"universal-streaming-english","mode":null,"api_version":"2025-05-12",
 "speaker_labels":false,"redact_pii":false,"filter_profanity":false,"domain":null,"voice_focus":null}}
```

A partial `Turn` (note: **no `type` field first — `type` is the LAST key**):

```json
{"turn_order":0,"turn_is_formatted":false,"end_of_turn":false,"transcript":"the quick",
 "end_of_turn_confidence":0.000036,
 "words":[{"start":560,"end":640,"text":"the","confidence":0.843129,"word_is_final":true},
          {"start":720,"end":800,"text":"quick","confidence":0.478606,"word_is_final":false}],
 "utterance":"","type":"Turn"}
```

The final `Turn` and the terminal frame:

```json
{"turn_order":0,"turn_is_formatted":false,"end_of_turn":true,
 "transcript":"the quick brown fox jumps over the lazy dog near the riverbank today",
 "end_of_turn_confidence":0.630866,"words":[…],"utterance":"","type":"Turn"}
{"turn_order":1,"turn_is_formatted":false,"end_of_turn":true,"transcript":"",
 "end_of_turn_confidence":0.265551,"words":[],"utterance":"","type":"Turn"}
{"type":"Termination","audio_duration_seconds":4,"session_duration_seconds":7}
```

`turn_is_formatted` was `false` on every frame: no `Turn` with punctuation and
casing ever arrived, because `format_turns` was not requested. A consumer that
waits for a formatted turn waits forever by default.

Sync happy path (A3a), the whole body:

```json
{"text": "Hello from the Gateway Probe.",
 "words": [{"text": "Hello", "confidence": 0.9903581667059645},
           {"text": "from", "confidence": 0.9929906191680624},
           {"text": "the", "confidence": 0.9621412018225779},
           {"text": "Gateway", "confidence": 0.990933989469842},
           {"text": "Probe.", "confidence": 0.8517905310139084}],
 "confidence": 0.9576429016360711,
 "audio_duration_ms": 1280,
 "session_id": "8c3fb9a0-7719-458a-9b7d-84f541800c25",
 "request_time_ms": 435.59588899370283}
```

Async completed transcript (A1c, the billing-relevant subset of a 2,630-byte body):

```json
{"id":"196bc2da-5f64-4b60-8ea7-190698945491","status":"completed",
 "text":"Hello from the Gateway Probe.","confidence":0.9773134,
 "language_code":"en_us","audio_duration":2,"throttled":false,
 "speech_models":["universal-3-5-pro","universal-2"],
 "speech_model_used":"universal-3-5-pro"}
```

Sync errors, all `application/problem+json`:

```json
{"status": 415, "title": "Unsupported Media Type", "detail": "request must be multipart/form-data with an `audio` part and an optional `config` part"}
{"status": 400, "title": "Bad Request", "detail": "request must include an `audio` file part"}
{"status": 400, "title": "Bad Audio", "detail": "truncated WAV: "}
{"status": 404, "title": "Not Found", "detail": "Invalid API key"}
{"status": 405, "title": "Method Not Allowed", "detail": "Method Not Allowed"}
```

REST errors, `application/json`:

```json
{"error": "Authentication error, API token missing/invalid"}
{"error": "Transcript creation error, audio_url not found"}
{"error": "speech_model is deprecated. Use \"speech_models\" instead. See documentation: https://www.assemblyai.com/docs/pre-recorded-audio/select-the-speech-model"}
```

### 1.4 The three findings that matter

**(a) A synchronous transcribe endpoint DOES exist.** Answering the question
definitively: **yes**. `POST https://sync.assemblyai.com/v1/transcribe` (and
`/transcribe`, which is an alias) returns a finished transcript in one
response, 1.0 s wall for 1.28 s of audio, `request_time_ms` 436. The product
page (https://www.assemblyai.com/products/sync-speech-to-text, read 2026-09-18)
and `assemblyai.com/docs/llms.txt` both list it, with `/v1/transcribe`,
`/v1/transcribe-live` and `/v1/warm`.

**(b) `X-AAI-Model` is a ROUTING header, and that is why we saw a 404.** The
404 is emitted by the AWS load balancer, not the app: `server: awselb/2.0`,
`content-type: text/plain`, body `Not found`. Present a recognised
`X-AAI-Model` and the request reaches uvicorn; omit it (A4a), misspell it
(A4d), or ask for a model sync does not serve — `universal-2` (A4e) — and the
ELB answers 404 before any application code runs. The current surface sends no
such header, so it can never have worked. **The model lives in a header, not
in the body and not in the query string**, which no existing surface's
`model_key` can express.

**(c) A bad key on the sync host is a 404, not a 401 or 403.**
`{"status":404,"title":"Not Found","detail":"Invalid API key"}`, this one from
uvicorn with `application/problem+json`. So on `sync.assemblyai.com` a 404 is
ambiguous between "wrong model routing" and "revoked credential", separable
only by the content-type and the `detail` string. On `api.assemblyai.com` a bad
key is a plain **401** (A7b) — the 403-means-rate-limit rule the catalog row
declares was not exercised here and remains unverified live.

**(d) TTS: no.** AssemblyAI has no text-to-speech product. `assemblyai.com/docs/llms.txt`
(read 2026-09-18) lists eight products — Pre-recorded STT, Real-time STT, Sync
STT, Dictation, Voice Agent API, Speech Understanding, Guardrails, LLM Gateway
— and no speech synthesis; TTS exists only as a bundled leg inside the Voice
Agent API, not as an addressable endpoint. `POST /v2/tts` and `POST /v1/speech`
both 404.

**(e) Billing units differ per product, and both round.** Sync reports
`audio_duration_ms: 1280` — exact milliseconds. Async reports
`audio_duration: 2` for the same 1.28 s file — **integer seconds, rounded
up**. Streaming reports `Termination.audio_duration_seconds: 4` and
`session_duration_seconds: 7` for 3.58 s of audio in an 8 s socket — integer
seconds again, and the billed number is the session one.

### 1.5 What the gateway would need — AssemblyAI

**`ProviderConn` rows.** Keep `assemblyai` (REST) and `assemblyai-streaming`
as they are. Replace the `assemblyai-sync` row's base URL:

```python
"assemblyai-sync": ProviderConn(
    id="assemblyai-sync", kind="openai",
    base_url="https://sync.assemblyai.com",     # unchanged and correct
    api_key_env="ASSEMBLYAI_API_KEY", credential_id="assemblyai",
    auth_scheme="raw",                          # verified live
    forbidden_means="auth",                     # NOT rate_limit: this host 404s a bad key and never 403s
    scrub_error_bodies="auth",                  # no credential reflection observed; "auth" is enough
    max_concurrency=16,
)
```

`path_prefix="/v1"` is optional — `/transcribe` and `/v1/transcribe` both
answer 200 — but pinning `/v1` is the documented form and costs nothing.

**`ModelSpec` rows.**

| id | provider | api_model | unit | price | source |
|---|---|---|---|---|---|
| `assemblyai.sync` | `assemblyai-sync` | `universal-3-5-pro` | `seconds` | `per_minute=0.0075` ($0.45/h) | https://www.assemblyai.com/products/sync-speech-to-text, 2026-09-18 |
| `assemblyai.streaming` | `assemblyai-streaming` | `universal-streaming-english` | `seconds` | `per_minute=0.0025` ($0.15/h of **session wall time**) | https://www.assemblyai.com/pricing, 2026-09-18 |
| `assemblyai.universal-3-5-pro-realtime` | `assemblyai-streaming` | `universal-3-5-pro` | `seconds` | `per_minute=0.0075` ($0.45/h session) | same |
| `assemblyai.universal-3-5-pro` (new, async) | `assemblyai` | `universal-3-5-pro` | `seconds` | `per_minute=0.0035` ($0.21/h of audio) | same |
| `assemblyai.universal-2` (new, async) | `assemblyai` | `universal-2` | `seconds` | `per_minute=0.0025` ($0.15/h of audio) | same |

All three existing rows' prices are still correct at 2026-09-18; only
`assemblyai.sync`'s `api_model` is load-bearing now, because it must be
emitted as `X-AAI-Model`.

**Surfaces.**

*Sync STT — buildable, with one new mechanism.*
route `/assemblyai/v1/transcribe` → upstream `POST /v1/transcribe`; body kind
**`multipart`** (not `raw`); framing `raw` (one buffered JSON object);
**the model id goes in the `X-AAI-Model` request header** — the gateway reads
`?model=` or `X-Gw-Model` from the client, resolves the catalog row, and
writes `spec.api_model` into `X-AAI-Model` upstream; usage is
`audio_duration_ms / 1000` → `usage.seconds`, exact. Per-surface body cap
40 MB. The only genuinely new thing is **"the model is a header"**: today a
surface's `model_key` names a JSON body key (`model`, `modelId`), and
`assemblyai_sync.py` already abandoned body parsing for `?model=`/`X-Gw-Model`
on the client side — it just never wrote the value anywhere upstream.

*Streaming STT — needs the Phase G WebSocket plane, and nothing else.* The
event vocabulary maps cleanly: `Begin` = commitment and first event,
`Turn` with `end_of_turn:false` = CONTENT/progress, `Turn` with
`end_of_turn:true` = CONTENT, `Termination` = TERMINAL carrying the meter,
`Error` = ERROR. Two rules that are provider-specific and now measured:
classify on the `Error` frame body, never on close code 1008 alone; and treat
`Termination.session_duration_seconds` as the bill, falling back to the
gateway's own socket clock with `cost_basis=estimated` when the socket dies
first. `type` is the last key in a `Turn` object, so a classifier that peeks
at a prefix of the frame will miss it.

*Token mint — a plain buffered JSON route.* `GET /assemblyai/v3/token` →
`GET streaming.assemblyai.com/v3/token`, query forwarded, with the gateway
forcing `max_session_duration_seconds` ≤ `LLMGW_DRAIN_GRACE`. This is now
verified to actually cap the session (§1.2). It fits the current surface model
with no changes at all and is the highest-leverage AssemblyAI route.

**What does NOT fit the surface model.** The async flow —
`POST /v2/upload` → `POST /v2/transcript` → poll `GET /v2/transcript/{id}` —
**cannot be a passthrough surface, and should not be attempted as one.** Three
separate reasons, any one of which is fatal:

1. *It is three client-visible round trips with client-held state.* A surface
   is one request to one upstream path. The gateway could proxy each of the
   three legs as its own trivial buffered route, but then it is not fronting
   "transcription", it is fronting three unrelated REST calls, and no single
   request has a cost.
2. *Usage arrives on a leg that carries no model decision.* `audio_duration`
   appears only on the terminal poll. Whichever leg the gateway prices, the
   poll that finally carries the number is a `GET` the client may never make
   (webhook path) or may make fifty times (1 s polling). Accounting would
   either double-bill or never bill.
3. *The poll loop fights every budget.* `LLMGW_BUDGET_TOTAL` defaults to 120 s;
   an async job queued behind the account's FIFO parallel-job limit can sit for
   minutes. A proxied poll loop also burns one admission slot per poll.

What it would need instead is a **job-shaped route kind** the gateway does not
have: submit-returns-a-handle, gateway-owned polling or a gateway-owned webhook
receiver, a durable per-job record that the cost is written to when the
terminal status arrives, and budgets in the minutes-to-hours range decoupled
from the HTTP request that created the job. That is a second program, on the
scale of the WebSocket plane. **Until it exists, the sync endpoint is the only
AssemblyAI batch STT the gateway can front** — which is now a much better
answer than it was this morning, because the sync endpoint turns out to work.

---

## 2. Sarvam

Base URL `https://api.sarvam.ai`, no version prefix. `server: uvicorn` on every
response; `x-request-id` on every response and `error.request_id` in every error
body, both in the form `20260918_<uuid>`.

**Auth: both forms work.** The documented header is `api-subscription-key:
<key>`. **`Authorization: Bearer <key>` is accepted identically** (probe 10d,
200 with a normal transcript). Bad or missing credential is **403**
`invalid_api_key_error` on HTTP, and a **403 on the WebSocket upgrade itself**
— Sarvam rejects before the 101, the opposite of AssemblyAI.

### 2.1 Probe table

| # | probe | request | status | key response fields | latency |
|---|---|---|---|---|---|
| 5 | auth, bearer form | `POST /speech-to-text` with `Authorization: Bearer <key>` | 200 | normal transcript | 508 ms |
| 6a | TTS | `POST /text-to-speech` JSON, `bulbul:v3`, `shubh`, `target_language_code:"en-IN"`, `speech_sample_rate:16000` | 200 | `request_id`, `audios:[<base64>]` → 41,004 B, RIFF/WAVE, 16 kHz mono s16le, 1.28 s | 618 ms |
| 6b | TTS, `language_code` spelling | same with `language_code` | 200 | also accepted; 62,850 B of audio for the same text (different take — the field is read, the two spellings are not identical in effect) | 762 ms |
| 6c | TTS Hindi | `"नमस्ते, आप कैसे हैं?"`, `hi-IN`, 22,050 Hz | 200 | 75,308 B WAV | 719 ms |
| — | TTS `bulbul:v2` | | 400 | `"Model 'bulbul:v2' has been deprecated. Please use 'bulbul:v3' instead."` | 275 ms |
| 7a | **TTS chunked-HTTP stream** | `POST /text-to-speech/stream`, `output_audio_codec:"linear16"` | 200 | `content-type: audio/pcm`, `transfer-encoding: chunked`; **11 chunks**, first byte @461 ms, last @922 ms, 106,496 B total, steady 11,000 B chunks ≈ 344 ms of audio each | 922 ms |
| 7b | `:stream` colon form | `POST /text-to-speech:stream` | 404 | `not_found_error` | 116 ms |
| 8a | STT | `POST /speech-to-text`, multipart part **`file`**, no model | 200 | `request_id`, `transcript`, `language_code:"en-IN"`, `language_probability:1.0` | 553 ms |
| 8b | STT v4 + timestamps | `model=saaras:v4`, `with_timestamps=true`, `language_code=unknown` | 200 | plus `timestamps:{words[],start_time_seconds[],end_time_seconds[]}` (one chunk: `0.0`–`3.58`) | 550 ms |
| 8c | STT translate | `POST /speech-to-text-translate` | 200 | `transcript`, `language_code`, `diarized_transcript:null`, `language_probability` | 773 ms |
| 9a | **TTS WebSocket** | `wss://api.sarvam.ai/text-to-speech/ws?model=bulbul:v3`, header auth; `config` → `text` → `flush` | 101 @196 ms | 5 × `{"type":"audio","data":{"request_id","content_type":"audio/pcm","audio":"<base64>"}}`; first audio **@727 ms, 228 ms after the `text` message**; socket stayed open with no terminal frame | — |
| 9b | **STT WebSocket** | `wss://api.sarvam.ai/speech-to-text/ws?model=saaras:v3&language-code=en-IN&vad_signals=true`; 46 × 100 ms base64 JSON audio messages (3.58 s speech + 1 s silence) | 101 @238 ms | `events/START_SPEECH` @1,137; `events/END_SPEECH` @4,900; **`type:"data"`** @5,139 with `transcript` and **`metrics:{audio_duration:4.096, processing_latency:0.112}`**; no close | — |
| 9c | STT WS, speech with no trailing silence | as 9b, 3.58 s only | 101 | `START_SPEECH` only — **no transcript ever arrived**. The final transcript is emitted on the VAD's END_SPEECH, not on the last audio frame | — |
| 10a | STT unknown model | `model=saaras:v99` | 400 | `invalid_request_error`, message enumerates the model set | 362 ms |
| 10b | STT bad key | | **403** | `invalid_api_key_error` | 267 ms |
| 10c | STT no auth header | | **403** | same body, same code | 322 ms |
| 10e | STT empty file part | | 400 | `"Failed to read the file, please check the audio format."` | 779 ms |
| 10f | STT no `file` part | | 400 | `"body.file : Field required"` | 541 ms |
| 10g | TTS unknown model | `bulbul:v99` | 400 | message enumerates `'bulbul:v2','bulbul:v3-beta','bulbul:v3','bulbul:v4-flash'` | 218 ms |
| 10h | TTS empty text | `text:""` | 400 | `"'text' cannot be empty"` — **not** a 200-with-zero-usage like Inworld | 253 ms |
| 10i | TTS empty body | `{}` | 400 | `"Either 'text' or 'inputs' must be provided"` | 226 ms |
| 10j | TTS bad speaker | | 400 | message enumerates all 34 speakers | 188 ms |
| 10k | TTS 2,800-char text | | 400 | `"text: String should have at most 2500 characters"` | 194 ms |
| 10l | WS bad key (TTS and STT) | | **HTTP 403 on the upgrade** | `{"error":{"message":"Invalid or missing authentication credentials","code":"invalid_api_key_error","request_id":""}}` — no 101 | 239 / 143 ms |

### 2.2 Verbatim bodies

TTS request and response (6a):

```json
POST https://api.sarvam.ai/text-to-speech
api-subscription-key: <SARVAM_KEY>
content-type: application/json

{"text": "Hello from the gateway probe.", "target_language_code": "en-IN",
 "speaker": "shubh", "model": "bulbul:v3", "speech_sample_rate": 16000}

200 application/json  (54,748 B)
{"request_id": "20260918_c2e4b1e5-bc75-40c8-9900-e463367be4f1",
 "audios": ["<base64, decodes to 41,004 bytes: 52 49 46 46 … 'RIFF$\xa0\x00\x00WAVEfmt '>"]}
```

**There is no usage, character count, duration or cost field anywhere in the
TTS response, and no usage header.** The only meter is `len(text)` on the way
in.

STT (8a) and STT-translate (8c):

```json
{"request_id": "20260918_b4327c48-f5ec-442b-8dbd-bee6a259f5fc",
 "transcript": "The quick brown fox jumps over the lazy dog near the river bank today.",
 "language_code": "en-IN", "language_probability": 1.0}

{"request_id": "20260918_b81a8e02-cdc1-4c56-b607-e47bd595e27c",
 "transcript": "The quick brown fox jumps over the lazy dog near the riverbank today.",
 "language_code": "en-IN", "diarized_transcript": null, "language_probability": 1.0}
```

**No duration or usage field on the buffered STT response either.** The
streaming socket is the only Sarvam surface that reports its own meter.

TTS WebSocket (9a), client then server:

```json
{"type": "config", "data": {"speaker": "shubh", "target_language_code": "en-IN", "output_audio_codec": "linear16", "output_audio_sample_rate": 16000}}
{"type": "text", "data": {"text": "Streaming text to speech probe."}}
{"type": "flush"}
---
{"type": "audio", "data": {"request_id": "20260918_acbd5fa7-…", "content_type": "audio/pcm", "audio": "<base64 25688 bytes>"}}
{"type": "audio", "data": {"request_id": "20260918_acbd5fa7-…", "content_type": "audio/pcm", "audio": "<base64 16060 bytes>"}}
… ×5, then silence; the socket is never closed by the server
```

STT WebSocket (9b), the frames that matter:

```json
{"audio": {"data": "<base64 of 3200 B PCM16>", "sample_rate": "16000", "encoding": "audio/wav"}}
---
{"type": "events", "data": {"signal_type": "START_SPEECH", "occured_at": 1789742190.5920734}}
{"type": "events", "data": {"signal_type": "END_SPEECH", "occured_at": 1789742194.3696604}}
{"type": "data", "data": {"request_id": "20260918_715aa999-…",
  "transcript": "The quick brown fox jumps over the lazy dog near the river bank today.",
  "timestamps": null, "diarized_transcript": null,
  "language_code": "en-IN", "language_probability": null,
  "audio_hash": null, "audio_mime": null,
  "metrics": {"audio_duration": 4.096, "processing_latency": 0.11247801780700684}}}
```

Every Sarvam error, HTTP and WebSocket, has one shape:

```json
{"error": {"message": "<human string>", "code": "<machine code>", "request_id": "<id or empty>"}}
```

with the codes observed being `invalid_request_error`, `invalid_api_key_error`
and `not_found_error`. The documented set also includes
`unprocessable_entity_error`, `insufficient_quota_error`, `authentication_error`,
`rate_limit_exceeded_error` and `internal_server_error`. No credential fragment
appeared in any of them.

### 2.3 Pricing

Sarvam publishes in **INR only**, and the public tables are by *service*, not
by model.

| service | price | unit | source |
|---|---|---|---|
| Text to Speech (real-time and streaming) | **₹3.00 per 1,000 characters** | characters, "charged per character, rounded up" | https://www.sarvam.ai/api-pricing and https://docs.sarvam.ai/api/getting-started/pricing, both read **2026-09-18** |
| Bulbul v3 | **₹30 per 10,000 characters** (the same ₹3/1k) | characters | https://docs.sarvam.ai/api/getting-started/pricing, 2026-09-18 |
| Speech to Text (real-time, streaming, batch) | **₹30.00 per hour**, "billed per second" | audio seconds | both pages, 2026-09-18 |
| Speech to Text with diarization | ₹45.00 per hour | audio seconds | docs pricing page, 2026-09-18 |
| Speech to Text & Translate | ₹30.00 per hour | audio seconds | docs pricing page, 2026-09-18 |
| Speech to Text, Translate & Diarization | ₹45.00 per hour | audio seconds | docs pricing page, 2026-09-18 |

Per-model rates (`bulbul:v2` vs `v3` vs `v4-flash`; `saaras:v3` vs `v4` vs
`saarika:*`) are **not published**. The catalog stores USD, so the rows below
carry a converted figure that must be re-derived whenever INR/USD moves — and
that is a real defect in the row, not a rounding detail, so it belongs in the
comment next to `priced_at`.

At **₹88.5/USD (2026-09-18)**: TTS ₹3/1k chars = **USD 33.90 per 1M
characters**; STT ₹30/h = **USD 0.339/h = USD 0.00565 per minute**.

### 2.4 What the gateway would need — Sarvam

**`ProviderConn` row** — new, and it needs no new machinery at all:

```python
"sarvam": ProviderConn(
    id="sarvam", kind="openai",
    base_url="https://api.sarvam.ai",
    api_key_env="SARVAM_API_KEY",
    auth_scheme="bearer",        # verified live: Bearer is accepted identically
                                 # to the documented api-subscription-key header.
                                 # auth_scheme="header" + auth_header="api-subscription-key"
                                 # is the documented alternative and also works.
    forbidden_means="auth",      # 403 IS a bad credential here, unlike AssemblyAI REST
    scrub_error_bodies="auth",   # no credential reflection observed in any of 12 error bodies
    max_concurrency=16,          # no published concurrency limit; no rate-limit headers seen
)
```

`auth_scheme="bearer"` is the reason Sarvam is the cheapest provider in this
sweep to add: it is the existing code path.

**`ModelSpec` rows.** Source for all of them:
https://www.sarvam.ai/api-pricing and https://docs.sarvam.ai/api/getting-started/pricing,
2026-09-18, converted at ₹88.5/USD.

| id | provider | api_model | unit | prices |
|---|---|---|---|---|
| `sarvam.bulbul-v3` | `sarvam` | `bulbul:v3` | `characters` | `input_per_m=33.90`, `output_per_m=0.0`, `context_window=2500`, `default_profile="tts"` |
| `sarvam.bulbul-v4-flash` | `sarvam` | `bulbul:v4-flash` | `characters` | same rate (no per-model price published), `default_profile="tts"` |
| `sarvam.saaras-v3` | `sarvam` | `saaras:v3` | `seconds` | `per_minute=0.00565` |
| `sarvam.saaras-v4` | `sarvam` | `saaras:v4` | `seconds` | `per_minute=0.00565` |
| `sarvam.saaras-v3-realtime` | `sarvam` | `saaras:v3-realtime` | `seconds` | `per_minute=0.00565` |

Do **not** add a `bulbul:v2` row: it is deprecated and answers 400.

**Surfaces.**

| surface | route | upstream | method | body | framing | model id lives | usage |
|---|---|---|---|---|---|---|---|
| `sarvam_tts` | `/sarvam/text-to-speech` | `/text-to-speech` | POST | `json` | `raw` (one buffered JSON object) | body key **`model`** | **none on the wire** — `characters = len(request.text)`, exact, computed at parse time |
| `sarvam_tts_stream` | `/sarvam/text-to-speech/stream` | `/text-to-speech/stream` | POST | `json` | **`raw`** (chunked `audio/pcm` or `audio/mpeg`, no framing, ends on close) | body key `model` | same: from the request text |
| `sarvam_stt` | `/sarvam/speech-to-text` | `/speech-to-text` | POST | **`multipart`** (part name `file`, plus form fields `model`, `mode`, `language_code`, `with_timestamps`) | `raw` | **multipart form field `model`** | **none on the wire** — see below |
| `sarvam_stt_translate` | `/sarvam/speech-to-text-translate` | `/speech-to-text-translate` | POST | `multipart` | `raw` | form field `model` | none |

Everything in that table is already built: `raw` framing exists (Phase D,
`elevenlabs_tts`), `characters` and `seconds` units exist, multipart forwarding
exists (`audio_transcription`), and `auth_scheme="bearer"` is the default path.
**Sarvam's HTTP half is the least work of any voice provider in the
capabilities set** — closer to a catalog entry plus four thin surfaces than to
a project.

**What does NOT fit.**

1. **STT has no meter.** The buffered `/speech-to-text` response carries no
   duration and no usage, and Sarvam bills per audio second. The gateway
   cannot read the bill off the wire. Options, in order of honesty: decode the
   uploaded audio's duration gateway-side from the container header (correct
   for WAV, a guess for compressed input, and it means parsing the request
   body, which the gateway deliberately avoids for audio); bill from the
   `timestamps.end_time_seconds` maximum when `with_timestamps=true` (the
   gateway would have to force the flag, changing the client's request); or
   record `cost_basis=estimated` with `seconds=None` and accept a $0 cost
   record. This is the one place Sarvam is *worse* than Inworld, which puts
   `processedCharactersCount` on the first frame. Note the contrast: the
   **streaming** STT socket reports `metrics.audio_duration` exactly
   (4.096 s), so the meter exists — it is simply absent from the HTTP product.
2. **Both streaming products are WebSockets** (`/text-to-speech/ws`,
   `/speech-to-text/ws`), so they wait on the Phase G plane exactly as Inworld
   and AssemblyAI do. When it exists, Sarvam is unusually easy on it: auth is
   rejected at the upgrade with a real HTTP 403 and a JSON body, so the
   1008-ambiguity problem AssemblyAI has does not arise, and the STT socket
   carries its own meter.
3. **Neither Sarvam socket closes itself.** Both stayed open indefinitely
   after the last useful frame (9a, 9b) with no server PING and no terminal
   event. A relay must own the close, and the drain must not wait for one.
4. **The TTS stream has no terminal frame either** — the chunked HTTP body
   simply ends. Fine for `raw` framing; worth stating because the gateway's
   "terminal event" concept has nothing to bind to.
5. **`target_language_code` vs `language_code`**: both are accepted by
   `/text-to-speech` (6a, 6b) and produced audibly different takes of the same
   sentence. The docs name `language_code`. A surface that rewrites or
   validates the body must pass both through untouched.

---

## 3. Spend

| provider | what | amount |
|---|---|---|
| Sarvam TTS | ~14 calls, ~600 characters total, ₹3/1k chars | ₹1.8 ≈ **USD 0.020** |
| Sarvam STT | ~12 calls × ~3.6 s + 2 WS sessions, ~55 s, ₹30/h | ₹0.46 ≈ **USD 0.005** |
| AssemblyAI async | 1 transcript, 2 s billed, $0.21/h | **< USD 0.001** |
| AssemblyAI sync | 3 transcripts × 1.28 s, $0.45/h | **< USD 0.001** |
| AssemblyAI streaming | 6 sessions, ~45 s of open socket, $0.15/h | **USD 0.002** |
| **total** | | **≈ USD 0.03** |

---

## 4. Corrections someone should apply

**`src/llmgw/catalog.py`**

1. `assemblyai-sync` row: `forbidden_means="rate_limit"` is **wrong**. That
   host does not 403 at all; a bad key there is a **404** with
   `application/problem+json` and `detail: "Invalid API key"`. Set
   `forbidden_means="auth"` and add an error rule for
   "404 + `application/problem+json` + `detail` containing `Invalid API key`
   → AuthenticationFailed", distinct from a routing 404.
2. `assemblyai` (REST) row: `forbidden_means="rate_limit"` is **unverified**.
   A bad key on `api.assemblyai.com` is a **401**, not a 403 (probe A7b). The
   403-as-rate-limit claim comes from documentation the sweep never exercised.
   Leave it, but mark it unverified in the comment rather than asserting it.
3. `assemblyai.sync` ModelSpec: `api_model="universal-3-5-pro"` is now
   **load-bearing**, not decorative. The comment says "The sync endpoint takes
   no model parameter; the id exists so the gateway has a row to price
   against." That is false: the endpoint requires `X-AAI-Model`, and
   `universal-2` is rejected there. Rewrite the comment and wire `api_model`
   into the header.
4. Add `sarvam` provider row and five `sarvam.*` model rows (§2.4).
5. Consider `assemblyai.universal-3-5-pro` / `assemblyai.universal-2` async
   rows ($0.21/h, $0.15/h) — needed only if the job-shaped route kind is ever
   built.

**`src/llmgw/surfaces/voice/assemblyai_sync.py`**

6. `upstream_path = "/transcribe"` with `body = "raw"` **cannot ever work**.
   The route is `/v1/transcribe` (or `/transcribe`), the body must be
   `multipart/form-data` with a part named `audio`, and `X-AAI-Model` must
   carry the api_model or the AWS load balancer 404s before the app sees the
   request. Change `body` to `"multipart"`, add the header injection, and keep
   `framing="raw"`.
7. The module docstring's shape — "raw PCM in (16 kHz mono s16le, up to 120 s /
   40 MB), one JSON object out (`text`, `audio_duration_ms`, `request_time_ms`,
   `session_id`)" — is right about the **response** (verified verbatim, §1.3)
   and wrong about the **request**. Raw PCM is supported, but only as the
   `audio` part of a multipart body with an optional `config` part
   (`{"sample_rate":16000,"channels":1}`), never as the whole body.
8. `usage_from_body` reading `audio_duration_ms / 1000` is **correct** and
   confirmed exact (1,280 ms for a 1.28 s file, no rounding). No change.
9. The docstring's "Its reference page was unreachable during the sweep, so the
   error-body shape is unverified; classification is by status" can be
   replaced with the five real bodies in §1.3 — all RFC-7807 shaped
   (`{status,title,detail}`, `application/problem+json`).

**`capabilities/voice-assemblyai.md`**

10. §1 "Sync STT `POST https://sync.assemblyai.com/transcribe` … HTTP, raw PCM
    body": the path is `/v1/transcribe`, the body is multipart, and
    `X-AAI-Model` is mandatory. Also drop "Not fetchable:
    `api-reference/sync-api/transcribe-live`" — the endpoint set is
    `/v1/transcribe`, `/v1/transcribe-live`, `/v1/warm`, per
    `assemblyai.com/docs/llms.txt`.
11. §4 "`token=` query param (temporary, **one-time**…)": not one-time. The
    same token opened a second session cleanly (probe A5f). Do not build a
    mint that assumes single use.
12. §4 should record that `max_session_duration_seconds` was **verified** to
    cap the session (300 s → `Begin.expires_at` 5 min out, against 3 h for
    header auth). §3/§5's 3-hour figure is confirmed.
13. §5 "Over the opens limit: close 1008 … Same code 1008 is used for
    bad/missing auth → a body-sniff on the `Error` frame is required" is
    **correct and now verified**: all three bad-credential cases returned 101,
    then `Error` `error_code: 1008`, then close 1008 with the constant reason
    `See Error message for details`.
14. §5 "Sync STT errors … not documented in the fetched pages (413 for >40 MB
    presumed)" — replace with the five observed bodies.
15. §6 "Sync STT: $0.45/h of audio" — still correct at 2026-09-18
    (assemblyai.com/products/sync-speech-to-text). §6's streaming and
    pre-recorded rates are also unchanged.
16. Add: AssemblyAI has **no TTS product** (llms.txt product list, 2026-09-18;
    `/v2/tts` and `/v1/speech` both 404). The file never claims one, but the
    cross-provider table's "TTS" row for AssemblyAI should say so explicitly
    rather than leaving a dash.
17. Add: `POST /v2/realtime/token` is **gone** (404). Only
    `GET streaming.assemblyai.com/v3/token` mints.
18. Add: the streaming default model with no query param is
    `universal-streaming-english`, echoed in `Begin.configuration.model`.
19. Add: async `audio_duration` is **integer seconds rounded up** (2 for a
    1.28 s file) while sync's `audio_duration_ms` is exact. Two products, two
    rounding rules, one price table.

**`capabilities/voice.md`**

20. The verdict paragraph and the "Products and transports" table list
    AssemblyAI batch/sync as "**Supported** (Phase D) for AssemblyAI sync
    (`assemblyai_sync`, raw PCM)". It is **not** supported: the surface as
    written 404s on every call. Downgrade the cell to "built but broken —
    wrong path, wrong body kind, missing routing header" until correction 6 is
    applied.
21. Line 33's "Still not supported: … ElevenLabs / AssemblyAI live verification
    (no keys provisioned)" is out of date for AssemblyAI: a key exists and
    everything in §1 above is live-verified as of 2026-09-18.
22. The "Auth" table's AssemblyAI row ("Not injectable: `build_headers` knows
    `Bearer` and `x-api-key`") predates `auth_scheme="raw"`, which now exists
    and is verified correct on all three hosts. Update.
23. "Errors and limits that collide": add the two new collisions —
    **404 means bad credential** on `sync.assemblyai.com`, and **the model is
    a routing header** (`X-AAI-Model`) whose absence produces a load-balancer
    404 that looks nothing like a model error.
24. Add Sarvam to the cross-provider matrix: HTTP TTS (buffered + chunked
    stream), HTTP STT and STT-translate (multipart), WebSocket TTS and STT,
    `Authorization: Bearer` accepted, 403 = bad credential, characters for TTS
    and audio seconds for STT, and the fact that **only the STT WebSocket
    reports its own duration** while both HTTP products report no usage at all.
