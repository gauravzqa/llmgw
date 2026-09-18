# WebSocket captures: Inworld TTS/STT and OpenAI Realtime (2026-09-18)

Live probe of the WebSocket APIs the Phase G data plane will relay. Every
frame was recorded at the WebSocket layer (a `websockets` 17.1 client with its
own keepalive disabled and `process_event` hooked, so server PING/PONG/CLOSE
frames appear too) with a monotonic timestamp in ms since the start of
`connect()`, direction, opcode, size, and the JSON with base64 audio replaced
by its decoded length. Client from Chennai (`CF-RAY …-MAA`); one round trip to
either provider is about 320 ms, which is the floor under every
"request → first reply" number below.

Raw captures (`<probe>.json`, `<probe>.log`) and the scripts (`wslog.py`,
`inworld_tts.py`, `inworld_stt.py`, `openai_rt.py`, `idle_probes.py`) are in the
session scratchpad under `phaseG-probe/captures/`. Credentials were loaded by
variable reference only; every saved file was scrubbed of the keys, of the
masked key prefixes both providers echo in their auth errors (`<KEY4>***`,
`<KEY8>***…`), of `?key=` values and of Cloudflare cookies.

Audio: one sentence ("Hello from the gateway probe.", 29 characters) on
`inworld-tts-1.5-mini` / `Aarav`, LINEAR16 16 kHz. Its 52,016 bytes of PCM
(1.63 s, RIFF header stripped) were the STT input everywhere: 100 ms chunks
(3,200 B) for Inworld; linearly resampled to 24 kHz (4,800 B chunks) for
OpenAI, because the GA transcription session refuses 16 kHz (probe 7d).

Sweep docs this settles: `capabilities/voice-inworld.md` (rows marked
"source-verified only"/"still unverified live") and `capabilities/voice-openai.md`
(Realtime section).

## 1. Inworld

Endpoints: `wss://api.inworld.ai/tts/v1/voice:streamBidirectional`,
`wss://api.inworld.ai/stt/v1/transcribe:streamBidirectional`. Credential
`Authorization: Basic <INWORLD_API_KEY>` (the portal key, already base64 of
`key:secret`, 76 characters). Upgrade response headers on every probe, valid
key or not: `sec-websocket-accept`, `date`, `server: istio-envoy`, `via: 1.1
google`, `Upgrade`, `Connection`, `Alt-Svc: h3=":443"`. No
`x-inworld-request-id`, no `permessage-deflate`, no cookies. Every server frame
is TEXT; the server never sent PING or PONG in any probe.

### 1.1 Probe table

| # | probe | request | upgrade | server frames | timings (ms from connect) | close |
|---|---|---|---|---|---|---|
| 1 | TTS header auth, happy | Basic header; `create` → `send_text` (29 chars) → `flush_context` → `close_context` | 101 @1,382 | `contextCreated` 1, `audioChunk` 5 (6,434 / 16,044 / 16,044 / 13,484 / 54 B decoded = 52,060 B), `flushCompleted` 1, `contextClosed` 1 | nothing unprompted for 2 s after 101; `create`→`contextCreated` 326; `send_text`→first audio 379; first→last audio 320; `close_context`→`contextClosed` 322 | client 1000; server echoed CLOSE 1000 ~320 ms later; socket had stayed open 3 s after `contextClosed` |
| 2a | TTS `?key=` query only | no header, key in query | 101 @476 | silent 3 s; after `create`: `error` code 16 `authentication is required` (`SESSION_TOKEN_INVALID`, `NO_RETRY`) | error 319 after `create` | **server** CLOSE 1000 `''` in the same ms as the error |
| 2b | TTS bad key (one base64 char flipped) | Basic header | 101 @457 | **silent for 20 s with no client message**; after `create`: `error` code 7 `Invalid credentials provided for API key "<KEY4>***"` | error 313 after `create` | **server** CLOSE 1000 immediately after the error |
| 2c | TTS no credential | no header | 101 @1,379 | silent 20 s; after `create`: same code-16 body as 2a | error 320 after `create` | **server** CLOSE 1000 |
| 3 | TTS idle 75 s | valid key; `create`, then nothing | 101 @522 | `contextCreated`; then **0 frames of any opcode for 75 s** (no PING, no close); afterwards `send_text` + `flush` on the same context produced 3 `audioChunk` + `flushCompleted` normally (usage 11 chars) | first audio 455 after the post-idle `send_text` | client 1000 @78,941 |
| 4 | TTS two contexts + 6th | `create` A, B; `send_text`+`flush` both; `create` C, D, E; `create` F; `close_context` ×6; `send_text` to unknown id | 101 @1,345 | `contextCreated` ×5, `audioChunk` 4 (A) + 3 (B) interleaved by context, `flushCompleted` ×2, `status` code 8 for F, `contextClosed` ×5, `status` code 5 ×2 | both `contextCreated` 320 after the paired `create`s (same ms); A first audio 369, B 394; B's audio arrives while A is still streaming | client 1000 |
| 5 | TTS invalid / oversize | `{"foo":"bar"}`; `this is not json`; 400 B BINARY; 2,100-char `send_text`; 1 MiB text frame | 101 @1,429 | `{"foo":"bar"}`: **no reply at all** (4 s); non-JSON: top-level `error` code 3 `invalid WebSocket request for the selected response protocol`; binary: same error; 2,100 chars: `result.status` code 3 `text length should not exceed 2000 characters.`; 1 MiB frame accepted, answered `status` code 5 `context … not found (payload=SEND_TEXT)` (context already closed) 1,563 ms later | socket stayed open through all five | client 1000 |
| 6 | STT stream | `transcribeConfig` (`inworld/inworld-stt-1`, LINEAR16 16 kHz mono, en-US), 17×100 ms `audioChunk`, `endTurn`, `closeStream` | 101 @522 | **no ack to `transcribeConfig`**; `speechStarted` 1, `transcription` interim 1 + final 1, `usage` 1 | `speechStarted` 428 ms after the first chunk; interim transcript 1,290 after the first chunk; final 337 after `endTurn`; `usage` 317 after `closeStream` | server did **not** close after `usage`; client 1000 8.3 s later |
| 6b | STT usage cadence | as 6 plus 60×100 ms of digital silence before `endTurn` (7.6 s streamed) | 101 @1,345 | `speechStarted`, `transcription` ×3 interim, `speechStopped` (`silenceDurationMs: 150`), `transcription` final (`silenceDurationMs: 300`, server-side end of turn, before `endTurn` was sent), `usage` after `closeStream` | final 1,919 after the last speech chunk; **no periodic usage frame in 7.6 s of audio** | client 1000 |
| 6c | STT bad model | `transcribeConfig` with `inworld/no-such-model` | 101 @1,369 | `error` code 3 `Unsupported model "inworld/no-such-model". Supported models: https://docs.inworld.ai/docs/tutorial-integrations/stt/supported-models` | error 326 after config | **server** CLOSE 1000 |

### 1.2 Frame logs

Probe 1, TTS happy path (`S0` = `"status":{"code":0,"message":"","details":[]}`):

```
 1381.5  upgrade 101 Switching Protocols
 3382.2  C>S  TEXT    272 B  {"create":{"modelId":"inworld-tts-1.5-mini","voiceId":"Aarav","audioConfig":{"audioEncoding":"LINEAR16","sampleRateHertz":16000},"bufferCharThreshold":120,"maxBufferDelayMs":3000,"autoMode":true,"applyTextNormalization":"ON"},"contextId":"ctx-32016aa9"}
 3708.0  S>C  TEXT    381 B  {"result":{"contextId":"ctx-32016aa9","contextCreated":{"voiceId":"Aarav","audioConfig":{"audioEncoding":"LINEAR16","sampleRateHertz":16000},"modelId":"inworld-tts-1.5-mini","maxBufferDelayMs":3000,"bufferCharThreshold":120,"applyTextNormalization":"ON","autoMode":true,"synthesisContext":null,"pronunciationDictionarySettings":null},S0}}
 3708.3  C>S  TEXT     85 B  {"send_text":{"text":"Hello from the gateway probe."},"contextId":"ctx-32016aa9"}
 3708.6  C>S  TEXT     50 B  {"flush_context":{},"contextId":"ctx-32016aa9"}
 4087.1  S>C  TEXT   8792 B  {"result":{"contextId":"ctx-32016aa9","audioChunk":{"audioContent":"<base64 6434 bytes (8580 chars)>","usage":{"processedCharactersCount":29,"modelId":"inworld-tts-1.5-mini"},"timestampInfo":null},S0}}
 4166.1  S>C  TEXT  21603 B  {"result":{"contextId":"ctx-32016aa9","audioChunk":{"audioContent":"<base64 16044 bytes>","usage":{"processedCharactersCount":0,"modelId":"inworld-tts-1.5-mini"},"timestampInfo":null},S0}}
 4364.6  S>C  TEXT  21603 B  … audioChunk <base64 16044 bytes>, processedCharactersCount 0
 4407.2  S>C  TEXT  18191 B  … audioChunk <base64 13484 bytes>, processedCharactersCount 0
 4407.5  S>C  TEXT    283 B  {"result":{"contextId":"ctx-32016aa9","audioChunk":{"audioContent":"UklGRi4AAABXQVZFZm10IBAAAAABAAEAgD4AAAB9AAACABAAZGF0YQoAAAD6//r/+v/7//r/","usage":{"processedCharactersCount":0,…},"timestampInfo":null},S0}}
 4407.6  S>C  TEXT    105 B  {"result":{"contextId":"ctx-32016aa9","flushCompleted":{},S0}}
 4409.0  C>S  TEXT     50 B  {"close_context":{},"contextId":"ctx-32016aa9"}
 4730.8  S>C  TEXT    104 B  {"result":{"contextId":"ctx-32016aa9","contextClosed":{},S0}}
 7734    C>S  CLOSE 1000 (client)
 8055.4  S>C  CLOSE   2 B   close_code=1000 reason=''
```

The first decoded chunk starts with a 44-byte `RIFF…WAVE` header (as on the
HTTP stream). So does the tiny trailing chunk (54 bytes = header + 5 samples),
which is a second WAV header inside the same context's audio; a raw-PCM
consumer must strip a header from every chunk that begins `RIFF`, not only the
first. Total 52,060 B decoded for 29 characters (1.63 s at 16 kHz).

Probe 2b, bad key (2a and 2c differ only in the error body, shown in the table):

```
  457.0  upgrade 101
         (20,000 ms of silence: no frame of any opcode from the server)
20458.4  C>S  TEXT    267 B  {"create":{…},"contextId":"ctx-bad"}
20771.5  S>C  TEXT     98 B  {"error":{"code":7,"message":"Invalid credentials provided for API key \"<KEY4>***\"","details":[]}}
20771.5  S>C  CLOSE     2 B  close_code=1000 reason=''
```

Probe 4, two contexts (audio elided to sizes; every server frame carries
`result.contextId`):

```
 1345.4  C>S create ctx-A            1345.6  C>S create ctx-B
 1664.9  S>C contextCreated ctx-A    1665.0  S>C contextCreated ctx-B
 4666.4  C>S send_text ctx-A "Context A speaking."   4666.7 C>S send_text ctx-B   4666.7 C>S flush A   4666.7 C>S flush B
 5036.1  S>C audioChunk ctx-A  6434 B  usage 19
 5061.0  S>C audioChunk ctx-B  6434 B  usage 19
 5118.3  S>C audioChunk ctx-A 18604 B  usage 0
 5118.6  S>C audioChunk ctx-A    54 B  usage 0
 5118.6  S>C flushCompleted ctx-A
 5315.6  S>C audioChunk ctx-B 16044 B
 5368.3  S>C audioChunk ctx-B 18604 B
 5368.7  S>C audioChunk ctx-B    54 B
 5368.7  S>C flushCompleted ctx-B
 5369.1  C>S create ctx-C, ctx-D, ctx-E          5683.0-5684.6  S>C contextCreated ×3
 8686.4  C>S create ctx-F
 9001.5  S>C  {"result":{"contextId":"ctx-F","status":{"code":8,"message":"You have reached the limit of 5 TTS contexts per connection. Please close other contexts on this connection to continue.","details":[]}}}
13003.5  C>S close_context ×6 (A..F)
13317.3  S>C contextClosed ctx-A … ctx-E (five frames, 13317-13325)
13324.6  S>C  {"result":{"contextId":"ctx-F","status":{"code":5,"message":"context ctx-F not found","details":[]}}}
17326.4  C>S  {"send_text":{"text":"ghost"},"contextId":"ctx-nope"}
17641.4  S>C  {"result":{"contextId":"ctx-nope","status":{"code":5,"message":"context ctx-nope not found (payload=SEND_TEXT)","details":[]}}}
```

Probe 5, invalid input:

```
 1428.8  C>S  TEXT     14 B  {"foo":"bar"}                       → nothing for 4 s
 5431.4  C>S  TEXT     16 B  this is not json
 5745.4  S>C  TEXT    121 B  {"error":{"code":3,"message":"invalid WebSocket request for the selected response protocol","status":"INVALID_ARGUMENT"}}
 9746.9  C>S  BINARY  400 B
10062.7  S>C  TEXT    121 B  (identical error)                   → socket still open
14065.2  C>S  create ctx-big → 14382.0 contextCreated
14382.4  C>S  send_text 2,100 chars + flush
14703.3  S>C  {"result":{"contextId":"ctx-big","status":{"code":3,"message":"text length should not exceed 2000 characters.","details":[]}}}   (no flushCompleted follows)
29705.0  C>S  close_context → 30021.0 contextClosed
30023.7  C>S  TEXT 1048627 B  {"send_text":{"text":"xxxx…1,048,576 chars"},"contextId":"ctx-big"}
31586.7  S>C  {"result":{"contextId":"ctx-big","status":{"code":5,"message":"context ctx-big not found (payload=SEND_TEXT)","details":[]}}}
```

Probe 6, STT (audio chunks elided; 17 × `{"audioChunk":{"content":"<base64 3200 bytes>"}}` from 3,524 to 5,018 ms):

```
  522.0  C>S  TEXT    259 B  {"transcribeConfig":{"modelId":"inworld/inworld-stt-1","audioEncoding":"LINEAR16","sampleRateHertz":16000,"numberOfChannels":1,"language":"en-US","endOfTurnConfidenceThreshold":0.7,"inworldSttV1Config":{"minEndOfTurnSilenceWhenConfident":160}}}
         (no acknowledgement; 3.0 s of silence until audio was sent at 3,524)
 3951.2  S>C  TEXT     61 B  {"result":{"speechStarted":{"startTimeMs":0,"confidence":0}}}
 4814.0  S>C  TEXT    144 B  {"result":{"transcription":{"transcript":"Hello from the gate.","isFinal":false,"wordTimestamps":[],"voiceProfile":null,"silenceDurationMs":0}}}
 5119.2  C>S  TEXT     15 B  {"endTurn":{}}
 5455.8  S>C  TEXT    152 B  {"result":{"transcription":{"transcript":"Hello from the Gateway Probe.","isFinal":true,"wordTimestamps":[],"voiceProfile":null,"silenceDurationMs":0}}}
11457.3  C>S  TEXT     19 B  {"closeStream":{}}
11774.7  S>C  TEXT     82 B  {"result":{"usage":{"transcribedAudioMs":1500,"modelId":"inworld/inworld-stt-1"}}}
         (server keeps the socket open; client CLOSE 1000 at 20,091, server echo 320 ms later)
```

Probe 6b differs only in the middle: with 6 s of silence appended, the server
ended the turn itself (`speechStopped {"silenceDurationMs":150}` at 7,525,
final transcript with `silenceDurationMs: 300` at 7,757, both before the
client's `endTurn` at 11,735, which then produced nothing), and the single
`usage` frame after `closeStream` read `transcribedAudioMs: 3150` for 7.6 s
streamed. `transcribedAudioMs` is therefore not "bytes streamed ÷ rate": it is
the provider's own count (speech plus the end-of-turn tail, 50 ms granularity),
and it arrives once, at the end.

Probe 6c, bad STT model:

```
 1368.8  C>S  transcribeConfig modelId "inworld/no-such-model"
 1694.4  S>C  TEXT    180 B  {"error":{"code":3,"message":"Unsupported model \"inworld/no-such-model\". Supported models: https://docs.inworld.ai/docs/tutorial-integrations/stt/supported-models","details":[]}}
 1694.6  S>C  CLOSE     2 B  close_code=1000 reason=''
```

### 1.3 Server frame taxonomy observed (Inworld)

| frame | where | class for the relay |
|---|---|---|
| `result.audioChunk` (TTS), `result.transcription` (STT, interim or final) | per context / stream | progress (content) |
| `result.contextCreated`, `result.flushCompleted`, `result.contextClosed`, `result.speechStarted`, `result.speechStopped` | per context | meta / liveness; `flushCompleted` is the per-utterance terminal, `contextClosed` the per-context terminal |
| `result.status` with `code != 0` and a `contextId` | per context | in-context error, socket survives (codes seen: 3 invalid text, 5 context not found, 8 context limit) |
| `result.usage` (STT) | once after `closeStream` | billing; the stream's terminal |
| top-level `error {code, message, details|status}` | connection | fatal when auth/model (server CLOSE 1000 follows in the same ms); **non-fatal** for malformed frames (code 3 with `"status":"INVALID_ARGUMENT"`, no `details`, socket stays open) |
| PING / PONG | none in 75 s idle | there is no server liveness signal at all |

## 2. OpenAI Realtime

Endpoint `wss://api.openai.com/v1/realtime` with `?model=gpt-realtime-mini`
(catalog row `openai.gpt-realtime-mini`) or `?intent=transcription`; header
`Authorization: Bearer <OPENAI_API_KEY>` only. Upgrade response headers on
every probe, including bad key / bad model: `sec-websocket-extensions:
permessage-deflate`, `server: cloudflare`, `x-openai-proxy-wasm: v0.1`,
`cf-cache-status: DYNAMIC`, `CF-RAY`, `Strict-Transport-Security`,
`X-Content-Type-Options: nosniff`, `set-cookie: __cf_bm=…` (Cloudflare bot
cookie, 30 min), `alt-svc`. No `x-request-id`, no `openai-processing-ms`, no
rate-limit headers on the upgrade. Every server data frame is TEXT.

### 2.1 Probe table

| # | probe | request | upgrade | server frames | timings (ms from connect) | close |
|---|---|---|---|---|---|---|
| 7 | transcription session, GA shape | `?intent=transcription`; `session.update {type: transcription, audio.input: {format: {audio/pcm, 24000}, transcription: {model: gpt-4o-mini-transcribe}, turn_detection: {server_vad}}}`; 17 × `input_audio_buffer.append` (4,800 B PCM each); `input_audio_buffer.commit` | 101 @1,338 | `session.created` 1, `session.updated` 1, `input_audio_buffer.speech_started` 1, `input_audio_buffer.committed` 1, `conversation.item.added` 1, `conversation.item.done` 1, `…transcription.delta` 6, `…transcription.completed` 1 (with `usage`); **no `rate_limits.updated`** | `session.created` 5.7 after 101; `session.updated` 256 after update; `speech_started` 261 after the first append; `committed` 260 after commit; first delta 1,190 after commit; completed 1,411 after commit | client 1000 |
| 7b | transcription, legacy beta field names | `transcription_session.update {input_audio_format: pcm16, input_audio_transcription: {…}, …}` | 101 @859 | `error` `invalid_value` on `type`, listing the 12 accepted client event types; socket stays open; a following minimal `session.update {type: transcription}` → `session.updated` | error 265 after the message | client 1000 |
| 7c | transcription with `OpenAI-Beta: realtime=v1` | header added | 101 @850 | first frame `error` `beta_api_shape_disabled`, then CLOSE | error at 849.8, i.e. before/with the upgrade completing | **server** 4000 `invalid_request_error.beta_api_shape_disabled` |
| 7d | transcription, 16 kHz declared | `session.update` with `format.rate: 16000` | 101 @864 | `error` `integer_below_min_value` on `session.audio.input.format.rate` ("Expected a value >= 24000"); the rest of the update is **discarded** (no transcription model set); the 16 kHz audio was still accepted, `speech_started`, `committed`, item added, but **no transcript ever arrived** (16 s wait) | | client 1000 |
| 8 | realtime session, text only | `?model=gpt-realtime-mini`; `session.update {type: realtime, output_modalities: [text]}`; `conversation.item.create` (user "Say hi in three words"); `response.create`; then `{"type":"nope"}`, `session.update {bogus_field: 1}`, `not json`, a 64 B BINARY frame; then a second item + `response.create` | 101 @888 | `session.created`, `session.updated`, then per response: `conversation.item.added/done` (user), `response.created`, `response.output_item.added`, `conversation.item.added` (assistant), `response.content_part.added`, `response.output_text.delta` ×5 ("Hi there, friend!"), `response.output_text.done`, `response.content_part.done`, `conversation.item.done`, `response.output_item.done`, `response.done`, **then** `rate_limits.updated`; 4 `error` events, one per bad input, socket open throughout; second response ("See ya!") completed normally | `session.created` 2.6 after 101; `session.updated` 263 after update; `response.created` 280 after `response.create`; first text delta 476 after `response.create`; `response.done` 518; `rate_limits.updated` 43 after `response.done` | client 1000 |
| 8b | realtime with `OpenAI-Beta: realtime=v1` | header added | 101 @1,409 | `error` `beta_api_shape_disabled` | first frame at 1,408.9 (with the upgrade) | **server** 4000 `invalid_request_error.beta_api_shape_disabled` |
| 9 | realtime idle 70 s | `session.created` then silence | 101 @2,107 | **server PING every 20.26 s** (4-byte random payload) at 22,104 / 42,362 / 62,620; no data frames; no close; `session.update` at 72 s → `session.updated` | | client 1000 @74,634 |
| 10 | bad bearer (last 4 chars changed) | | **101** @896 | `error` `invalid_api_key` ("Incorrect API key provided: <KEY8>***…AAAA") | same ms as 101 | **server** 3000 `invalid_request_error.invalid_api_key` |
| 10b | no `Authorization` | | **101** @838 | `error` code `null`, "Missing bearer or basic authentication in header" | same ms | **server** 3000 `invalid_request_error` |
| 11 | `?model=gpt-nope` | | **101** @862 | `error` `invalid_model` ('Model "gpt-nope" is not supported in realtime mode…') | same ms | **server** 4000 `invalid_request_error.invalid_model` |
| 11b | no `model` parameter | | **101** @889 | `error` `missing_model` ("You must provide a model parameter, for example wss://api.openai.com/v1/realtime?model=gpt-realtime-1.5") | same ms | **server** 4000 `invalid_request_error.missing_model` |

In every server-closed case the CLOSE frame arrived in the same millisecond
as the `error` event, carrying the error's `type.code` as the close reason.
Close code 3000 is used for authentication failures, 4000 for request-shape
failures (model, beta header). The ~2.2 s between that CLOSE and the recorded
close time in the raw files is the client library's close handshake, not the
server.

### 2.2 Frame logs

Probe 7, transcription (append frames elided: 17 × `{"type":"input_audio_buffer.append","audio":"<base64 4800 bytes>"}` from 1,601 to 3,204 ms):

```
 1338.0  upgrade 101
 1343.7  S>C  TEXT    429 B  {"type":"session.created","event_id":"event_…","session":{"type":"transcription","object":"realtime.transcription_session","id":"sess_…","expires_at":1789728574,"audio":{"input":{"format":{"type":"audio/pcm","rate":24000},"transcription":null,"noise_reduction":null,"turn_detection":{"type":"server_vad","threshold":0.5,"prefix_padding_ms":300,"silence_duration_ms":200}}},"include":null}}
 1344.1  C>S  TEXT    230 B  {"type":"session.update","session":{"type":"transcription","audio":{"input":{"format":{"type":"audio/pcm","rate":24000},"transcription":{"model":"gpt-4o-mini-transcribe"},"turn_detection":{"type":"server_vad"}}}}}
 1600.3  S>C  TEXT    489 B  {"type":"session.updated",…"transcription":{"model":"gpt-4o-mini-transcribe","language":null,"prompt":null},…}
 1861.0  S>C  TEXT    143 B  {"type":"input_audio_buffer.speech_started","event_id":"…","audio_start_ms":0,"item_id":"item_…"}
 3305.8  C>S  TEXT     37 B  {"type":"input_audio_buffer.commit"}
 3565.4  S>C  TEXT    143 B  {"type":"input_audio_buffer.committed","event_id":"…","previous_item_id":null,"item_id":"item_…"}
 3565.5  S>C  TEXT    247 B  {"type":"conversation.item.added",…"item":{"id":"item_…","type":"message","status":"completed","role":"user","content":[{"type":"input_audio","transcript":null}]}}
 3567.9  S>C  TEXT    246 B  {"type":"conversation.item.done",… same item …}
 4495.7  S>C  TEXT    202 B  {"type":"conversation.item.input_audio_transcription.delta","event_id":"…","item_id":"item_…","content_index":0,"delta":"Hello","obfuscation":"hbKWaqwA7ya"}
 4500.7 … 4518.0  five more deltas: " from", " the", " Gateway", " Probe", "."
 4717.2  S>C  TEXT    345 B  {"type":"conversation.item.input_audio_transcription.completed","event_id":"…","item_id":"item_…","content_index":0,"transcript":"Hello from the Gateway Probe.","usage":{"type":"tokens","total_tokens":24,"input_tokens":16,"input_token_details":{"text_tokens":0,"audio_tokens":16},"output_tokens":8}}
 8971.2  S>C  CLOSE (echo of client 1000)
```

Probe 8, text session (event ids elided; `R1`/`R2` = the two response ids):

```
  890.1  S>C  session.created   session.type=realtime, model=gpt-realtime-mini, output_modalities=[audio], voice alloy, audio/pcm 24000 in+out, server_vad 0.5/300/200 idle_timeout_ms null create_response true interrupt_response true, max_output_tokens "inf", truncation auto, expires_at (60 min)
  890.7  C>S  {"type":"session.update","session":{"type":"realtime","output_modalities":["text"]}}
 1153.7  S>C  session.updated   output_modalities=[text], everything else unchanged
 1153.9  C>S  {"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"Say hi in three words"}]}}
 1154.1  C>S  {"type":"response.create"}
 1422.4  S>C  conversation.item.added   (user item, status completed)
 1424.7  S>C  conversation.item.done
 1433.9  S>C  response.created   {"response":{"object":"realtime.response","id":R1,"status":"in_progress","status_details":null,"output":[],"conversation_id":"conv_…","output_modalities":["text"],"max_output_tokens":"inf","audio":{"output":{"format":{"type":"audio/pcm","rate":24000},"voice":"alloy"}},"usage":null,"metadata":null}}
 1629.1  S>C  response.output_item.added   (assistant item, in_progress)
 1630.2  S>C  conversation.item.added      (assistant item)
 1630.3  S>C  response.content_part.added  part {"type":"text","text":""}
 1630.3  S>C  response.output_text.delta   {"response_id":R1,"item_id":…,"output_index":0,"content_index":0,"delta":"Hi","obfuscation":"A5Nj2cC01aseXj"}
 1636.3 … 1666.1  deltas " there", ",", " friend", "!"
 1666.3  S>C  response.output_text.done    text "Hi there, friend!"
 1666.3  S>C  response.content_part.done
 1667.3  S>C  conversation.item.done       (assistant item completed)
 1668.5  S>C  response.output_item.done
 1672.2  S>C  response.done   {"response":{"object":"realtime.response","id":R1,"status":"completed","status_details":null,"output":[{…assistant message…}],"conversation_id":"conv_…","output_modalities":["text"],"max_output_tokens":"inf","audio":{…},"usage":{"total_tokens":127,"input_tokens":120,"output_tokens":7,"input_token_details":{"text_tokens":120,"audio_tokens":0,"image_tokens":0,"cached_tokens":64,"cached_tokens_details":{"text_tokens":64,"audio_tokens":0,"image_tokens":0}},"output_token_details":{"text_tokens":7,"audio_tokens":0}},"metadata":null}}
 1672.8  C>S  {"type":"nope"}
 1715.6  S>C  rate_limits.updated   {"rate_limits":[{"name":"tokens","limit":15000000,"remaining":14999716,"reset_seconds":0.001}]}
 1939.1  S>C  error   {"error":{"type":"invalid_request_error","code":"invalid_value","message":"Invalid value: 'nope'. Supported values are: 'session.update', 'session.close', 'input_audio_buffer.append', 'session.input_audio_buffer.append', 'input_audio_buffer.commit', 'input_audio_buffer.clear', 'conversation.item.create', 'conversation.item.truncate', 'conversation.item.delete', 'conversation.item.retrieve', 'response.create', and 'response.cancel'.","param":"type","event_id":null}}
 1939.6  C>S  {"type":"session.update","session":{"type":"realtime","bogus_field":1}}
 2208.6  S>C  error   {"error":{"type":"invalid_request_error","code":"unknown_parameter","message":"Unknown parameter: 'session.bogus_field'.","param":"session.bogus_field","event_id":null}}
 2209.0  C>S  TEXT "not json"
 2471.9  S>C  error   {"error":{"type":"invalid_request_error","code":"invalid_json","message":"Invalid event: failed to parse JSON value. …","param":null,"event_id":null}}
 2472.0  C>S  BINARY 64 B
 2737.2  S>C  error   {"error":{"type":"invalid_request_error","code":"invalid_event","message":"Expected a text WebSocket message; binary frames are not supported.","param":null,"event_id":null}}
 2737.4  C>S  conversation.item.create "Now say bye in two words"; response.create
 3010.3 … 3223.5  the same 14-event cycle for R2, text "See ya!", usage {"total_tokens":144,"input_tokens":139,"output_tokens":5,"input_token_details":{"text_tokens":139,"audio_tokens":0,"image_tokens":0,"cached_tokens":128,"cached_tokens_details":{"text_tokens":128,"audio_tokens":0,"image_tokens":0}},"output_token_details":{"text_tokens":5,"audio_tokens":0}}
 3230.4  S>C  rate_limits.updated   remaining 14999718
 3483.0  S>C  CLOSE (echo of client 1000)
```

Note `error.event_id` was `null` on all four errors even though the client
events had no `event_id` to echo; the outer `event_id` is the server's.

Probe 10, bad bearer (10b, 11, 11b have the same shape with the bodies in the table):

```
  895.7  upgrade 101 Switching Protocols  (same headers as a good key)
  895.6  S>C  TEXT    433 B  {"type":"error","event_id":"event_…","error":{"type":"invalid_request_error","code":"invalid_api_key","message":"Incorrect API key provided: <KEY8>***…AAAA. You can find your API key at https://platform.openai.com/account/api-keys.","param":null,"event_id":null}}
  895.6  S>C  CLOSE    39 B  close_code=3000 reason='invalid_request_error.invalid_api_key'
```

Probe 9, idle:

```
 2107.3  upgrade 101
 2112.4  S>C  session.created
22103.8  S>C  PING  4 B
42362.3  S>C  PING  4 B
62619.8  S>C  PING  4 B
72115.5  C>S  session.update → 72,632 session.updated
```

### 2.3 Server frame taxonomy observed (OpenAI Realtime)

| frame | class for the relay |
|---|---|
| `response.output_text.delta` (and by the same shape `response.output_audio.delta`), `conversation.item.input_audio_transcription.delta` | progress (content) |
| `session.created`, `session.updated`, `input_audio_buffer.speech_started/committed`, `conversation.item.added/done`, `response.created`, `response.output_item.added/done`, `response.content_part.added/done`, `response.output_text.done` | meta / liveness |
| `response.done` (with `response.usage`), `conversation.item.input_audio_transcription.completed` (with `usage`) | per-response / per-item terminal and billing |
| `rate_limits.updated` | meta, arrives **after** `response.done` (43 ms later) |
| `error` | non-fatal by itself (four in a row, session kept working); fatal only when a CLOSE 3000/4000 follows in the same ms |
| PING | liveness, every 20.26 s from the server |

## 3. Facts the relay must honour

1. Inworld checks the credential **lazily, on the first client message, not at upgrade**: every upgrade (good key, bad key, `?key=`, no header) returned 101 with identical headers, and a socket that sends nothing receives nothing for at least 20 s whatever its credential. "101 then silence" is the normal state of an authenticated-but-idle socket, not a failure signal.
2. Inworld: only `Authorization: Basic <key>` works; the `?key=` query form is treated as no credential (code 16 `authentication is required`, `SESSION_TOKEN_INVALID`, `NO_RETRY`).
3. Inworld bad key: after the first client message, ~320 ms later (one round trip) a top-level `error` code 7 `Invalid credentials provided for API key "<first 4 chars>***"` and a server CLOSE 1000 in the same millisecond; missing credential is code 16 with the `InworldStatus` details block. Both echo-scrub cases: code 7 bodies contain a key prefix.
4. Inworld sends no PING, PONG or any frame during 75 s of idle on an open TTS context, and the context is fully usable afterwards; liveness on Inworld = client-driven only.
5. OpenAI sends a WebSocket PING every ~20.3 s (4-byte payload) and nothing else in 70 s idle; the session was still live at 72 s. A relay must answer those PINGs itself (or pass them through) or the upstream will time out.
6. Inworld TTS class map: `result.audioChunk` = progress; `contextCreated` / `flushCompleted` / `contextClosed` = meta (per-utterance terminal is `flushCompleted`, per-context terminal is `contextClosed`); `result.status.code != 0` with a `contextId` = in-context error, socket survives; top-level `error` = connection-level, fatal only when followed by CLOSE (auth, bad model), non-fatal for malformed frames (code 3 `invalid WebSocket request for the selected response protocol`).
7. Inworld STT class map: `result.transcription` (interim/final) = progress; `speechStarted` / `speechStopped` = meta; `result.usage` = terminal + billing; `transcribeConfig` is **not acknowledged**, so the first server frame is `speechStarted`, only once audio flows.
8. OpenAI class map: `*.delta` = progress; `response.done` and `…transcription.completed` = per-response terminal + usage; `rate_limits.updated` = meta and arrives after `response.done`, so `response.done` is not the last frame of a response cycle; `error` = non-fatal by itself.
9. Inworld multiplexing: every server frame carries `result.contextId` (the client's own string, echoed verbatim), including error `status` frames for unknown or over-limit contexts; audio from different contexts interleaves on the socket; the 6th `create` gets `result.status` code 8 "You have reached the limit of 5 TTS contexts per connection…" and the socket stays open; an operation on an unknown context gets code 5 `context <id> not found (payload=SEND_TEXT)`.
10. Provider end-of-session: Inworld never closes a healthy socket itself (after `contextClosed`, after STT `usage`, after 75 s idle); its only server-initiated close is 1000 with an empty reason right after a fatal `error`. OpenAI closes only on fatal errors, with 3000 (`invalid_request_error.invalid_api_key`, `invalid_request_error` for a missing header) or 4000 (`invalid_request_error.invalid_model`, `.missing_model`, `.beta_api_shape_disabled`), reason = `error.type[.code]`. A client 1000 is echoed as 1000 by both.
11. Usage: Inworld TTS `result.audioChunk.usage.processedCharactersCount` is on the **first** audio chunk of each flush (29, 19, 19, 11 observed = exact character count of the text) and `0` on every later chunk, with `modelId` alongside; sum, do not take the last. Inworld STT: a single `result.usage {transcribedAudioMs, modelId}` after `closeStream` (1500 ms for 1.63 s streamed; 3150 ms for 7.6 s streamed with 6 s of silence): provider-counted, not derivable from bytes, and absent if the stream ends without `closeStream`. OpenAI transcription: `conversation.item.input_audio_transcription.completed.usage = {type: "tokens", total_tokens, input_tokens, input_token_details {text_tokens, audio_tokens}, output_tokens}` (16 audio tokens for 1.63 s), no `rate_limits.updated` in a transcription session. OpenAI realtime: `response.done.response.usage` as in item 15.
12. Message size: Inworld accepted a 1,048,627-byte text frame without a transport-level rejection (answered with an ordinary in-context status); the per-`send_text` limit is 2,000 characters (`result.status` code 3 `text length should not exceed 2000 characters.`, and no `flushCompleted` follows the rejected flush). OpenAI rejects BINARY frames with `error invalid_event` ("Expected a text WebSocket message") and non-JSON with `invalid_json`, both non-fatal. Largest server frame seen: Inworld 25,012 B (18,604 B of 16 kHz PCM); OpenAI 1,254 B (`session.created`).
13. OpenAI `error` is non-fatal: four consecutive bad inputs (`{"type":"nope"}`, unknown `session` field, non-JSON, binary) each produced one `error` event and the next `response.create` completed normally on the same socket. The fatal cases are distinguishable only by the CLOSE frame that follows in the same millisecond, not by the event.
14. OpenAI checks auth and the model **after** a 101: bad key, no key, unknown model and missing model all upgrade successfully (same headers) and fail with `error` + CLOSE (3000 auth / 4000 shape) about 0 ms after the handshake. `OpenAI-Beta: realtime=v1` is fatal (4000 `beta_api_shape_disabled`) on both `?model=` and `?intent=transcription`; the relay must strip it. The legacy `transcription_session.update` event and `input_audio_format`-style fields are rejected (`invalid_value` on `type`); the GA shape is `session.update {session: {type: "transcription", audio: {input: {format: {type: "audio/pcm", rate: 24000}, transcription: {model}, turn_detection}}}}` and `rate` must be >= 24000 (`integer_below_min_value`); a rejected `session.update` is discarded whole.
15. Exact `response.done.response.usage` shape: `{"total_tokens": 127, "input_tokens": 120, "output_tokens": 7, "input_token_details": {"text_tokens": 120, "audio_tokens": 0, "image_tokens": 0, "cached_tokens": 64, "cached_tokens_details": {"text_tokens": 64, "audio_tokens": 0, "image_tokens": 0}}, "output_token_details": {"text_tokens": 7, "audio_tokens": 0}}`; the second response in the same session showed `cached_tokens: 128` of 139, so cache reads are real and per-modality even in a text-only session. `rate_limits.updated` carried only `{"name": "tokens", "limit": 15000000, "remaining": …, "reset_seconds": 0.001}` (no `requests` entry).
16. Latency floor from this vantage: connect→101 0.46–1.4 s (Inworld), 0.84–2.1 s (OpenAI); any Inworld request→reply is one RTT (~320 ms) plus a few ms; Inworld first audio 370–455 ms after `send_text`; OpenAI first text delta 476 ms after `response.create`, first transcript delta 1.19 s after `commit`.
17. Inworld LINEAR16 chunks: the first chunk and the tiny final chunk of each flush both begin with a 44-byte RIFF/WAVE header; strip on every `RIFF`-prefixed chunk.

## 4. Contradictions with the sweep docs

- `voice-inworld.md` "with a bad key the socket returned 101 and then nothing for 15 s" and "the rejection arrives as the first text frame": both are artefacts of whether the earlier probe sent a message. On the TTS socket nothing arrives, for any credential, until the client sends; the auth error comes ~320 ms after the first client message, and then the server closes 1000.
- `voice-inworld.md` "`RECOGNITION_USAGE` every 5 s": not observed on `inworld/inworld-stt-1`. In 7.6 s of streamed audio no periodic usage frame arrived; one `result.usage {transcribedAudioMs, modelId}` arrives after `closeStream`.
- `voice-inworld.md` WS in-context error shape "`result.status.code != 0`": confirmed, plus a top-level `error` variant with a string `"status":"INVALID_ARGUMENT"` and no `details` for malformed frames, which is non-fatal (the doc treats top-level `error` as connection-fatal).
- `voice-inworld.md` "`Context not found` (code 5) treated as benign": confirmed benign at the socket level; the message now carries `(payload=SEND_TEXT)`.
- `voice-openai.md` said no `rate_limits.updated` arrived in a one-response session; it does arrive, 43 ms **after** `response.done`. The 16 Sep probe closed too early to see it.
- `voice-openai.md` bad-key expectation of an HTTP 401 (from the REST probe) does not carry over: on the WebSocket it is a 101 then `error` + CLOSE 3000.
- The task brief's `OpenAI-Beta: realtime=v1` and `transcription_session.update` / `input_audio_format: pcm16` are the retired beta shapes; both are rejected live, as `voice-openai.md` predicted.

## 5. Spend

Inworld TTS: 29 + 19 + 19 + 11 = 78 characters synthesised (the 2,100-character
and 1 MiB frames were rejected unbilled): under $0.002 at the $15–25 per 1M
on-demand rates. Inworld STT: 1,500 + 3,150 = 4,650 ms billed (`transcribedAudioMs`):
about $0.0002 at $0.15/h. OpenAI transcription: 16 audio input + 8 output tokens
on `gpt-4o-mini-transcribe`, plus one session with audio appended but no
transcript produced: under $0.0002. OpenAI realtime-mini: 259 text input tokens
(192 cached) + 12 output tokens across two responses: about $0.0002; the idle
session used no tokens. Total for the whole probe: about $0.003. 21 sockets
opened, none left open.
