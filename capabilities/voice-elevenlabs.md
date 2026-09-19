# Voice sweep: ElevenLabs vs llmgw (2026-09-16)

Docs read today at `elevenlabs.io/docs` (several legacy paths 404 and were re-found via search; URLs below are the ones actually read). Repo: `/Users/sanjay/PREP/Evo/llmgw` @ `bc1d065`. Layrs: `/Users/sanjay/Layrs/voice-agent` (read-only).

Fit legend. **Proxyable today**: plain HTTP JSON, or chunked bytes a new surface could pump with small changes. **Needs new transport**: WebSocket/WebRTC. **Needs new accounting**: characters / credits / hours, not tokens. **Not a gateway concern**: control plane. Most rows carry two labels because the data plane is proxyable but the meter is not.

Repo facts every row leans on: routes are exactly two `POST` JSON paths (`server/app.py:237-239`) plus `/v1/responses` 501 (`:286`); `ProviderKind` is `Literal["openai","anthropic"]` (`catalog.py:60`); `upstream.py:309` hard-sets `content-type: application/json` upstream and `NEVER_FORWARDED` strips the client's `content-type`/`content-length` (`config.py:119`), so multipart cannot transit; every upstream chunk is fed to an `SSEParser` with a 1 MiB frame bound (`pump.py:198,366`; `FrameTooLarge` kills the request, `pump.py:64`); the response-header allowlist is `content-type, cache-control, x-accel-buffering` (`app.py:310`); request cap 4 MiB, buffered response cap 8 MiB (`config.py:395,401`); `ModelSpec` prices are per-million tokens in four buckets (`catalog.py:110-121`, `accounting.py:97`); no WebSocket code anywhere (`grep -ri websocket src/` is empty).

## 1. Products and endpoints

| capability | ElevenLabs (doc URL) | protocol/unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| TTS `POST /v1/text-to-speech/{voice_id}` | elevenlabs.io/docs/api-reference/text-to-speech/convert | REST JSON in, whole binary audio out | Proxyable (buffered) + new accounting | `app.py:237` two routes; `executor.py:1281` bounded non-stream drain; `config.py:401` 8 MiB cap | Data plane. 10k chars of mp3_44100_128 ≈ 10 MB: cap must rise per surface |
| TTS `POST …/stream` | …/text-to-speech/stream | REST JSON in, chunked binary audio out | Proxyable with a raw pump mode + new accounting | `pump.py:366` parses every chunk as SSE | Data plane, the one that matters for latency |
| TTS `POST …/stream/with-timestamps` | …/text-to-speech/stream-with-timestamps | JSON in, stream of JSON objects (`audio_base64`, `alignment`) | Proxyable: newline-delimited JSON, not SSE; parser needs a NDJSON mode | `sse.py` SSE-only | Base64 inflates bytes 33% |
| TTS WebSocket `wss …/stream-input` | …/text-to-speech/v-1-text-to-speech-voice-id-stream-input | WS, JSON text frames both ways, base64 audio | Needs new transport | no WS | The LLM-to-speech path real-time agents use |
| TTS multi-context WS `…/multi-stream-input` | …/v-1-text-to-speech-voice-id-multi-stream-input | WS, `context_id` per stream | Needs new transport | — | One socket, many interleaved utterances |
| STT batch `POST /v1/speech-to-text` (Scribe v2) | …/speech-to-text/convert | multipart (`file` ≤ 3–5 GB) or `source_url`, JSON out; `webhook: true` → 202 | **Built and working** (`elevenlabs_stt`, 19 Sep 2026): multipart transits since PLAN-2 B4, and `audio_duration_secs` in the response is the exact hours meter | `upstream.py:309`, `config.py:119`, `config.py:395` 4 MiB | Data plane but batch-shaped |
| STT realtime `wss /v1/speech-to-text/realtime` (Scribe v2 Realtime, ~150 ms) | …/speech-to-text/v-1-speech-to-text-realtime | WS, base64 PCM in, JSON transcripts out | Needs new transport + hours accounting | — | The live-interview STT path |
| Speech-to-speech `POST /v1/speech-to-speech/{voice_id}[/stream]` | …/speech-to-speech/stream | multipart audio in, binary audio out | Not proxyable (multipart) | same | |
| Agents Platform `wss /v1/convai/conversation` (+ WebRTC via conversation token, signed URLs) | …/agents-platform/api-reference/agents-platform/websocket | WS/WebRTC, full duplex, JSON + base64 PCM/µ-law | Needs new transport; arguably not a gateway concern (it is a competing agent runtime) | — | Replaces LLM+TTS+STT; per-minute billing |
| Sound effects, Music (`music_v2_5`), Dubbing, Voice design (`eleven_ttv_v3`), cloning, Voice library, pronunciation dictionaries | elevenlabs.io/docs/overview/models | REST JSON/multipart, mostly async jobs | Not a gateway concern (control plane / offline) | — | Music/SFX are JSON-in binary-out and would ride the same raw pump if ever wanted |
| Usage `GET /v1/usage/character-stats` (deprecated → `/v1/workspace/analytics/query/usage-by-product-over-time`), `GET /v1/user/subscription` | …/usage/get, …/user/subscription/get | REST JSON | Not a gateway concern; useful for a `credential_ok`/quota probe | `/probe` reports only `credential_present` | `subscription` gives `character_count/character_limit/next_character_count_reset_unix`, `status` |
| Data residency bases `api.us.`, `api.eu.residency.`, `api.in.residency.`, `api.sg.residency.elevenlabs.io` | stream-input page (servers list) | base URL | Proxyable: `ProviderConn.base_url` | `catalog.py:73` | India endpoint is relevant to Layrs (Mumbai Supabase, `sin` Fly) |

## 2. Streaming shape

| capability | ElevenLabs (doc URL) | protocol/unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| `/stream`: chunked binary, `audio/mpeg` or raw PCM per `output_format`; no framing, no terminal marker, ends on connection close | …/text-to-speech/stream | one-directional bytes | Raw pump mode needed: "first event" = first body byte, "progress" = any byte, terminal = EOF. Commitment flag transfers unchanged (`pump.py:490` sets before the first send) | `pump.py:410-431` classify/progress on SSE events only; `surfaces/base.py:182-200` `Surface` is typed on `SSEEvent` | Native ending on post-commit failure = just close (no marker to withhold), which is C2's "close the way the provider would" |
| `/stream/with-timestamps`: NDJSON objects with `audio_base64`, `alignment`, `normalized_alignment` | …/stream-with-timestamps | one-directional JSON lines | Proxyable with an NDJSON splitter feeding the same `classify/apply_usage` hooks; each object is a "content" event | `sse.py` handles `data:` frames only | Alignment lets a client do word-highlighting; irrelevant to the gateway |
| WS stream-input: client sends `{"text":" ", voice_settings, generation_config.chunk_length_schedule[120,160,250,290] (50–500 each)}` then `{"text": "...", try_trigger_generation, flush}`, `{"text": ""}` to close; server sends `{"audio": base64, "alignment", "normalizedAlignment"}` and `{"isFinal": true}`; `inactivity_timeout` (docs: 20 s default, ≤180 s); `auto_mode`, `sync_alignment`, `enable_ssml_parsing`, `seed` as query params | …/v-1-text-to-speech-voice-id-stream-input | bidirectional WS; audio base64 in JSON text frames | Needs new transport. Deadline model maps: connect → WS handshake, first_event → first `audio`, progress → any `audio`, client_stall → backpressure on outbound frames. Breaker/admission/credential scoping transfer. Commitment = first `audio` frame relayed | no WS | Concurrency is charged only while the model generates (models page), so idle sockets are cheap upstream |
| Multi-context WS: `initialiseContext`, `sendText{context_id}`, `flushContext`, `keepContextAlive` (empty text resets timeout), `closeContext`, `closeSocket`; per-context `isFinal` | …/multi-stream-input | bidirectional WS | Needs new transport; one upstream socket fanning to N client "streams" breaks the 1:1 request model | — | |
| STT realtime WS: query `model_id`, `audio_format` (pcm_8000…48000, ulaw_8000; default pcm_16000), `commit_strategy manual|vad`, `vad_threshold`, `vad_silence_threshold_secs`, `min_speech/silence_duration_ms`, `include_timestamps`, `keyterms`, `entity_detection`, `enable_logging`; client `input_audio_chunk{audio_base64, commit, sample_rate, previous_text}`; server `session_started`, `partial_transcript`, `committed_transcript`, `committed_transcript_with_timestamps`, `committed_transcript_entities`, errors `auth_error, quota_exceeded, rate_limited, input_error, session_time_limit_exceeded, chunk_size_exceeded, insufficient_audio_activity, transcriber_error` | …/speech-to-text/v-1-speech-to-text-realtime | bidirectional WS; audio in, JSON out | Needs new transport. "Progress" is a partial transcript; a healthy silent user produces none, so the progress clock must key on audio *in* as well as text *out* | `clocks.py:41-55` progress is provider-content only | `session_time_limit_exceeded` implies a max session; value not documented |
| Agents WS: `conversation_initiation_client_data`, `user_audio_chunk`, server `ping{event_id, ping_ms}` requiring `pong`, `audio{audio_base_64,event_id,alignment,is_final}`, `interruption`, `client_tool_call`, `vad_score`, `queue_status`, close code 4300 on queue timeout | …/agents-platform/websocket | full duplex WS or WebRTC | Needs new transport, and the ping/pong is app-level (the gateway would have to answer it or relay within the deadline) | — | |
| Keepalives | WS: inactivity timeout, `keepContextAlive`; HTTP stream: none documented | | For the raw pump, silence = stall; the progress budget (15 s) is far above a 75 ms model | `clocks.py:267` | |

## 3. Latency knobs and metrics

| capability | ElevenLabs (doc URL) | protocol/unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| Model tier: `eleven_flash_v2_5` ~75 ms, `eleven_flash_v2` ~75 ms (English), `eleven_v3_conversational` ~280 ms (WS, recommended for agents), `eleven_v3` / `eleven_multilingual_v2` HTTP-only quality tiers; `eleven_turbo_v2(_5)` **deprecated** | elevenlabs.io/docs/overview/models | model id | Catalog rows needed; `ModelSpec` has no latency class, no "WS-capable" flag, no per-request char limit (5k / 10k / 40k) | `catalog.py:110-121` | Char limit is the analogue of `max_output` |
| `optimize_streaming_latency` 0–4 | convert / stream pages | query param | **Deprecated**; passthrough if sent | — | Do not build on it |
| `chunk_length_schedule`, `auto_mode`, `try_trigger_generation`, `flush` | stream-input | WS config | WS only | — | The real TTFB knobs for LLM-driven speech |
| `output_format`: mp3_22050_32…44100_192, opus_48000_*, pcm_8000…48000, wav_*, alaw_8000, ulaw_8000; default `mp3_44100_128`; high bitrates/PCM 44.1k gated by paid tiers | convert page | query param | Passthrough as query string; the gateway forwards the path+query verbatim? — **check**: `join_url` (`upstream.py:261-279`) joins base + surface path; a new surface must carry `{voice_id}` and the query string through | `app.py:237` fixed paths only | PCM avoids decode latency client-side; bytes/sec: pcm_16000 = 32 KB/s, mp3_44100_128 = 16 KB/s |
| Request stitching `previous_text/next_text`, `previous_request_ids/next_request_ids` (≤3) | convert page | body | Passthrough; the request id comes from the `request-id` response header, which the gateway strips | `app.py:310` | Stitching across a gateway is impossible until `request-id` is forwarded |
| Regional TTFB expectations: 100–150 ms NA/EU/SEA, 150–200 ms South/NE Asia (Flash + WS); `api.us.elevenlabs.io` pins region | elevenlabs.io/docs/best-practices/latency-optimization | — | Provider fact; India residency base is the right pick for Layrs | — | |
| Provider-side metrics: `/usage/character-stats?metric=ttfb_avg|ttfb_p95|concurrency|request_count` | …/usage/get | REST | Not a gateway concern; a cross-check for the gateway's own `time_to_first_event` histogram | `metrics.py` TTFE histogram | |

## 4. Auth and headers

| capability | ElevenLabs (doc URL) | protocol/unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| `xi-api-key` header (only scheme; no Bearer) | elevenlabs.io/docs/api-reference/authentication | header | New `ProviderKind` needed: `build_headers` knows bearer and `x-api-key`+`anthropic-version` only | `upstream.py:312-316` | Trivial addition; Anthropic's `x-api-key` path is 80% of it |
| Restricted keys: endpoint scopes, credit quota per key, IP allowlist | authentication page | control plane | Operator hygiene; a per-tenant quota can live at ElevenLabs instead of the gateway | — | |
| Single-use tokens (`/scribe-token` etc., 15 min TTL) for browser WS; signed URLs / conversation tokens for Agents | …/realtime/client-side-streaming | control plane | Not a gateway concern unless the gateway mints them (it holds the key) | — | Reasonable future role: token minting endpoint per tenant |
| WS auth: `xi-api-key` header or `authorization` / `token` query param | stream-input page | | WS only | — | Query-param secrets would land in access logs; header form only |
| Response headers: `request-id`, `x-trace-id`, `character-cost` (documented on the SDK raw-response page); `history-item-id` widely reported but not found in current docs | elevenlabs.io/docs/api-reference/introduction | headers | All stripped by the allowlist; `character-cost` is the **only per-call meter** for TTS and must be read | `app.py:310` | Without `character-cost` the gateway has to count request characters itself (normalisation changes the billed count) |
| `enable_logging=false` (zero-retention, enterprise) | convert page | query | Passthrough | — | |

## 5. Errors, rate limits, concurrency

| capability | ElevenLabs (doc URL) | protocol/unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| Body `{"detail": {"type", "code", "message", "status" (legacy), "request_id", "param"}}`; 422 validation `detail: [{loc,msg,type}]` | elevenlabs.io/docs/eleven-api/resources/errors | JSON | `errors.py` sniffs `error.type/message` (OpenAI/Anthropic shape); ElevenLabs nests under `detail` → status-only classification | `errors.py:843-910` | Needs a third body-shape reader |
| 400 `validation_error` codes incl. `text_too_long`, `unsupported_model`, `invalid_output_format`, `request_too_large`, `malformed_json` | errors page | | `InvalidRequest` by status: correct | `errors.py:898` | |
| 401 `invalid_api_key`, `missing_api_key`, `unauthorized` | errors page | | `AuthenticationFailed`, credential-scoped breaker, scrubbed: correct | `errors.py:892` | |
| 402 `payment_required` / `insufficient_credits` | errors page | | `InsufficientCredits` (try-next, POLICY): correct | `errors.py:894` | |
| 403 `feature_not_available`, `subscription_required`, `voice_access_denied`, `model_access_denied` | errors page | | **Misclassified**: lumped with 401 → credential breaker opens for a plan/voice problem | `errors.py:892` | Same 403 issue as OpenAI region block, worse here |
| 404 `voice_not_found`, `model_not_found` | errors page | | `ModelNotFound` by status | `errors.py:896` | Voice id is part of the path, so "unknown model" semantics fit |
| 409 `already_processing`, `concurrent_modification` | errors page | | Falls to `UpstreamServerError`, retried | `errors.py:918` | Control-plane mostly |
| 429 `too_many_concurrent_requests` / `concurrent_limit_exceeded`, `system_busy`, `rate_limit_exceeded`; **no `Retry-After` documented**; `system_busy`: "retry and it will succeed" | errors page; elevenlabs.io/docs/help-center/technical/api-error-code-429 | | `RateLimited` NEUTRAL with jittered backoff: correct; Retry-After floor unused | `errors.py:890`, `retry.py:186` | Concurrency is per *plan* per *account*, i.e. per credential: the `ProviderKeyLimiter` connection cap is exactly the right primitive and should be set to the plan number (Pro 10, Scale 15; Flash ≈ 2×) |
| Queueing: over the limit, "requests are processed in a queue alongside lower-priority requests" (~50 ms added); priority 3–5 standard, 6 enterprise; concurrency counted only while generating (WS) | elevenlabs.io/docs/models#concurrency-and-priority | | Same shape as DeepSeek's queue: a queued request is a slow first byte, not an error; first_event 20 s is ample | `clocks.py:251` | |
| STT concurrency separate table (batch 8–60, realtime 6–45 by plan); files > 8 min count `min(4, ceil(secs/480))` slots | elevenlabs.io/docs/overview/capabilities/speech-to-text | | Per-credential limiter would need a per-product dimension | `admission.py` one cap per key | |
| 500 `internal_error`, 503 `service_unavailable`/`maintenance` | errors page | | `UpstreamServerError` / `UpstreamOverloaded`: correct | `errors.py:911-915` | |
| WS error frames (`quota_exceeded`, `rate_limited`, `session_time_limit_exceeded`…) | realtime STT page | JSON over WS | WS only | — | |

## 6. Usage and billing units

| capability | ElevenLabs (doc URL) | protocol/unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| TTS billed per character (credits); v3 / multilingual_v2 $0.10 per 1k chars, v3_conversational / Flash $0.05 per 1k (0.5 credit per char) on the API price list; plan quotas 10k–9.9M chars/month | elevenlabs.io/pricing/api | characters | Needs new accounting: `TOKEN_KINDS` are four token buckets, prices per 1M tokens | `accounting.py:97,292-313`; `catalog.py:110-121` | A `unit` field (`tokens|characters|seconds|minutes`) on `ModelSpec` and a matching bucket is the minimal change; 1 char ≈ 1 unit at `output_per_m`-style rate ×1000 |
| Per-call meter: `character-cost` response header; `/v1/history/{id}` after the fact | introduction page (SDK raw response) | header | Read the header in the new surface's `apply_usage` equivalent; header arrives **before** the body, so the count is exact even on a cut stream | `app.py:310` strips it today | For WS, no per-message meter is documented: count sent characters |
| STT billed per hour of audio: Scribe v2 $0.22/h (+$0.07/h entity detection, +$0.05/h keyterms), Scribe v2 Realtime $0.39/h; multi-channel billed per channel | pricing/api; speech-to-text capability page | hours | Needs new accounting (`seconds` unit); duration is known from the response (`words[].end`) for batch, from audio sent for realtime | — | |
| Agents: $0.08/min speech engine, burst $0.16/min, concurrent-call caps 4–40 by plan | pricing/api | minutes | Not a gateway concern | — | |
| Quota state: `/v1/user/subscription` `character_count`, `character_limit`, `next_character_count_reset_unix`, `status`, `current_overage` | …/user/subscription/get | REST | A `credential_ok` probe (also wanted for DeepSeek `/user/balance`) | `/probe` `credential_present` only | |
| Minimum billing / rounding | not documented | | — | — | Unknown |

## 7. Layrs usage today

| item | finding | evidence |
|---|---|---|
| ElevenLabs in code | **Not used.** The only `eleven` hit in `voice-agent` and `evals` is a number-word regex (`harness/interview/packs/behavioral/evidence.py:253`) | grep |
| Env placeholders | `.env.example` lists `ELEVENLABS_API_KEY` and `ELEVEN_API_KEY` next to `DEEPGRAM_API_KEY`; nothing reads them | `voice-agent/.env.example` |
| Live stack | Inworld TTS (`inworld-tts-1.5-mini`, voice `Aarav`) + Inworld STT (`inworld/inworld-stt-1`, realtime) via LiveKit plugins; fallbacks OpenAI TTS and AssemblyAI STT (Universal-Streaming, socket torn down during idle to save billing) | `harness/config.py:77-91,196`; `framework/skeleton/session.py:87-135`; `main.py:67-68` |
| What a switch to ElevenLabs would depend on | LiveKit's `elevenlabs` plugin is WebSocket-based (stream-input) and would need `eleven_flash_v2_5` or `eleven_v3_conversational`, PCM output, and `chunk_length_schedule` tuning; none of the HTTP endpoints. Not verified against the plugin source in this sweep | — |

## What llmgw has and lacks for this provider

Transfers unchanged: connect / first-event / progress / total deadlines and the "total never resets" arithmetic (`clocks.py`); the commitment flag set before the first client write (`pump.py:490`); byte-bounded backpressure (`pump.py` buffer in bytes, which matters more for 32 KB/s PCM than for text); per-credential concurrency cap (`ProviderKeyLimiter`), which is exactly how ElevenLabs meters concurrency; breaker scoped to provider+model+credential; tenant admission; `accept-encoding: identity` (audio is already compressed; leave it); status-code error taxonomy for 400/401/402/404/429/5xx; capture pipeline; drain.

Missing, in order of how much code each is:

1. **A raw-bytes pump mode.** Today every chunk goes through `SSEParser`; an mp3 stream has no newlines, so the 1 MiB frame bound trips `FrameTooLarge` and the request dies (`pump.py:64,198,366`). Needed: `Surface.framing = "sse" | "ndjson" | "raw"`; in raw mode first byte = first event, every chunk = progress, EOF = terminal, native ending = close. Small (a branch in `pump.run`, no new invariants).
2. **A third `ProviderKind`, `elevenlabs`**: `xi-api-key` injection (`upstream.py:312-316`), no version header, base URL with residency variants. Small.
3. **Path templating and query passthrough.** Routes are fixed strings (`app.py:237`); TTS needs `/v1/text-to-speech/{voice_id}/stream?output_format=…`. `voice_id` should be part of the catalog row (it is what you price and pin), or a path param the surface validates. Medium.
4. **Units other than tokens.** `ModelSpec.unit ∈ {tokens, characters, seconds}` and a fifth accounting bucket; TTS cost = `character-cost` header × rate; STT cost = audio seconds × rate. Read `character-cost` from response headers (the surface currently sees only body events). Medium.
5. **Per-surface request and response caps.** 4 MiB / 8 MiB are text numbers; a 10k-char TTS reply is up to ~10 MB (buffered path) and STT uploads are GBs. Small for TTS (streaming path is unbounded already); multipart for STT is a separate problem.
6. **Multipart forwarding** for STT batch and speech-to-speech: `upstream.py:309` forces `application/json` and `config.py:119` strips the client's `content-type`. Either forward `content-type` + raw body for a multipart surface, or support only the `source_url` JSON variant. Medium.
7. **`detail.*` error body reader** so 403 plan/voice denials do not open the credential breaker, and `code` reaches the client. Small.
8. **WebSocket transport** for stream-input, multi-context, realtime STT and Agents: a new ASGI `websocket` scope handler, an upstream `websockets`/`httpx-ws` client, frame relay in both directions with the deadline model remapped (first `audio` frame = first event; progress must also count *inbound* audio for STT), app-level ping/pong relay for Agents, and a commitment rule (first relayed audio frame). Large; it is a second data plane, not a surface.

## What llmgw would need (concrete, minimal viable TTS support)

- `catalog.py`: `ProviderKind` += `"elevenlabs"`; `ProviderConn("elevenlabs", base_url="https://api.in.residency.elevenlabs.io", api_key_env="ELEVENLABS_API_KEY", max_concurrency=<plan concurrency>)`; `ModelSpec` gains `unit="characters"`, `per_unit_rate` (or reuse `output_per_m` as per-million characters: $50/M for Flash, $100/M for v3), `max_input_chars`, `voice_id`, `latency_class`.
- `surfaces/elevenlabs_tts.py`: `parse_request` reads `text` length (for estimate) and `model_id`; `framing="raw"`; `apply_usage` from the `character-cost` response header; `native_ending` = `b""`.
- `pump.py`: raw framing branch (no parser; `mark_progress` per chunk).
- `app.py`: route `/elevenlabs/v1/text-to-speech/{voice_id}/stream` → upstream same path + query string; response allowlist += `request-id`, `character-cost` (namespace them as `X-Gw-Upstream-*` if preferred).
- `errors.py`: `detail`-shaped body reader; 403 → policy-scoped class, not credential.
- `config.py`: per-surface `max_response_bytes`.
- Tests: a fake ElevenLabs mode in `fakes/upstream.py` emitting chunked bytes with `character-cost`, plus the existing contract shapes (commit, stall, cancel, drain) run against it.

## Gaps ranked (impact on the Layrs voice agent)

1. **No WebSocket transport.** Everything the live agent would actually use — TTS stream-input, realtime STT, Agents — is WS. The HTTP surfaces above help batch article narration, not the interview loop. Until this exists the voice path bypasses the gateway entirely, and so do its breakers, budgets and cost records.
2. **Binary streams kill the pump.** Even the HTTP `/stream` endpoint cannot transit today (`FrameTooLarge` on a newline-free body). One branch in `pump.run` fixes it and unlocks TTS-by-HTTP, music and SFX.
3. **Characters and hours are not a currency the gateway has.** No cost record can be right for TTS/STT until `ModelSpec` and accounting learn a unit other than tokens; `character-cost` is stripped before anyone reads it.
4. **`xi-api-key` cannot be injected.** Two-line gap, but it is a hard stop.
5. **Voice id and query string cannot be routed.** Fixed paths only; the TTS URL carries the voice and the output format.
6. **403 plan/voice/model denials open the credential breaker.** With ElevenLabs the 403 family is the *common* policy failure (`model_access_denied`, `voice_access_denied`, `feature_not_available`), so this misclassification would take the provider offline for every tenant on a per-voice mistake.
7. **Multipart is unforwardable.** STT batch and speech-to-speech need it; only the `source_url` JSON variant of STT could ride the JSON path.
8. **Concurrency is a plan number the gateway does not know.** `max_concurrency` on the provider row must be set to the plan's limit (and roughly doubled for Flash); otherwise the provider's 429 arrives before the gateway's own `ProviderKeyExhausted`, which is the wrong layer to learn it at.

Lower: `request-id` needed for prosody stitching is stripped; `/v1/user/subscription` would make a good `credential_ok`/quota probe; residency base URLs should be a per-provider config, not a code change.

## Doc deltas

- Layrs does not use ElevenLabs, so no deprecated ids are in play. If it ever does: `eleven_turbo_v2` and `eleven_turbo_v2_5` are **deprecated** (use `eleven_flash_v2_5` or `eleven_v3_conversational`); `optimize_streaming_latency` is **deprecated** (use WS `chunk_length_schedule`/`auto_mode`); `scribe_v1` is **deprecated** (use `scribe_v2`, `scribe_v2_realtime`); `cloud_storage_url` on STT is deprecated for `source_url`; `/v1/usage/character-stats` is deprecated for the workspace analytics query endpoint.
- The latency-optimization page describes the HTTP `/stream` endpoint as "Server-sent events"; the API reference and the `with-timestamps` variant show it is chunked binary (and NDJSON respectively). Build for bytes, not SSE.
- The error reference now documents a structured `detail.type/code` body; older help-center pages and many SDK snippets still reference `detail.status` (`too_many_concurrent_requests`, `system_busy`). Read both.
- `voice-agent/.env.example` lists both `ELEVENLABS_API_KEY` and `ELEVEN_API_KEY`; the official SDKs read `ELEVENLABS_API_KEY`. Drop the other to avoid two names for one secret.
- Several documented paths 404 (`/docs/api-reference/introduction` content is the SDK page; `/docs/troubleshooting/errors`, `/docs/agents-platform/api-reference/conversational-ai/websocket`); the live ones are `/docs/eleven-api/resources/errors`, `/docs/agents-platform/api-reference/agents-platform/websocket`, `/docs/api-reference/speech-to-text/v-1-speech-to-text-realtime`.


---

## Corrections applied 19 Sep 2026

The first live calls with a real key. What this sweep got wrong:

1. **"Not proxyable as-is (multipart stripped)"** for `POST /v1/speech-to-text`
   has been false since PLAN-2 B4: multipart bodies transit byte-for-byte
   with the client's boundary, and the `model_id` text field is spliced to
   the wire id by `apply_api_model_multipart`. The surface is
   `elevenlabs_stt`.
2. **"duration is known from the response (`words[].end`)"** — no derivation
   is needed. The response carries **`audio_duration_secs`** at the top
   level (1.84 for a file AssemblyAI measured at 1840 ms), exact and NOT
   rounded. The other top-level keys are `language_code`,
   `language_probability`, `text`, `words[]`, `transcription_id`.
   The response ALSO carries `character-cost`, `fiat-cost-before-overages`
   and `fiat-currency` headers — ElevenLabs' own credit meter, which this
   file never mentions and which the gateway deliberately does not bill on
   for a per-hour row.
3. **"India residency base is the right pick for Layrs"** is wrong for this
   account. `api.in.residency.elevenlabs.io` answers **400
   `{"detail":{"type":"authentication_error","code":"invalid_api_key"}}`** to
   the same key `api.elevenlabs.io` accepts, on both TTS and STT: residency
   endpoints need a residency-enabled (Enterprise) key. The `elevenlabs`
   provider row now points at the global host. Revisit if the account is
   ever upgraded.
4. **A bad key is NOT always a 400.** `POST /v1/speech-to-text` answers a bad
   key with a plain **401** (`detail.code: "unauthorized"`), unlike the TTS
   host's 400 that `errors._AUTH_400_HINTS` exists for. Both shapes are now
   fixtures.
5. **Free-tier voices.** A library voice (e.g. `21m00Tcm4TlvDq8ikWAM`) is a
   **402 `paid_plan_required`** on the free tier; the premade voices from
   `GET /v1/voices` work. The live smoke's default voice changed for this.
6. **§6 pricing.** Scribe v2 at $0.22/h is confirmed on
   elevenlabs.io/pricing/api (19 Sep 2026), the same rate on every plan from
   Free to Business. The catalog row is `elevenlabs.scribe-v2`.
7. The available `model_id`s, from the provider's own 400: `scribe_v1`,
   `scribe_v1_experimental`, `scribe_v2`, `scribe_v2_medical`.
