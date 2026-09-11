# Live-provider feature smoke, 11 Sep 2026

Not a benchmark. One request per feature through the gateway to real providers over real TLS, to confirm each request shape passes through the gateway without being broken. Script: `bench/live_smoke.py` (`python -m bench.live_smoke`). Gateway built in-process with `live.smoke.build_live_app` (`fake_upstreams=False`, the same `build_app` the server uses), on an ephemeral port, capture to a temp JSONL, no tenants file (anonymous tenant, 100 rps).

Run at 2026-09-11 13:55 UTC on the 16-core laptop. Credentials loaded from an env file outside the repo via `live.env`; none printed.

## Result table

| Check | Status | Provider and model | Requests | What was asserted |
|---|---|---|---|---|
| a.stream | PASS | deepseek / deepseek-flash | 1 | 152 SSE events, all valid JSON, text present, usage frame (36 in, 150 out), `[DONE]`, `content-type: text/event-stream`, headers `x-gw-served-by`, `x-gw-attempts=1`, `x-gw-policy-id` present |
| a.nonstream | PASS | deepseek / deepseek-flash | 1 | HTTP 200 JSON, `choices[0].message.content` present, usage (36, 161) |
| b.tools.openai | PASS | openai / gpt-4o-mini | 1 | tool_call delta with id `call_TGJM...`, name `get_weather`, arguments concatenate to `{"city":"Bangalore"}`, `finish_reason=tool_calls`, `[DONE]` |
| b.tools.deepseek | PASS | deepseek / deepseek-flash | 1 | same shape from DeepSeek: id `call_00_ET...`, arguments `{"city": "Bangalore"}`, `finish_reason=tool_calls` |
| c.tool_roundtrip | PASS | openai / gpt-4o-mini | 1 | assistant tool_call plus `tool` role result sent back; text streams: "The current weather in Bangalore is 24°C with cloudy skies.", `finish_reason=stop` |
| d.thinking.anthropic | PASS | anthropic / claude-haiku-4-5 (identity provider) | 1 | `thinking: {enabled, budget 1024}` on `/anthropic/v1/messages`; 44 events; `thinking_delta` frames (192 chars) then `text_delta` "391"; `message_stop`; every `event:` name intact, so the gateway passed the bytes through with no normalisation |
| d.thinking.deepseek | PASS | deepseek / deepseek-v4-pro | 1 | `thinking: {type: enabled}` accepted; `reasoning_content` deltas (90 chars) then content "391"; `[DONE]`; passed through untouched |
| e.vision | PASS | openai / gpt-4o-mini | 1 | 16x16 solid red PNG built in-process (81 bytes) as a data URL; answer "Red." |
| f.images | SKIP | none | 3 | not supported by the gateway, chat-completions and messages only: `/v1/images/generations` 404, `/v1/responses` 501, `/v1/embeddings` 404 |
| g.json_mode | PASS | openai / gpt-4o-mini | 1 | `response_format: json_object`; concatenated stream parses to `{"city": "Bangalore", "country": "India"}` |
| h.cancel | PASS | deepseek / deepseek-flash | 1 | client closed after 3 events; gateway logged "truncating after commitment: client_disconnected"; `llmgw_streams_open` back to 0 in 0.21 s; capture record written: `outcome=canceled, basis=estimated, committed=true, tokens={input 0, output 239}, cost_usd=0.0000669` |
| i.bad_key | PASS | deepseek with a deliberately invalid key | 1 | HTTP 401 passed through, `x-gw-attempts=1`, no retry |
| i.ghost_model | PASS | deepseek, wire model `deepseek-v4-ghost-does-not-exist` | 1 | HTTP 400 passed through ("The supported API model names are deepseek-flash, deepseek-v4-pro"), `x-gw-attempts=1`, no retry |
| i.unknown_workload | PASS | none | 1 | HTTP 400 `policy_error` from the gateway itself, `x-gw-attempts=0`, `x-gw-served-by=-`, no provider contacted |

12 PASS, 0 FAIL, 1 SKIP. Total across the three runs below: 36 requests, about 9,400 input tokens (8,516 of them the one vision call) and about 1,000 output tokens. Gateway-billed cost from the capture files: $0.00196 + $0.00206 + $0.00005, plus two direct probe calls, about $0.005 in all.

## Raw output

Run 2 (all checks), 13:55:36Z:

```
credentials: ANTHROPIC_API_KEY set, OPENAI_API_KEY set, OPENROUTER_API_KEY set, DEEPSEEK_API_KEY set
gateway: http://127.0.0.1:46881  policy=.../policy.toml  capture=.../capture.jsonl
FAIL a.stream: 203 events, text='', reasoning_chars=636, finish=length, usage=(36, 200), DONE=True, bad_json=0, headers={'x-gw-served-by': 'deepseek/deepseek.deepseek-v4-flash', 'x-gw-attempts': '1', 'x-gw-policy-id': 'pol_4f6c3359'}, missing=[], wall=1.67s  [deepseek/deepseek.deepseek-v4-flash]
PASS a.nonstream: HTTP 200, text='Hello, good', reasoning_chars=654, finish=length, usage=(36,200), content-type=application/json  [deepseek/deepseek.deepseek-v4-flash]
PASS b.tools.openai: 10 events, tool_calls=[{'id': 'call_TGJMRc1DlYTbS4rPoPlIja5i', 'name': 'get_weather', 'arguments': '{"city":"Bangalore"}'}], finish=tool_calls, DONE=True, usage=(73, 15)  [openai/openai.gpt-4o-mini]
PASS b.tools.deepseek: 25 events, tool_calls=[{'id': 'call_00_ET_0iLMJD34NTPf05Hq9axo3287', 'name': 'get_weather', 'arguments': '{"city": "Bangalore"}'}], finish=tool_calls, DONE=True, usage=(330, 50)  [deepseek/deepseek.deepseek-v4-flash]
PASS c.tool_roundtrip: 17 events, text='The current weather in Bangalore is 24°C with cloudy skies., finish=stop, usage=(117, 14)  [openai/openai.gpt-4o-mini]
PASS d.thinking.anthropic: 44 events, named events=['content_block_delta', 'content_block_start', 'content_block_stop', 'message_delta', 'message_start', 'message_stop', 'ping'], thinking_chars=192, text='391', message_stop=True, bad_json=0, tokens=(55,128); the gateway forwarded the Anthropic frames as-is (event names intact), no normalisation  [anthropic-identity/anthropic.haiku-4-5-identity]
PASS d.thinking.deepseek: 37 events, reasoning_chars=90, text='391', DONE=True, usage=(97, 35); reasoning_content deltas passed through untouched  [deepseek/deepseek.deepseek-v4-pro]
PASS e.vision: text='Red.', said_red=True, usage=(8516, 2), png_bytes=81  [openai/openai.gpt-4o-mini]
SKIP f.images: not supported by the gateway, chat-completions and messages only: /v1/images/generations -> 404; /v1/responses -> 501; /v1/embeddings -> 404
PASS g.json_mode: 21 events, parsed={'city': 'Bangalore', 'country': 'India'}, usage=(22, 17)  [openai/openai.gpt-4o-mini]
truncating /v1/chat/completions after commitment: client_disconnected
PASS h.cancel: closed after 3 events; streams_open 0.0 -> 0.0 in 0.21s (back to baseline=True); capture record: outcome=canceled, basis=estimated, committed=True, tokens={'input': 0, 'output': 239, 'cache_read': 0, 'cache_write': 0}, cost_usd=6.692e-05, error_code=None  [deepseek/deepseek.deepseek-v4-flash]
PASS i.bad_key: HTTP 401, attempts=1, served_by=-, body='{"error":{"message":"Authentication Fails, Your api key: ****0000 is invalid","type":"authentication_error","param":null'  [badkey]
PASS i.ghost_model: HTTP 400, attempts=1, body='{"error":{"message":"The supported API model names are deepseek-flash, deepseek-v4-pro, but you passed deepseek-v4-ghost'  [ghost]
PASS i.unknown_workload: HTTP 400, attempts=0, served_by=-, body='{"error": {"type": "policy_error", "message": "unknown workload \'no-such-workload\'; known: [\'anthropic\', \'badkey\', \'chat'

summary: 12 PASS, 1 FAIL, 1 SKIP; 16 requests, tokens in=9282 out=661 (from usage frames), wall=14.1s
capture: 12 records, gateway-billed cost $0.00206
```

Run 3 (check a only, after raising its max_tokens to 400), 13:56:06Z:

```
PASS a.stream: 152 events, text='Hello, hope you are well.', reasoning_chars=449, finish=stop, usage=(36, 150), DONE=True, bad_json=0, headers={'x-gw-served-by': 'deepseek/deepseek.deepseek-v4-flash', 'x-gw-attempts': '1', 'x-gw-policy-id': 'pol_4f6c3359'}, missing=[], wall=1.51s  [deepseek/deepseek.deepseek-v4-flash]
PASS a.nonstream: HTTP 200, text='Hello there, how are you?', reasoning_chars=562, finish=stop, usage=(36,161), content-type=application/json  [deepseek/deepseek.deepseek-v4-flash]

summary: 2 PASS, 0 FAIL, 0 SKIP; 2 requests, tokens in=72 out=311 (from usage frames), wall=3.2s
capture: 2 records, gateway-billed cost $0.00005
```

Run 1 (13:53Z) had the same three FAILs as the first version of the script: a.stream and a.nonstream with empty text, and i.unknown_workload. All three were test problems, explained below, not gateway problems.

## The one FAIL that was chased, and what it was

`a.stream` and `a.nonstream` returned `text=''` with `finish_reason=length` at `max_tokens` 32 and again at 200. A direct probe through the gateway showed why: the wire model `deepseek-flash` reasons by default. At 32 tokens the entire budget went to `reasoning_content` (`completion_tokens_details.reasoning_tokens: 32`); at 200, 151 of 159 tokens were reasoning. At 400 it finished with `finish_reason=stop` and text. The gateway forwarded every event and the usage frame correctly in all three cases. This is provider behaviour.

The `i.unknown_workload` FAIL in run 2 was my assertion: the gateway sets `x-gw-served-by: -` (a dash, not absent) when no provider was contacted. Assertion corrected.

## Findings worth carrying

Nothing in this smoke is a gateway bug. Four things are worth writing down.

1. **Catalog says `can_reason=False` for `deepseek.deepseek-v4-flash`; the wire model reasons by default and bills for it.** A small `max_tokens` on this model buys reasoning and no answer. A marketplace catalog must record this per model (reasoning on by default, how to switch it off, whether reasoning tokens are billed as output) because it changes both the customer's bill and the meaning of `max_tokens`.

2. **Canceled streams bill input as 0.** The `h.cancel` capture record has `tokens.input = 0, output = 239, basis = estimated`. The prompt was about 36 tokens. When the usage frame never arrives, the gateway estimates output from what it saw but does not estimate input from the request it forwarded. For a marketplace that is revenue left on the floor on every client cancel; the fix is to count prompt tokens at ingress and carry them into the estimate. Also note the output estimate (239) is larger than the three events the client read: the gateway had already pulled that much from the provider before the disconnect propagated, so the provider will bill for it, which makes 239 the honest number.

3. **Provider error bodies pass through verbatim, including a key fragment.** The 401 body from DeepSeek reads "Your api key: ****0000 is invalid" and reached the client unchanged. Four masked characters of the gateway's own provider key is not a leak today, but in a marketplace where the key belongs to the platform and the client is a tenant, provider error bodies should be rewritten to the gateway's own error shape rather than forwarded.

4. **Vision cost surprise is the provider's, not ours.** An 81-byte 16x16 PNG cost 8,516 input tokens on gpt-4o-mini. That is the model's image token floor. The gateway's usage capture recorded it exactly, which is the point.

## What the gateway does with each shape

Everything is byte-faithful passthrough. Tool-call deltas, `reasoning_content`, Anthropic `thinking_delta`, `response_format`, image data URLs: none are parsed or rewritten on the way through. The gateway reads each event only to classify it (progress, heartbeat, usage, terminal) for its clocks and its usage capture, and `_PROGRESS_KEYS` in `surfaces/openai.py` already includes `tool_calls` and `reasoning_content`, so a stream that is nothing but tool-call deltas or reasoning for many seconds does not trip the progress budget. The only dialect the gateway will not translate is cross-surface: an OpenAI-shaped body cannot fall back to an Anthropic target (documented in `live/smoke.py`).

## Not covered

Streaming tool calls with parallel tool calls (more than one index); Anthropic tool use (`tool_use` content blocks); OpenRouter (no credit on the account); the `/v1/responses` surface (501, not implemented); embeddings and images (no route).
