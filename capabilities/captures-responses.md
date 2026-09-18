# Live captures: OpenAI Responses API and DeepSeek Responses endpoint

Captured 2026-09-17 23:08-23:09 UTC with a stdlib `http.client` probe (raw bytes, no SDK).
Raw per-probe captures (verbatim SSE/JSON, credential-scrubbed) live in the session scratchpad
`phaseF-probe/` (`pNN_*.txt` = headers + body, `pNN_*.raw` = body bytes only, `probe.py` = the script).
Ids are real and left in place; keys were never written anywhere.

Total spend estimate: about USD 0.04 (14 gpt-4o-mini calls at 8-384 tokens, 2 gpt-5-nano calls,
one `web_search_preview` call which dominates at roughly USD 0.025-0.03; DeepSeek: 11 calls, under USD 0.001).

## OpenAI (`https://api.openai.com/v1/responses`)

| # | Request | Status | Key headers | Notable fields |
|---|---------|--------|-------------|----------------|
| 1 | `GET /v1/models` | 200 | `content-type: application/json` | 136 models. Presence: `gpt-4o-mini` yes, `gpt-4.1-mini` yes, `gpt-4.1-nano` yes, `gpt-5-mini` yes, `gpt-5-nano` yes, `gpt-5` yes, `o4-mini` yes |
| 2 | non-stream `gpt-4o-mini`, "Say hi in three words.", `max_output_tokens: 32` | 200 | `x-request-id: req_40c1511511a942b8aabeb7ee33d72530`, `openai-processing-ms: 1985`, `content-type: application/json`, `openai-version: 2020-10-01` | `status: completed`, `model: gpt-4o-mini-2024-07-18` (snapshot echoed), `output[0].type: message`, `incomplete_details: null`, `error: null`, usage 13 in / 6 out / 19 total, `store: true`, `service_tier: default` |
| 3 | same + `stream: true` | 200 | `content-type: text/event-stream; charset=utf-8`, `transfer-encoding: chunked`, `x-request-id: req_de5373985bf54b45bdc112bd8a09502f`, `openai-processing-ms: 1254` | 14 events, `event:` line AND `data.type` on every frame (always equal), `sequence_number` 0..13, NO `data: [DONE]`, terminal `response.completed` carries `response.usage` |
| 4 | stream `gpt-5-nano`, "17*23", `reasoning.effort: low`, cap 64 | 200 | as above | `status: completed` at 64 (no retry needed). `output: [reasoning, message]`; no `response.reasoning*` events at all: the reasoning item is `output_item.added` then `output_item.done` with `content: []`, `summary: []` and an `encrypted_content` blob (sent even though `include` was not requested). usage 19 in / 59 out (`reasoning_tokens: 0`!) / 78 total. `model: gpt-5-nano-2025-08-07` |
| 4b | stream `gpt-5-nano`, no `reasoning` param, cap 256 | 200 | | `reasoning.effort` echoed as `medium`, `mode: standard`, `context: current_turn`. usage 19 in / 100 out (`reasoning_tokens: 64`) / 119 total. Same event shape as 4 |
| 5 | stream `gpt-4o-mini`, essay, cap 16 | 200 | | 24 events, terminal is `response.incomplete` (seq 23) with `response.status: incomplete`, `incomplete_details.reason: max_output_tokens`, message item `status: incomplete`; NO `response.completed`. usage 16 in / 16 out / 32 |
| 5' | same, non-stream | 200 | | `status: incomplete`, `incomplete_details: {"reason":"max_output_tokens"}`, `completed_at: null`; body also carries `billing: {"payer":"openai"}` (present in some bodies, not others; treat as optional) |
| 6A | non-stream "My favourite colour is teal. Reply OK.", cap 16 | 200 | | `id: resp_091a414f8db4fab5006aac72e3817887d1ab1ef065b5756096`, `store: true`, usage 16/3/19 |
| 6B | non-stream + `previous_response_id` = A | 200 | | `previous_response_id` echoed; answer recalls turn A; usage 32 in (`cached_tokens: 0`, no cache hit at this size) / 18 out / 50 |
| 6C | `previous_response_id: resp_000000000000000000000000000000` | 400 | | `error.type: invalid_request_error`, `error.code: previous_response_not_found`, `error.param: previous_response_id` |
| 7 | non-stream `background: true`, "hi", cap 16 | 200 | | `status: queued`, `background: true`, `output: []`, `usage: null`, `completed_at: null`, `service_tier: auto`. `GET /v1/responses/{id}` 2 s later: `status: completed`, `service_tier: default`, full `output` + `usage` 8/10/18 (one poll sufficed) |
| 7' | `background: true` + `stream: true` | 200 | `text/event-stream` | Streams normally. 18 events: `response.created` (status `queued`) -> extra `response.queued` -> `response.in_progress` -> deltas -> `response.completed`. No cursor/`starting_after` needed for a fresh request |
| 8a | model `gpt-nope` | 404 | | `error.code: model_not_found`, `error.type: invalid_request_error`, `error.param: null` |
| 8b | `max_output_tokens: "lots"` | 400 | | `error.code: invalid_type`, `error.param: max_output_tokens` |
| 8c | stream + `tools:[{"type":"web_search_preview"}]`, cap 64 | 200 | | 71 events: `output_item.added` (`web_search_call`, `action.type: search`), `response.web_search_call.in_progress` / `.searching` / `.completed` (item_id, output_index, sequence_number only), `output_item.done` (action has `queries` + `query`), then the message. usage 310 in / 74 out / 384 (output exceeded the 64 cap yet `status: completed`); `tool_usage.web_search.num_requests: 1`; `annotations: []` (cut before citation); tools echoed expanded with `search_context_size`, `user_location` |
| 9 | model `gpt-4o-mini-2024-07-18`, cap 16 | 200 | | Accepted, echoed unchanged. (A first attempt with cap 8 got 400 `integer_below_min_value`: "Expected a value >= 16") |

### Excerpt: non-streamed body (probe 2, verbatim)

```json
{
  "id": "resp_01eecaf64d828a82006aac72d116c487d18f04516cc8bdcf08",
  "object": "response",
  "created_at": 1789686481,
  "status": "completed",
  "background": false,
  "completed_at": 1789686482,
  "error": null,
  "frequency_penalty": 0.0,
  "incomplete_details": null,
  "instructions": null,
  "max_output_tokens": 32,
  "max_tool_calls": null,
  "model": "gpt-4o-mini-2024-07-18",
  "moderation": null,
  "output": [
    {
      "id": "msg_01eecaf64d828a82006aac72d2490887d1a2274af798ad3c3f",
      "type": "message",
      "status": "completed",
      "content": [
        {"type": "output_text", "annotations": [], "logprobs": [], "text": "Hello there, friend!"}
      ],
      "role": "assistant"
    }
  ],
  "parallel_tool_calls": true,
  "presence_penalty": 0.0,
  "previous_response_id": null,
  "prompt_cache_key": null,
  "prompt_cache_retention": "in_memory",
  "reasoning": {"context": null, "effort": null, "summary": null},
  "safety_identifier": null,
  "service_tier": "default",
  "store": true,
  "temperature": 1.0,
  "text": {"format": {"type": "text"}, "verbosity": "medium"},
  "tool_choice": "auto",
  "tool_usage": {
    "image_gen": {"input_tokens": 0, "input_tokens_details": {"image_tokens": 0, "text_tokens": 0},
                  "output_tokens": 0, "output_tokens_details": {"image_tokens": 0, "text_tokens": 0}, "total_tokens": 0},
    "web_search": {"num_requests": 0}
  },
  "tools": [],
  "top_logprobs": 0,
  "top_p": 1.0,
  "truncation": "disabled",
  "usage": {
    "input_tokens": 13,
    "input_tokens_details": {"cache_write_tokens": 0, "cached_tokens": 0},
    "output_tokens": 6,
    "output_tokens_details": {"reasoning_tokens": 0},
    "total_tokens": 19
  },
  "user": null,
  "metadata": {}
}
```

### Excerpt: streamed event sequence (probe 3)

Every frame is exactly `event: <name>\ndata: <json>\n\n`; the `event:` name always equals `data.type`.

```
seq  event / type
0    response.created            (response.status = in_progress, usage = null)
1    response.in_progress
2    response.output_item.added  (item.type = message, status in_progress, content [])
3    response.content_part.added (part.type = output_text, text "")
4-9  response.output_text.delta  x6
10   response.output_text.done   (full text)
11   response.content_part.done
12   response.output_item.done   (item.status = completed)
13   response.completed          (response.status = completed, response.usage populated)
<stream closes; no data: [DONE]>
```

Raw first frame (verbatim bytes, wrapped):

```
event: response.created
data: {"type":"response.created","response":{"id":"resp_0920e7981f0eb7ff006aac72d394c487d19d9e9d1b4ae121ba","object":"response","created_at":1789686483,"status":"in_progress","background":false,"completed_at":null,"error":null,"frequency_penalty":0.0,"incomplete_details":null,"instructions":null,"max_output_tokens":32,"max_tool_calls":null,"model":"gpt-4o-mini-2024-07-18","moderation":null,"output":[],"parallel_tool_calls":true,"presence_penalty":0.0,"previous_response_id":null,"prompt_cache_key":null,"prompt_cache_retention":"in_memory","reasoning":{"context":null,"effort":null,"summary":null},"safety_identifier":null,"service_tier":"auto","store":true,"temperature":1.0,"text":{"format":{"type":"text"},"verbosity":"medium"},"tool_choice":"auto","tool_usage":{"image_gen":{"input_tokens":0,"input_tokens_details":{"image_tokens":0,"text_tokens":0},"output_tokens":0,"output_tokens_details":{"image_tokens":0,"text_tokens":0},"total_tokens":0},"web_search":{"num_requests":0}},"tools":[],"top_logprobs":0,"top_p":1.0,"truncation":"disabled","usage":null,"user":null,"metadata":{}},"sequence_number":0}
```

`response.output_item.added` / `response.content_part.added` / one delta / `response.output_text.done`:

```
data: {"type":"response.output_item.added","item":{"id":"msg_0920e7981f0eb7ff006aac72d4b93887d188e7203f22964398","type":"message","status":"in_progress","content":[],"role":"assistant"},"output_index":0,"sequence_number":2}
data: {"type":"response.content_part.added","content_index":0,"item_id":"msg_0920e7981f0eb7ff006aac72d4b93887d188e7203f22964398","output_index":0,"part":{"type":"output_text","annotations":[],"logprobs":[],"text":""},"sequence_number":3}
data: {"type":"response.output_text.delta","content_index":0,"delta":"Hello","item_id":"msg_0920e7981f0eb7ff006aac72d4b93887d188e7203f22964398","logprobs":[],"obfuscation":"zb2VZr3uMaj","output_index":0,"sequence_number":4}
data: {"type":"response.output_text.done","content_index":0,"item_id":"msg_0920e7981f0eb7ff006aac72d4b93887d188e7203f22964398","logprobs":[],"output_index":0,"sequence_number":10,"text":"Hello, how's it?"}
```

Note the `obfuscation` padding field on every delta (random string; must be passed through or dropped, never parsed).

`response.completed` (verbatim):

```
event: response.completed
data: {"type":"response.completed","response":{"id":"resp_0920e7981f0eb7ff006aac72d394c487d19d9e9d1b4ae121ba","object":"response","created_at":1789686483,"status":"completed","background":false,"completed_at":1789686484,"error":null,"frequency_penalty":0.0,"incomplete_details":null,"instructions":null,"max_output_tokens":32,"max_tool_calls":null,"model":"gpt-4o-mini-2024-07-18","moderation":null,"output":[{"id":"msg_0920e7981f0eb7ff006aac72d4b93887d188e7203f22964398","type":"message","status":"completed","content":[{"type":"output_text","annotations":[],"logprobs":[],"text":"Hello, how's it?"}],"role":"assistant"}],"parallel_tool_calls":true,"presence_penalty":0.0,"previous_response_id":null,"prompt_cache_key":null,"prompt_cache_retention":"in_memory","reasoning":{"context":null,"effort":null,"summary":null},"safety_identifier":null,"service_tier":"default","store":true,"temperature":1.0,"text":{"format":{"type":"text"},"verbosity":"medium"},"tool_choice":"auto","tool_usage":{"image_gen":{"input_tokens":0,"input_tokens_details":{"image_tokens":0,"text_tokens":0},"output_tokens":0,"output_tokens_details":{"image_tokens":0,"text_tokens":0},"total_tokens":0},"web_search":{"num_requests":0}},"tools":[],"top_logprobs":0,"top_p":1.0,"truncation":"disabled","usage":{"input_tokens":13,"input_tokens_details":{"cache_write_tokens":0,"cached_tokens":0},"output_tokens":7,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":20},"user":null,"metadata":{}},"sequence_number":13}
```

Note `service_tier` flips from `auto` in `response.created` to `default` in `response.completed`.

### Excerpt: `response.incomplete` (probe 5, verbatim)

```
event: response.incomplete
data: {"type":"response.incomplete","response":{"id":"resp_0ebc56db5c0ec906006aac72df961c87d187505ffe8929bff7","object":"response","created_at":1789686495,"status":"incomplete","background":false,"completed_at":null,"error":null,"frequency_penalty":0.0,"incomplete_details":{"reason":"max_output_tokens"},"instructions":null,"max_output_tokens":16,"max_tool_calls":null,"model":"gpt-4o-mini-2024-07-18","moderation":null,"output":[{"id":"msg_0ebc56db5c0ec906006aac72e0c1a087d18fe6d5f0d91b8538","type":"message","status":"incomplete","content":[{"type":"output_text","annotations":[],"logprobs":[],"text":"Rivers are vital components of Earth's hydrological system, serving as lifelines for"}],"role":"assistant"}],"parallel_tool_calls":true,"presence_penalty":0.0,"previous_response_id":null,"prompt_cache_key":null,"prompt_cache_retention":"in_memory","reasoning":{"context":null,"effort":null,"summary":null},"safety_identifier":null,"service_tier":"default","store":true,"temperature":1.0,"text":{"format":{"type":"text"},"verbosity":"medium"},"tool_choice":"auto","tool_usage":{"image_gen":{"input_tokens":0,"input_tokens_details":{"image_tokens":0,"text_tokens":0},"output_tokens":0,"output_tokens_details":{"image_tokens":0,"text_tokens":0},"total_tokens":0},"web_search":{"num_requests":0}},"tools":[],"top_logprobs":0,"top_p":1.0,"truncation":"disabled","usage":{"input_tokens":16,"input_tokens_details":{"cache_write_tokens":0,"cached_tokens":0},"output_tokens":16,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":32},"user":null,"metadata":{}},"sequence_number":23}
```

Preceding frames are identical to the completed case (`output_text.done`, `content_part.done`, `output_item.done` with item `status: incomplete`). `completed_at` stays `null`.

### Excerpt: reasoning model (probes 4 / 4b, gpt-5-nano)

Event sequence (identical at effort low and medium; no `response.reasoning*` events were emitted because no `reasoning.summary` was requested):

```
0 response.created  1 response.in_progress
2 response.output_item.added   item.type = reasoning, content [], summary [], encrypted_content "gAAAA..." (1.6 KB)
3 response.output_item.done    same reasoning item
4 response.output_item.added   item.type = message
5 response.content_part.added  6 response.output_text.delta ("391")  7 response.output_text.done
8 response.content_part.done   9 response.output_item.done  10 response.completed
```

Usage objects:

```json
{"input_tokens":19,"input_tokens_details":{"cache_write_tokens":0,"cached_tokens":0},"output_tokens":59,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":78}     // effort low, model gpt-5-nano-2025-08-07
{"input_tokens":19,"input_tokens_details":{"cache_write_tokens":0,"cached_tokens":0},"output_tokens":100,"output_tokens_details":{"reasoning_tokens":64},"total_tokens":119}   // effort medium (default)
```

Echoed `reasoning` object on gpt-5: `{"context":"current_turn","effort":"medium","mode":"standard","summary":null}` (gpt-4o-mini echoes `{"context":null,"effort":null,"summary":null}`).
At effort low the reasoning item still appeared and 58 of 59 output tokens are unaccounted for by visible text yet `reasoning_tokens` is 0; do not assume `output_tokens - reasoning_tokens == visible tokens`.

### Excerpt: background (probe 7)

POST response (200, immediately):

```json
{"id":"resp_0424f82d76ccade0006aac72e635cc87d1ae4c81ee34dadef9","object":"response","created_at":1789686502,"status":"queued","background":true,"completed_at":null,"error":null,"incomplete_details":null,"max_output_tokens":16,"model":"gpt-4o-mini-2024-07-18","output":[],"previous_response_id":null,"service_tier":"auto","store":true,"usage":null, "...": "all other fields as in probe 2"}
```

`GET /v1/responses/resp_0424...` after 2 s (200, `openai-processing-ms: 194`): same envelope with `status: completed`, `background: true`, `completed_at: 1789686504`, `service_tier: default`, `output[message]`, `usage: {input 8, output 10, total 18}`.

`background: true` + `stream: true`: a normal SSE stream with one extra frame:

```
0 response.created      (response.status = "queued")
1 response.queued       (response.status = "queued")     <-- only in background mode
2 response.in_progress  ... 17 response.completed
```

### Excerpt: error bodies (verbatim)

```
HTTP 400  (probe 6C, bad previous_response_id)
{"error": {"message": "Previous response with id 'resp_000000000000000000000000000000' not found.", "type": "invalid_request_error", "param": "previous_response_id", "code": "previous_response_not_found"}}

HTTP 404  (probe 8a, unknown model)
{"error": {"message": "The model `gpt-nope` does not exist or you do not have access to it.", "type": "invalid_request_error", "param": null, "code": "model_not_found"}}

HTTP 400  (probe 8b, wrong type)
{"error": {"message": "Invalid type for 'max_output_tokens': expected an integer, but got a string instead.", "type": "invalid_request_error", "param": "max_output_tokens", "code": "invalid_type"}}

HTTP 400  (probe 9 first attempt, cap 8)
{"error": {"message": "Invalid 'max_output_tokens': integer below minimum value. Expected a value >= 16, but got 8 instead.", "type": "invalid_request_error", "param": "max_output_tokens", "code": "integer_below_min_value"}}
```

All errors: `content-type: application/json`, `x-request-id` present, envelope `{"error":{message,type,param,code}}`. No mid-stream error was triggered in this run; per the published spec it is `event: error` / `data: {"type":"error","code":...,"message":...,"param":...,"sequence_number":N}` or a terminal `response.failed` carrying `response.error`. Treat as unverified.

### Excerpt: web search (probe 8c)

```
data: {"type":"response.output_item.added","item":{"id":"ws_0510c28bdc947c26006aac72f1c32c87d187d9fe360553f1a6","type":"web_search_call","status":"in_progress","action":{"type":"search"}},"output_index":0,"sequence_number":2}
data: {"type":"response.web_search_call.in_progress","item_id":"ws_0510c28bdc947c26006aac72f1c32c87d187d9fe360553f1a6","output_index":0,"sequence_number":3}
data: {"type":"response.web_search_call.searching","item_id":"ws_...","output_index":0,"sequence_number":4}
data: {"type":"response.web_search_call.completed","item_id":"ws_...","output_index":0,"sequence_number":5}
data: {"type":"response.output_item.done","item":{"id":"ws_...","type":"web_search_call","status":"completed","action":{"type":"search","queries":["top headlines news today"],"query":"top headlines news today"}},"output_index":0,"sequence_number":6}
```

Final usage: `{"input_tokens":310,"input_tokens_details":{"cache_write_tokens":0,"cached_tokens":0},"output_tokens":74,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":384}`; `response.tool_usage.web_search.num_requests: 1`. Message `output_index` is 1 (the tool call took 0).

## DeepSeek (`https://api.deepseek.com`)

Model sent: `deepseek-v4-flash` (accepted, echoed as `deepseek-flash`). Error message for a bad model lists the canonical names: `deepseek-flash, deepseek-v4-pro`.
No `x-request-id`/processing-ms style headers; only `content-type`, `date`, `transfer-encoding: chunked`, `server: elb`.

| # | Request | Status | Key headers | Notable fields |
|---|---------|--------|-------------|----------------|
| 10a | `POST /v1/responses` non-stream, "Say hi.", cap 32 | 200 | `content-type: application/json` | Works. `id` is a bare UUID (`8434d721-9b0f-4aef-ac21-9d978585223c`, no `resp_` prefix), `model: deepseek-flash`, `store: false`, `status: incomplete` (`max_output_tokens`) because 32 tokens were all spent on reasoning: `output: [reasoning]` only, usage 33 in / 32 out (`reasoning_tokens: 32`) / 65 |
| 10b | `POST /responses` (no `/v1`) | 200 | same | Identical behaviour and shape. Both paths work |
| 10c | `POST /beta/chat/completions` chat body | 200 | | chat.completion with `reasoning_content` |
| 10d | `POST /beta/v1/chat/completions` | 200 | | identical |
| 10e | `POST /v1/chat/completions` (control) | 200 | | identical |
| 11a | stream `/v1/responses`, "17*23", cap 64, no reasoning param | 200 | `content-type: text/event-stream; charset=utf-8` | 32 events, `event:` + `data.type`, `sequence_number` 0..31, no `[DONE]`. Reasoning streams in clear: `response.reasoning_text.delta` x18 / `.done` inside a `reasoning` item whose part type is `reasoning_text`. `output: [reasoning, message]`; message item has extra `"phase":"final_answer"`. usage 43 in / 20 out (`reasoning_tokens: 18`) / 63 |
| 11b | same + `"reasoning":{"effort":"low"}` | 200 | | Accepted; echoed `reasoning: {"effort":"low","summary":null}`. Same events; usage 43/21 (19 reasoning)/64 |
| 11c | same + `"output_config":{"effort":"low"}` | 200 | | Silently ignored: `reasoning.effort` echoed `null`, no `output_config` key in response. Same events/usage as 11b |
| 12a | model `deepseek-nope` | 400 | `content-type: application/octet-stream` (!) | `{"error":{"message":"The supported API model names are deepseek-flash, deepseek-v4-pro, but you passed deepseek-nope.","type":"invalid_request_error","param":null,"code":"invalid_request_error"}}` |
| 12b | `previous_response_id: resp_000...` | 200 | | NOT rejected. Silently ignored; `previous_response_id: null` echoed, normal (incomplete at cap 16) response |
| 12c | `background: true` | 200 | | NOT rejected. Silently ignored; `background: false` echoed, synchronous full response |

### Excerpt: DeepSeek non-streamed body (probe 10a, verbatim)

```json
{"id":"8434d721-9b0f-4aef-ac21-9d978585223c","object":"response","created_at":1789686564,"status":"incomplete","background":false,"completed_at":1789686565,"content_filters":null,"error":null,"frequency_penalty":0.0,"incomplete_details":{"reason":"max_output_tokens"},"instructions":null,"max_output_tokens":32,"max_tool_calls":null,"model":"deepseek-flash","moderation":null,"output":[{"type":"reasoning","id":"01b3a6bc-1bca-4951-aaa5-e6c96f7907d7","status":"incomplete","content":[{"type":"reasoning_text","text":"The user just said \"Say hi.\" That's a simple request. I should respond with a greeting. Keep it brief and friendly. No need for tools or"}],"summary":[],"encrypted_content":"8434d721-9b0f-4aef-ac21-9d978585223c-0"}],"parallel_tool_calls":true,"presence_penalty":0.0,"previous_response_id":null,"prompt_cache_key":null,"prompt_cache_retention":null,"reasoning":{"effort":null,"summary":null},"safety_identifier":null,"service_tier":"default","store":false,"temperature":1.0,"text":{"format":{"type":"text"},"verbosity":null},"tool_choice":"auto","tools":[],"top_logprobs":0,"top_p":1.0,"truncation":"disabled","usage":{"input_tokens":33,"input_tokens_details":{"cached_tokens":0},"output_tokens":32,"output_tokens_details":{"reasoning_tokens":32},"total_tokens":65},"user":null,"metadata":{}}
```

Key diff vs OpenAI envelope: DeepSeek adds `content_filters`, lacks `tool_usage`; `usage.input_tokens_details` has only `cached_tokens` (no `cache_write_tokens`); `completed_at` is set even when `incomplete`; `encrypted_content` is `"<response-id>-<index>"`, not an opaque blob; `store` is always `false`.

### Excerpt: DeepSeek streamed sequence (probe 11a)

```
0  response.created               (status in_progress, usage null, model deepseek-flash)
1  response.in_progress
2  response.output_item.added     item {type: reasoning, status: in_progress, content [], summary [], encrypted_content "<id>-0"}
3  response.content_part.added    part {type: reasoning_text, text ""}
4-21 response.reasoning_text.delta  {content_index, delta, item_id, output_index, sequence_number}   (no obfuscation field)
22 response.reasoning_text.done   {..., text: "<full reasoning>"}
23 response.content_part.done     part {type: reasoning_text, text}
24 response.output_item.done      reasoning item, status completed, content [{type: reasoning_text, text}]
25 response.output_item.added     item {type: message, ...}
26 response.content_part.added    part {type: output_text}
27 response.output_text.delta     {"delta":"391","logprobs":[],...}   (no obfuscation field)
28 response.output_text.done  29 response.content_part.done  30 response.output_item.done
31 response.completed
```

Verbatim frames:

```
event: response.created
data: {"type":"response.created","response":{"id":"bc5f4f54-80e9-4928-a5b2-ee0bfb874556","object":"response","created_at":1789686568,"status":"in_progress","background":false,"completed_at":null,"content_filters":null,"error":null,"frequency_penalty":0.0,"incomplete_details":null,"instructions":null,"max_output_tokens":64,"max_tool_calls":null,"model":"deepseek-flash","moderation":null,"output":[],"parallel_tool_calls":true,"presence_penalty":0.0,"previous_response_id":null,"prompt_cache_key":null,"prompt_cache_retention":null,"reasoning":{"effort":null,"summary":null},"safety_identifier":null,"service_tier":"default","store":false,"temperature":1.0,"text":{"format":{"type":"text"},"verbosity":null},"tool_choice":"auto","tools":[],"top_logprobs":0,"top_p":1.0,"truncation":"disabled","usage":null,"user":null,"metadata":{}},"sequence_number":0}

event: response.reasoning_text.delta
data: {"type":"response.reasoning_text.delta","content_index":0,"delta":"We","item_id":"72005f0a-03c3-441e-b9b4-c0af51afc868","output_index":0,"sequence_number":4}

event: response.output_text.delta
data: {"type":"response.output_text.delta","content_index":0,"delta":"391","item_id":"8b5deacb-cb1f-434d-b581-efcd9f481fc5","logprobs":[],"output_index":1,"sequence_number":27}

event: response.completed
data: {"type":"response.completed","response":{"id":"bc5f4f54-80e9-4928-a5b2-ee0bfb874556","object":"response","created_at":1789686568,"status":"completed","background":false,"completed_at":1789686569,"content_filters":null,"error":null,"frequency_penalty":0.0,"incomplete_details":null,"instructions":null,"max_output_tokens":64,"max_tool_calls":null,"model":"deepseek-flash","moderation":null,"output":[{"type":"reasoning","id":"72005f0a-03c3-441e-b9b4-c0af51afc868","status":"completed","content":[{"type":"reasoning_text","text":"We need answer only number. 17*23=391. Need final only number."}],"summary":[],"encrypted_content":"bc5f4f54-80e9-4928-a5b2-ee0bfb874556-0"},{"type":"message","id":"8b5deacb-cb1f-434d-b581-efcd9f481fc5","status":"completed","content":[{"type":"output_text","annotations":[],"logprobs":[],"text":"391"}],"phase":"final_answer","role":"assistant"}],"parallel_tool_calls":true,"presence_penalty":0.0,"previous_response_id":null,"prompt_cache_key":null,"prompt_cache_retention":null,"reasoning":{"effort":null,"summary":null},"safety_identifier":null,"service_tier":"default","store":false,"temperature":1.0,"text":{"format":{"type":"text"},"verbosity":null},"tool_choice":"auto","tools":[],"top_logprobs":0,"top_p":1.0,"truncation":"disabled","usage":{"input_tokens":43,"input_tokens_details":{"cached_tokens":0},"output_tokens":20,"output_tokens_details":{"reasoning_tokens":18},"total_tokens":63},"user":null,"metadata":{}},"sequence_number":31}
```

### Excerpt: DeepSeek error / ignored-param bodies

```
HTTP 400, content-type: application/octet-stream  (probe 12a)
{"error":{"message":"The supported API model names are deepseek-flash, deepseek-v4-pro, but you passed deepseek-nope.","type":"invalid_request_error","param":null,"code":"invalid_request_error"}}

HTTP 200 (probe 12b, previous_response_id sent)   -> "previous_response_id":null, "store":false, normal response
HTTP 200 (probe 12c, background:true sent)        -> "background":false, synchronous completed/incomplete response
```

## Facts the surface must honour

- Terminal events: exactly one of `response.completed`, `response.incomplete` (cap hit; `response.incomplete_details.reason == "max_output_tokens"`, `completed_at` null) or, per spec but not observed, `response.failed`; after it the connection closes.
- There is NO `data: [DONE]` sentinel on either provider; end-of-stream is the terminal event plus socket close.
- Every frame has both an `event:` line and `data.type`, and they are always identical; every frame carries a monotonically increasing `sequence_number` starting at 0 (both providers).
- Usage lives only in the terminal event's `response.usage` (and in the non-streamed body); `response.created`/`response.in_progress` carry `usage: null`.
- Usage shape: `{input_tokens, input_tokens_details:{cached_tokens[, cache_write_tokens]}, output_tokens, output_tokens_details:{reasoning_tokens}, total_tokens}`; OpenAI has `cache_write_tokens`, DeepSeek does not.
- Arithmetic: `total_tokens == input_tokens + output_tokens` on every capture (13+6=19, 19+100=119, 310+74=384, 43+20=63, 33+32=65); therefore `reasoning_tokens` is a SUBSET of `output_tokens` (64 of 100; 18 of 20; 32 of 32) and `cached_tokens` is a SUBSET of `input_tokens` (0 observed, but never added on top). Do not add them again.
- OpenAI `reasoning_tokens` can be 0 while a `reasoning` item exists and output_tokens is far above visible text (gpt-5-nano effort low: 59 out, 0 reasoning, 1 visible token); do not derive visible-token counts from it.
- Response id: OpenAI `resp_<50 hex>`, item ids `msg_`/`rs_`/`ws_` prefixed; DeepSeek bare UUIDs for both response and items. Same id appears in `response.created` and the terminal event.
- Model echo: OpenAI echoes the dated snapshot (`gpt-4o-mini` -> `gpt-4o-mini-2024-07-18`, `gpt-5-nano` -> `gpt-5-nano-2025-08-07`) and accepts the snapshot id as input; DeepSeek echoes an alias (`deepseek-v4-flash` -> `deepseek-flash`). Never compare request `model` to response `model` for equality.
- `max_output_tokens` minimum is 16 on OpenAI (400 `integer_below_min_value` below that); with a tool call, `output_tokens` may exceed the cap while `status` is still `completed`.
- Pre-stream errors are plain JSON with status 400/404 and envelope `{"error":{message,type,param,code}}`; OpenAI codes seen: `model_not_found` (404), `invalid_type`, `integer_below_min_value`, `previous_response_not_found` (400, `param: previous_response_id`). DeepSeek returns 400 with `code: invalid_request_error` and `content-type: application/octet-stream`; do not rely on content-type to detect a JSON error.
- Mid-stream errors inside a 200: not triggered here; spec says `event: error` / `data.type == "error"` (with `code`, `message`, `param`, `sequence_number`) or terminal `response.failed` with `response.error`. Handle both defensively.
- OpenAI `background: true` non-stream returns 200 immediately with `status: "queued"`, `output: []`, `usage: null`, `service_tier: "auto"`; poll `GET /v1/responses/{id}` until `status` is terminal (was `completed` within 2 s). `background: true` + `stream: true` streams normally with an extra `response.queued` frame after `response.created` (whose `response.status` is `"queued"`).
- OpenAI `store` defaults to `true`; `previous_response_id` works across calls and is echoed; DeepSeek always reports `store: false`.
- OpenAI streams a `reasoning` output item for gpt-5 models with `content: []`, `summary: []` and an `encrypted_content` blob even without `include`; no `response.reasoning*` delta events unless a summary is requested. DeepSeek streams reasoning in clear via `response.reasoning_text.delta`/`.done` with part type `reasoning_text`, and its message item carries a non-standard `"phase":"final_answer"`.
- OpenAI `response.output_text.delta` frames carry an `obfuscation` padding string and `logprobs: []`; DeepSeek deltas carry `logprobs: []` but no `obfuscation`. Pass unknown fields through.
- OpenAI `service_tier` is `auto` in `response.created` and `default` in the terminal event; some non-streamed bodies include `billing: {"payer":"openai"}`. Tolerate both.
- Web search: `output[]` gets a `web_search_call` item (`action.type: search`, `queries`, `query`) at `output_index` 0 with lifecycle events `response.web_search_call.in_progress|searching|completed` (item_id/output_index/sequence_number only); the message follows at `output_index` 1; search count is in `response.tool_usage.web_search.num_requests`, not in `usage`.
- DeepSeek path: BOTH `POST https://api.deepseek.com/v1/responses` and `POST https://api.deepseek.com/responses` work identically, so the gateway can keep `/v1/responses` upstream. `/beta/chat/completions` and `/beta/v1/chat/completions` both return 200 as well.
- DeepSeek params: OpenAI-style `"reasoning":{"effort":"low"}` is accepted and echoed; `output_config.effort` is silently ignored; `previous_response_id` and `background` are silently ignored (200, echoed as `null`/`false`), never rejected, so the gateway must reject/strip them itself for DeepSeek. Model name `deepseek-v4-flash` is accepted (canonical `deepseek-flash`; other canonical `deepseek-v4-pro`).
- DeepSeek thinking spends the budget on reasoning first: `max_output_tokens: 32` yielded `status: incomplete` with only a `reasoning` item and no `message`; small caps will routinely produce reasoning-only, message-less responses.
