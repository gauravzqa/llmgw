"""Live-provider feature smoke: does each gateway feature work end to end.

Not a benchmark. One request per feature (two where non-streaming applies),
small max_tokens, cheapest models, real providers over real TLS. The point is
"the gateway passes this shape through without breaking it", checked once.

    python -m bench.live_smoke                 # start a gateway in-process
    python -m bench.live_smoke --gateway URL   # test an already-running one

Checks (each prints PASS / FAIL / SKIP and a one-line reason):

    a  plain streaming chat, plus the non-streaming form
    b  streaming tool calls: id, name, arguments concatenate to JSON,
       finish_reason tool_calls
    c  second-turn tool-result round trip streams a normal answer
    d  reasoning: Anthropic extended thinking through /anthropic/v1/messages,
       and DeepSeek's native thinking toggle if the provider accepts it
    e  vision: a 16x16 PNG built in-process, no download
    f  image generation: does the gateway expose any images endpoint
    g  JSON mode: response_format json_object, concatenated stream parses
    h  client cancellation: close after three events, streams_open returns to
       baseline, a capture record is still written
    i  invalid provider key, nonexistent wire model, unknown workload: right
       status, no retry (X-Gw-Attempts is 1)

Keys are read only from the environment (via live.env, which loads a
credential file outside the repo). Nothing here prints, logs or writes a key.
"""

from __future__ import annotations

import argparse
import json
import os
import struct
import sys
import tempfile
import time
import zlib
from dataclasses import dataclass, replace
from typing import Any

import httpx
from live.smoke import (
    GHOST_MODEL_ID,
    IDENTITY_MODEL_ID,
    OPENAI_MODEL_ID,
    build_live_app,
    live_catalog,
    serve,
)

from live import env
from llmgw.catalog import ModelSpec

BADKEY_PROVIDER = "deepseek-badkey"
BADKEY_MODEL_ID = "smoke.deepseek-badkey"
BADKEY_ENV = "LLMGW_SMOKE_BAD_KEY"

CHAT_WL = "chat"          # DeepSeek flash, OpenAI dialect, cheapest
OPENAI_WL = "openai"      # gpt-4o-mini: tools, vision, json mode
ANTHROPIC_WL = "anthropic"  # Haiku 4.5 through the identity provider
DEEPSEEK_PRO_WL = "deepseek-pro"  # v4-pro, can_reason in the catalog
BADKEY_WL = "badkey"
GHOST_WL = "ghost"

POLICY_TOML = f"""
default_workload = "{CHAT_WL}"

[defaults.budgets]
total = 120.0
connect = 5.0
first_event = 45.0
progress = 30.0
client_stall = 30.0

[defaults.retry]
max_attempts = 2
base_delay = 0.25
max_delay = 2.0
respect_retry_after = true

[workloads.{CHAT_WL}]
incumbent = "deepseek.deepseek-v4-flash"

[workloads.{OPENAI_WL}]
incumbent = "{OPENAI_MODEL_ID}"

[workloads.{ANTHROPIC_WL}]
incumbent = "{IDENTITY_MODEL_ID}"

[workloads.{DEEPSEEK_PRO_WL}]
incumbent = "deepseek.deepseek-v4-pro"

[workloads.{BADKEY_WL}]
incumbent = "{BADKEY_MODEL_ID}"

[workloads.{GHOST_WL}]
incumbent = "{GHOST_MODEL_ID}"
"""

CHAT_ROUTE = "/v1/chat/completions"
MSG_ROUTE = "/anthropic/v1/messages"


# --------------------------------------------------------------------------
# Result bookkeeping
# --------------------------------------------------------------------------


@dataclass
class Result:
    check: str
    status: str  # PASS / FAIL / SKIP
    detail: str
    model: str = ""
    requests: int = 0
    tokens_in: int = 0
    tokens_out: int = 0


RESULTS: list[Result] = []


def record(check: str, status: str, detail: str, model: str = "",
           requests: int = 0, tokens_in: int = 0, tokens_out: int = 0) -> None:
    r = Result(check, status, detail, model, requests, tokens_in, tokens_out)
    RESULTS.append(r)
    print(f"{status:4} {check}: {detail}" + (f"  [{model}]" if model else ""),
          flush=True)


# --------------------------------------------------------------------------
# SSE helpers
# --------------------------------------------------------------------------


@dataclass
class Event:
    name: str | None
    data: str


def iter_sse(resp: httpx.Response):
    """Yield (event name, data) per SSE event from a streaming response."""
    name: str | None = None
    data_lines: list[str] = []
    for raw in resp.iter_lines():
        line = raw.rstrip("\r")
        if line == "":
            if data_lines:
                yield Event(name, "\n".join(data_lines))
            name, data_lines = None, []
            continue
        if line.startswith(":"):
            continue
        if line.startswith("event:"):
            name = line[6:].strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
    if data_lines:
        yield Event(name, "\n".join(data_lines))


def gw_headers(resp: httpx.Response) -> dict[str, str]:
    return {k: v for k, v in resp.headers.items() if k.lower().startswith("x-gw-")}


def usage_of(payload: dict[str, Any]) -> tuple[int, int]:
    u = payload.get("usage") or {}
    if "prompt_tokens" in u:
        return int(u.get("prompt_tokens") or 0), int(u.get("completion_tokens") or 0)
    if "input_tokens" in u or "output_tokens" in u:
        return int(u.get("input_tokens") or 0), int(u.get("output_tokens") or 0)
    return 0, 0


def chat_body(messages: list[dict[str, Any]], *, stream: bool,
              max_tokens: int = 64, **extra: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "model": "placeholder",  # replaced on the wire by the target's api_model
        "messages": messages,
        "max_tokens": max_tokens,
        "stream": stream,
    }
    if stream:
        body["stream_options"] = {"include_usage": True}
    body.update(extra)
    return body


def run_stream(client: httpx.Client, url: str, body: dict[str, Any]):
    """POST a streaming request; return (response, events, headers)."""
    events: list[Event] = []
    with client.stream("POST", url, json=body) as resp:
        headers = gw_headers(resp)
        status = resp.status_code
        ctype = resp.headers.get("content-type", "")
        if status != 200:
            text = resp.read().decode("utf-8", "replace")[:300]
            return status, headers, ctype, events, text
        for ev in iter_sse(resp):
            events.append(ev)
    return status, headers, ctype, events, ""


def openai_stream_summary(events: list[Event]) -> dict[str, Any]:
    """Fold OpenAI-dialect chunks: text, tool calls, finish, usage, DONE."""
    text: list[str] = []
    reasoning: list[str] = []
    tool_calls: dict[int, dict[str, Any]] = {}
    finish: str | None = None
    usage: tuple[int, int] | None = None
    done = False
    bad_json = 0
    for ev in events:
        if ev.data.strip() == "[DONE]":
            done = True
            continue
        try:
            payload = json.loads(ev.data)
        except json.JSONDecodeError:
            bad_json += 1
            continue
        if isinstance(payload.get("usage"), dict):
            usage = usage_of(payload)
        for choice in payload.get("choices") or []:
            delta = choice.get("delta") or {}
            if delta.get("content"):
                text.append(delta["content"])
            if delta.get("reasoning_content"):
                reasoning.append(delta["reasoning_content"])
            for tc in delta.get("tool_calls") or []:
                idx = int(tc.get("index", 0))
                slot = tool_calls.setdefault(idx, {"id": None, "name": None, "args": []})
                if tc.get("id"):
                    slot["id"] = tc["id"]
                fn = tc.get("function") or {}
                if fn.get("name"):
                    slot["name"] = fn["name"]
                if fn.get("arguments"):
                    slot["args"].append(fn["arguments"])
            if choice.get("finish_reason"):
                finish = choice["finish_reason"]
    return {
        "text": "".join(text),
        "reasoning": "".join(reasoning),
        "tool_calls": [
            {"id": v["id"], "name": v["name"], "arguments": "".join(v["args"])}
            for _, v in sorted(tool_calls.items())
        ],
        "finish": finish,
        "usage": usage,
        "done": done,
        "bad_json": bad_json,
        "n_events": len(events),
    }


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------


WEATHER_TOOL = {
    "type": "function",
    "function": {
        "name": "get_weather",
        "description": "Get the current weather for a city.",
        "parameters": {
            "type": "object",
            "properties": {
                "city": {"type": "string", "description": "City name"},
                "unit": {"type": "string", "enum": ["celsius", "fahrenheit"]},
            },
            "required": ["city"],
        },
    },
}


def tiny_png(rgb: tuple[int, int, int] = (220, 30, 30), size: int = 16) -> str:
    """A solid-colour PNG built from stdlib only. Returns base64 text."""
    import base64

    def chunk(tag: bytes, data: bytes) -> bytes:
        crc = zlib.crc32(tag + data) & 0xFFFFFFFF
        return struct.pack(">I", len(data)) + tag + data + struct.pack(">I", crc)

    row = b"\x00" + bytes(rgb) * size
    raw = row * size
    ihdr = struct.pack(">IIBBBBB", size, size, 8, 2, 0, 0, 0)
    png = (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", ihdr)
           + chunk(b"IDAT", zlib.compress(raw, 9)) + chunk(b"IEND", b""))
    return base64.b64encode(png).decode("ascii")


def metrics_streams_open(client: httpx.Client, base: str) -> float | None:
    r = client.get(f"{base}/metrics")
    if r.status_code != 200:
        return None
    total = 0.0
    seen = False
    for line in r.text.splitlines():
        if line.startswith("llmgw_streams_open"):
            try:
                total += float(line.rsplit(" ", 1)[1])
                seen = True
            except ValueError:
                pass
    return total if seen else None


def capture_lines(path: str) -> list[dict[str, Any]]:
    try:
        with open(path, "rb") as f:
            return [json.loads(line) for line in f if line.strip()]
    except FileNotFoundError:
        return []


# --------------------------------------------------------------------------
# Checks
# --------------------------------------------------------------------------


def check_a_chat(client: httpx.Client, base: str) -> None:
    # max_tokens is 400 here, not 32: the wire model `deepseek-flash` reasons
    # by default and at 32 and again at 200 the budget went to reasoning_content, leaving
    # content empty with finish_reason=length. Provider behaviour, not ours.
    url = f"{base}/workloads/{CHAT_WL}{CHAT_ROUTE}"
    body = chat_body([{"role": "user", "content": "Say hello in five words."}],
                     stream=True, max_tokens=400)
    t0 = time.monotonic()
    status, headers, ctype, events, err = run_stream(client, url, body)
    if status != 200:
        record("a.stream", "FAIL", f"HTTP {status} {err}", CHAT_WL, 1)
    else:
        s = openai_stream_summary(events)
        need = ("x-gw-served-by", "x-gw-attempts", "x-gw-policy-id")
        missing = [h for h in need if h not in headers]
        ok = (s["n_events"] > 0 and s["bad_json"] == 0 and s["done"]
              and s["usage"] is not None and s["text"] and not missing
              and ctype.startswith("text/event-stream"))
        tin, tout = s["usage"] or (0, 0)
        record("a.stream", "PASS" if ok else "FAIL",
               f"{s['n_events']} events, text={s['text']!r:.40}, "
               f"reasoning_chars={len(s['reasoning'])}, finish={s['finish']}, "
               f"usage={s['usage']}, DONE={s['done']}, bad_json={s['bad_json']}, "
               f"headers={ {k: headers[k] for k in need if k in headers} }, "
               f"missing={missing}, wall={time.monotonic()-t0:.2f}s",
               headers.get("x-gw-served-by", CHAT_WL), 1, tin, tout)
    # non-streaming form
    body = chat_body([{"role": "user", "content": "Say hello in five words."}],
                     stream=False, max_tokens=400)
    r = client.post(url, json=body)
    hs = gw_headers(r)
    try:
        payload = r.json()
        msg = payload["choices"][0]["message"]
        text = msg.get("content") or ""
        tin, tout = usage_of(payload)
        ok = r.status_code == 200 and bool(text) and "x-gw-served-by" in hs
        record("a.nonstream", "PASS" if ok else "FAIL",
               f"HTTP {r.status_code}, text={text!r:.40}, "
               f"reasoning_chars={len(msg.get('reasoning_content') or '')}, "
               f"finish={payload['choices'][0].get('finish_reason')}, "
               f"usage=({tin},{tout}), content-type={r.headers.get('content-type')}",
               hs.get("x-gw-served-by", CHAT_WL), 1, tin, tout)
    except Exception as e:  # noqa: BLE001
        record("a.nonstream", "FAIL",
               f"HTTP {r.status_code} body not a completion: {e}: {r.text[:200]}",
               CHAT_WL, 1)


def _tool_call_once(client: httpx.Client, base: str, wl: str, label: str
                    ) -> dict[str, Any] | None:
    url = f"{base}/workloads/{wl}{CHAT_ROUTE}"
    body = chat_body(
        [{"role": "user", "content": "What is the weather in Bangalore right now? "
                                      "Use the tool."}],
        stream=True, max_tokens=96, tools=[WEATHER_TOOL], tool_choice="auto")
    status, headers, ctype, events, err = run_stream(client, url, body)
    if status != 200:
        record(label, "FAIL", f"HTTP {status} {err}", wl, 1)
        return None
    s = openai_stream_summary(events)
    tcs = s["tool_calls"]
    args_ok = False
    parsed: Any = None
    if tcs:
        try:
            parsed = json.loads(tcs[0]["arguments"])
            args_ok = isinstance(parsed, dict) and "city" in parsed
        except json.JSONDecodeError:
            args_ok = False
    ok = (bool(tcs) and tcs[0]["id"] and tcs[0]["name"] == "get_weather"
          and args_ok and s["finish"] == "tool_calls" and s["done"]
          and s["bad_json"] == 0)
    tin, tout = s["usage"] or (0, 0)
    record(label, "PASS" if ok else "FAIL",
           f"{s['n_events']} events, tool_calls={tcs}, finish={s['finish']}, "
           f"DONE={s['done']}, usage={s['usage']}",
           headers.get("x-gw-served-by", wl), 1, tin, tout)
    return {"tool_calls": tcs, "ok": ok, "served_by": headers.get("x-gw-served-by", wl)}


def check_b_c_tools(client: httpx.Client, base: str) -> None:
    primary = _tool_call_once(client, base, OPENAI_WL, "b.tools.openai")
    _tool_call_once(client, base, CHAT_WL, "b.tools.deepseek")
    if not primary or not primary["ok"]:
        record("c.tool_roundtrip", "SKIP", "no valid tool call from b to feed back",
               OPENAI_WL)
        return
    tc = primary["tool_calls"][0]
    url = f"{base}/workloads/{OPENAI_WL}{CHAT_ROUTE}"
    messages = [
        {"role": "user", "content": "What is the weather in Bangalore right now? "
                                     "Use the tool."},
        {"role": "assistant", "content": None, "tool_calls": [
            {"id": tc["id"], "type": "function",
             "function": {"name": tc["name"], "arguments": tc["arguments"]}}]},
        {"role": "tool", "tool_call_id": tc["id"],
         "content": json.dumps({"city": "Bangalore", "temp_c": 24, "sky": "cloudy"})},
    ]
    body = chat_body(messages, stream=True, max_tokens=64, tools=[WEATHER_TOOL])
    status, headers, ctype, events, err = run_stream(client, url, body)
    if status != 200:
        record("c.tool_roundtrip", "FAIL", f"HTTP {status} {err}", OPENAI_WL, 1)
        return
    s = openai_stream_summary(events)
    ok = (bool(s["text"]) and s["finish"] == "stop" and s["done"]
          and not s["tool_calls"] and s["bad_json"] == 0)
    tin, tout = s["usage"] or (0, 0)
    record("c.tool_roundtrip", "PASS" if ok else "FAIL",
           f"{s['n_events']} events, text={s['text']!r:.60}, finish={s['finish']}, "
           f"usage={s['usage']}", headers.get("x-gw-served-by", OPENAI_WL), 1, tin, tout)


def check_d_reasoning(client: httpx.Client, base: str) -> None:
    # Anthropic extended thinking. budget_tokens must be >= 1024 and below
    # max_tokens, so this is the one request that exceeds the 200-token cap.
    url = f"{base}/workloads/{ANTHROPIC_WL}{MSG_ROUTE}"
    body = {
        "model": "placeholder",
        "max_tokens": 1200,
        "stream": True,
        "thinking": {"type": "enabled", "budget_tokens": 1024},
        "messages": [{"role": "user",
                      "content": "What is 17 * 23? Think briefly, then answer "
                                 "with just the number."}],
    }
    status, headers, ctype, events, err = run_stream(client, url, body)
    if status != 200:
        record("d.thinking.anthropic", "FAIL", f"HTTP {status} {err}", ANTHROPIC_WL, 1)
    else:
        names = [e.name for e in events]
        thinking = []
        text = []
        tin = tout = 0
        bad = 0
        stop = False
        for e in events:
            try:
                p = json.loads(e.data)
            except json.JSONDecodeError:
                bad += 1
                continue
            if e.name == "message_start":
                tin, _ = usage_of(p.get("message") or {})
            if e.name == "message_delta":
                _, tout = usage_of(p)
            if e.name == "content_block_delta":
                d = p.get("delta") or {}
                if d.get("type") == "thinking_delta":
                    thinking.append(d.get("thinking", ""))
                elif d.get("type") == "text_delta":
                    text.append(d.get("text", ""))
            if e.name == "message_stop":
                stop = True
        th = "".join(thinking)
        tx = "".join(text)
        ok = bool(th) and bool(tx) and stop and bad == 0 and "391" in tx
        record("d.thinking.anthropic", "PASS" if ok else "FAIL",
               f"{len(events)} events, named events={sorted(set(n for n in names if n))}, "
               f"thinking_chars={len(th)}, text={tx!r:.30}, message_stop={stop}, "
               f"bad_json={bad}, tokens=({tin},{tout}); the gateway forwarded the "
               f"Anthropic frames as-is (event names intact), no normalisation",
               headers.get("x-gw-served-by", ANTHROPIC_WL), 1, tin, tout)
    # DeepSeek native thinking toggle on v4-pro, if the provider accepts it.
    url = f"{base}/workloads/{DEEPSEEK_PRO_WL}{CHAT_ROUTE}"
    body = chat_body([{"role": "user", "content": "What is 17 * 23? Answer with just "
                                                   "the number."}],
                     stream=True, max_tokens=160, thinking={"type": "enabled"})
    status, headers, ctype, events, err = run_stream(client, url, body)
    if status != 200:
        record("d.thinking.deepseek", "SKIP",
               f"provider did not accept the thinking parameter: HTTP {status} "
               f"{err[:160]}", DEEPSEEK_PRO_WL, 1)
        return
    s = openai_stream_summary(events)
    if s["reasoning"]:
        ok = s["done"] and s["bad_json"] == 0 and "391" in s["text"]
        tin, tout = s["usage"] or (0, 0)
        record("d.thinking.deepseek", "PASS" if ok else "FAIL",
               f"{s['n_events']} events, reasoning_chars={len(s['reasoning'])}, "
               f"text={s['text']!r:.30}, DONE={s['done']}, usage={s['usage']}; "
               f"reasoning_content deltas passed through untouched",
               headers.get("x-gw-served-by", DEEPSEEK_PRO_WL), 1, tin, tout)
    else:
        tin, tout = s["usage"] or (0, 0)
        record("d.thinking.deepseek", "SKIP",
               f"HTTP 200 but no reasoning_content in the stream (model ignored the "
               f"toggle); text={s['text']!r:.30}",
               headers.get("x-gw-served-by", DEEPSEEK_PRO_WL), 1, tin, tout)


def check_e_vision(client: httpx.Client, base: str) -> None:
    url = f"{base}/workloads/{OPENAI_WL}{CHAT_ROUTE}"
    b64 = tiny_png()
    body = chat_body([{"role": "user", "content": [
        {"type": "text", "text": "What colour is this image? One word."},
        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}"}},
    ]}], stream=True, max_tokens=16)
    status, headers, ctype, events, err = run_stream(client, url, body)
    if status != 200:
        record("e.vision", "FAIL", f"HTTP {status} {err}", OPENAI_WL, 1)
        return
    s = openai_stream_summary(events)
    ok = bool(s["text"]) and s["done"] and s["bad_json"] == 0
    tin, tout = s["usage"] or (0, 0)
    record("e.vision", "PASS" if ok else "FAIL",
           f"text={s['text']!r}, said_red={'red' in s['text'].lower()}, "
           f"usage={s['usage']}, png_bytes={len(b64)*3//4}",
           headers.get("x-gw-served-by", OPENAI_WL), 1, tin, tout)


def check_f_images(client: httpx.Client, base: str) -> None:
    out = []
    for path in ("/v1/images/generations", "/v1/responses", "/v1/embeddings"):
        r = client.post(f"{base}{path}", json={"model": "x", "prompt": "a cat"})
        out.append(f"{path} -> {r.status_code}")
    record("f.images", "SKIP",
           "not supported by the gateway, chat-completions and messages only: "
           + "; ".join(out), "", 3)


def check_g_json(client: httpx.Client, base: str) -> None:
    url = f"{base}/workloads/{OPENAI_WL}{CHAT_ROUTE}"
    body = chat_body([{"role": "user", "content":
                       "Return a JSON object with keys city and country for "
                       "Bangalore. JSON only."}],
                     stream=True, max_tokens=48,
                     response_format={"type": "json_object"})
    status, headers, ctype, events, err = run_stream(client, url, body)
    if status != 200:
        record("g.json_mode", "FAIL", f"HTTP {status} {err}", OPENAI_WL, 1)
        return
    s = openai_stream_summary(events)
    try:
        obj = json.loads(s["text"])
        ok = isinstance(obj, dict) and "city" in obj and s["done"]
        detail = f"parsed={obj}"
    except json.JSONDecodeError as e:
        ok = False
        detail = f"concatenated text is not JSON: {e}: {s['text']!r:.80}"
    tin, tout = s["usage"] or (0, 0)
    record("g.json_mode", "PASS" if ok else "FAIL",
           f"{s['n_events']} events, {detail}, usage={s['usage']}",
           headers.get("x-gw-served-by", OPENAI_WL), 1, tin, tout)


def check_h_cancel(client: httpx.Client, base: str, capture_path: str | None) -> None:
    url = f"{base}/workloads/{CHAT_WL}{CHAT_ROUTE}"
    before_open = metrics_streams_open(client, base)
    before_lines = len(capture_lines(capture_path)) if capture_path else 0
    body = chat_body([{"role": "user", "content":
                       "Count from 1 to 300, separated by spaces. Do not stop early."}],
                     stream=True, max_tokens=150)
    n = 0
    served = CHAT_WL
    # A dedicated client so closing it really closes the socket.
    with httpx.Client(timeout=60.0) as c2:
        with c2.stream("POST", url, json=body) as resp:
            served = resp.headers.get("x-gw-served-by", served)
            if resp.status_code != 200:
                record("h.cancel", "FAIL", f"HTTP {resp.status_code}", served, 1)
                return
            for _ in iter_sse(resp):
                n += 1
                if n >= 3:
                    break
            resp.close()
    t0 = time.monotonic()
    after_open = None
    while time.monotonic() - t0 < 5.0:
        after_open = metrics_streams_open(client, base)
        if after_open is not None and before_open is not None and after_open <= before_open:
            break
        time.sleep(0.2)
    back = (after_open is not None and before_open is not None
            and after_open <= before_open)
    settle = time.monotonic() - t0
    rec_detail = "capture not configured"
    rec_ok = True
    if capture_path:
        deadline = time.monotonic() + 5.0
        lines: list[dict[str, Any]] = []
        while time.monotonic() < deadline:
            lines = capture_lines(capture_path)
            if len(lines) > before_lines:
                break
            time.sleep(0.2)
        new = lines[before_lines:]
        if new:
            r = new[-1]
            rec_detail = (f"capture record: outcome={r.get('outcome')}, "
                          f"basis={r.get('basis')}, committed={r.get('committed')}, "
                          f"tokens={r.get('tokens')}, cost_usd={r.get('cost_usd')}, "
                          f"error_code={r.get('error_code')}")
            rec_ok = r.get("outcome") in ("canceled", "interrupted", "completed")
        else:
            rec_detail = "NO capture record written within 5 s"
            rec_ok = False
    ok = back and rec_ok
    record("h.cancel", "PASS" if ok else "FAIL",
           f"closed after {n} events; streams_open {before_open} -> {after_open} "
           f"in {settle:.2f}s (back to baseline={back}); {rec_detail}", served, 1)


def check_i_errors(client: httpx.Client, base: str) -> None:
    # invalid provider key
    url = f"{base}/workloads/{BADKEY_WL}{CHAT_ROUTE}"
    r = client.post(url, json=chat_body([{"role": "user", "content": "hi"}],
                                        stream=False, max_tokens=8))
    hs = gw_headers(r)
    ok = r.status_code in (401, 403) and hs.get("x-gw-attempts") == "1"
    record("i.bad_key", "PASS" if ok else "FAIL",
           f"HTTP {r.status_code}, attempts={hs.get('x-gw-attempts')}, "
           f"served_by={hs.get('x-gw-served-by')}, body={r.text[:120]!r}",
           BADKEY_WL, 1)
    # nonexistent wire model at the provider
    url = f"{base}/workloads/{GHOST_WL}{CHAT_ROUTE}"
    r = client.post(url, json=chat_body([{"role": "user", "content": "hi"}],
                                        stream=False, max_tokens=8))
    hs = gw_headers(r)
    ok = r.status_code in (400, 404) and hs.get("x-gw-attempts") == "1"
    record("i.ghost_model", "PASS" if ok else "FAIL",
           f"HTTP {r.status_code}, attempts={hs.get('x-gw-attempts')}, "
           f"body={r.text[:120]!r}", GHOST_WL, 1)
    # unknown workload never reaches a provider
    url = f"{base}/workloads/no-such-workload{CHAT_ROUTE}"
    r = client.post(url, json=chat_body([{"role": "user", "content": "hi"}],
                                        stream=False, max_tokens=8))
    hs = gw_headers(r)
    # The gateway answers a policy_error 400 with X-Gw-Attempts: 0 and
    # X-Gw-Served-By: "-", meaning no provider was contacted.
    ok = (r.status_code in (400, 404) and hs.get("x-gw-attempts") == "0"
          and hs.get("x-gw-served-by", "-") == "-")
    record("i.unknown_workload", "PASS" if ok else "FAIL",
           f"HTTP {r.status_code}, attempts={hs.get('x-gw-attempts')}, "
           f"served_by={hs.get('x-gw-served-by')}, body={r.text[:120]!r}", "", 1)


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------


def smoke_catalog():
    base = live_catalog()
    bad_provider = replace(base.providers["deepseek"], id=BADKEY_PROVIDER,
                           api_key_env=BADKEY_ENV, credential_id=BADKEY_PROVIDER)
    bad_model = ModelSpec(id=BADKEY_MODEL_ID, provider=BADKEY_PROVIDER,
                          api_model="deepseek-flash", input_per_m=0.14,
                          output_per_m=0.28, priced_at="2026-09-11")
    return base.with_overrides(providers={BADKEY_PROVIDER: bad_provider},
                               models={BADKEY_MODEL_ID: bad_model})


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--gateway", default=None,
                    help="base URL of a running gateway; default starts one in-process")
    ap.add_argument("--capture", default=None,
                    help="capture JSONL path (in-process mode picks a temp file)")
    ap.add_argument("--only", default="abcdefghi",
                    help="letters of the checks to run, default all")
    args = ap.parse_args(argv)

    statuses = env.ensure_loaded()
    print("credentials: " + ", ".join(str(s) for s in statuses), flush=True)
    env.require("DEEPSEEK_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_API_KEY")
    os.environ.setdefault(BADKEY_ENV, "sk-invalid-on-purpose-0000")

    gw = None
    capture_path = args.capture
    if args.gateway:
        base = args.gateway.rstrip("/")
    else:
        tmp = tempfile.mkdtemp(prefix="llmgw-smoke-")
        policy_path = os.path.join(tmp, "policy.toml")
        with open(policy_path, "w", encoding="utf-8") as f:
            f.write(POLICY_TOML)
        capture_path = capture_path or os.path.join(tmp, "capture.jsonl")
        app = build_live_app(policy_path, catalog=smoke_catalog(),
                             capture_path=capture_path)
        gw = serve(app)
        base = gw.base_url
        print(f"gateway: {base}  policy={policy_path}  capture={capture_path}",
              flush=True)

    t0 = time.monotonic()
    try:
        with httpx.Client(timeout=90.0) as client:
            if "a" in args.only:
                check_a_chat(client, base)
            if "b" in args.only or "c" in args.only:
                check_b_c_tools(client, base)
            if "d" in args.only:
                check_d_reasoning(client, base)
            if "e" in args.only:
                check_e_vision(client, base)
            if "f" in args.only:
                check_f_images(client, base)
            if "g" in args.only:
                check_g_json(client, base)
            if "h" in args.only:
                check_h_cancel(client, base, capture_path)
            if "i" in args.only:
                check_i_errors(client, base)
    finally:
        if gw is not None:
            gw.stop()

    n = {s: sum(1 for r in RESULTS if r.status == s) for s in ("PASS", "FAIL", "SKIP")}
    reqs = sum(r.requests for r in RESULTS)
    tin = sum(r.tokens_in for r in RESULTS)
    tout = sum(r.tokens_out for r in RESULTS)
    print(f"\nsummary: {n['PASS']} PASS, {n['FAIL']} FAIL, {n['SKIP']} SKIP; "
          f"{reqs} requests, tokens in={tin} out={tout} (from usage frames), "
          f"wall={time.monotonic()-t0:.1f}s", flush=True)
    if capture_path:
        recs = capture_lines(capture_path)
        cost = sum(float(r.get("cost_usd") or 0) for r in recs)
        print(f"capture: {len(recs)} records, gateway-billed cost ${cost:.5f}",
              flush=True)
    return 1 if n["FAIL"] else 0


if __name__ == "__main__":
    sys.exit(main())
