"""The attempt loop, in the small: fallback, commitment, budgets, and time.

No sockets and no sleeping. Every target is an `httpx.MockTransport` route
keyed on hostname, every clock is a `ManualClock`, and every sink is a list.
That is what makes it possible to assert the things a socket test cannot see
-- that the incumbent's transport was never entered, that `sink_factory` was
never awaited, that three attempts wanting a full phase budget each spent one
total between them.

The contract tier next door proves the same loop works against a real hostile
upstream on a real port. This tier proves it is correct in the small, and it
has to stay fast enough that nobody thinks about running it.
"""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest
from fakes import wire

from llmgw import errors as E
from llmgw.catalog import Catalog, ModelSpec, ProviderConn
from llmgw.clocks import Budgets, Deadline, ManualClock
from llmgw.errors import Outcome
from llmgw.executor import AttemptRecord, ExecutionResult, Executor
from llmgw.policy import ExecutionPlan
from llmgw.retry import RetryPolicy
from llmgw.surfaces import OPENAI_CHAT
from llmgw.upstream import Upstream, UpstreamStream

KEY_ENV = "LLMGW_TEST_KEY"
KEY = "sk-secret-do-not-log-0a1b2c3d4e5f"
PATH = "/v1/chat/completions"
BODY = b'{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}'

CANDIDATE = "cand"
INCUMBENT = "inc"
THIRD = "third"


@pytest.fixture(autouse=True)
def _key(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(KEY_ENV, KEY)


@pytest.fixture(autouse=True)
async def _no_orphan_tasks():
    """Every test returns the loop to its baseline task count.

    The executor builds a `Pump`, which builds a `TaskGroup`. An orphaned pump
    task holds an upstream connection open forever, and it is invisible in the
    assertion the test actually wrote -- so the fixture is what notices rather
    than the author.
    """
    baseline = len(asyncio.all_tasks())
    yield
    for _ in range(50):
        if len(asyncio.all_tasks()) <= baseline:
            break
        await asyncio.sleep(0)
    assert len(asyncio.all_tasks()) == baseline, "the executor left a task behind"


# ================================================================== the rig


def provider(pid: str) -> ProviderConn:
    """One provider per target, each on its own hostname.

    Distinct hosts because the `MockTransport` handler routes on hostname --
    which is also how the contract tier distinguishes targets, so the two tiers
    disagree about nothing.
    """
    return ProviderConn(
        id=pid, kind="openai", base_url=f"https://{pid}.invalid", api_key_env=KEY_ENV
    )


def catalog_for(*pids: str) -> Catalog:
    providers = {pid: provider(pid) for pid in pids}
    models = {
        pid: ModelSpec(
            id=pid,
            provider=pid,
            api_model=f"wire-{pid}",
            input_per_m=1.0,
            output_per_m=2.0,
            priced_at="2026-09-09",
        )
        for pid in pids
    }
    return Catalog(models=models, providers=providers)


def make_budgets(**overrides) -> Budgets:
    defaults = dict(total=600.0, connect=2.0, first_event=20.0, progress=15.0,
                    client_stall=30.0)
    defaults.update(overrides)
    return Budgets(**defaults).validate()


def make_plan(catalog: Catalog, *pids: str, budgets: Budgets | None = None):
    return ExecutionPlan(
        policy_id="pol_test",
        workload_id="chat",
        targets=tuple(catalog.resolve(pid) for pid in pids),
        budgets=budgets if budgets is not None else make_budgets(),
        retry=None,
    )


def streamed(status: int = 200, *, chunks=(b"data: hi\n\n",), headers=None):
    """A MockTransport response that is still a STREAM when send() returns.

    `httpx.Response(200, content=b"...")` is born already-read, so `aiter_raw()`
    on it raises `StreamConsumed` -- a rig artifact that looks exactly like a
    transport bug.
    """

    async def gen():
        for chunk in chunks:
            yield chunk

    return httpx.Response(status, headers=headers or {}, content=gen())


def sse(surface=OPENAI_CHAT) -> bytes:
    frames = wire.openai_stream()
    return wire.joined(frames)


def ok(surface=OPENAI_CHAT):
    def make(_request: httpx.Request) -> httpx.Response:
        return streamed(200, chunks=(sse(surface),),
                        headers={"content-type": "text/event-stream"})

    return make


def status(code: int, *, body: bytes = b'{"error":{"type":"server_error"}}', **headers):
    def make(_request: httpx.Request) -> httpx.Response:
        return streamed(code, headers=headers, chunks=(body,))

    return make


def raises(exc: BaseException):
    def make(_request: httpx.Request) -> httpx.Response:
        raise exc

    return make


def dies_after(n_frames: int):
    """200, some real frames, then a truncated chunked body.

    The post-commitment failure that matters: the client has already been sent
    a status and a prefix of a real answer.
    """

    def make(_request: httpx.Request) -> httpx.Response:
        async def gen():
            for frame in wire.openai_stream()[:n_frames]:
                yield frame
            raise httpx.RemoteProtocolError("peer closed connection")

        return httpx.Response(200, headers={"content-type": "text/event-stream"},
                              content=gen())

    return make


def hangs(clock: ManualClock):
    """Never answers. The connect phase has to be what ends this attempt."""

    async def make(_request: httpx.Request) -> httpx.Response:
        await clock.sleep(1_000_000.0)
        raise AssertionError("unreachable: the phase budget should have fired")

    return make


class Routes:
    """A MockTransport handler that dispatches on hostname and counts opens."""

    def __init__(self, **by_host):
        self._by_host = by_host
        self.opened: list[str] = []

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        host = request.url.host.split(".", 1)[0]
        self.opened.append(host)
        make = self._by_host[host]
        result = make(request)
        if asyncio.iscoroutine(result):
            result = await result
        return result

    def count(self, host: str) -> int:
        return self.opened.count(host)


class ListSink:
    def __init__(self) -> None:
        self.chunks: list[bytes] = []

    async def send(self, chunk: bytes) -> None:
        self.chunks.append(bytes(chunk))

    @property
    def body(self) -> bytes:
        return b"".join(self.chunks)


class Factory:
    """A `sink_factory` that remembers whether -- and when -- it was called.

    `calls` is the assertion the whole P3 decision rests on: it is the moment
    `http.response.start` would have gone out, and every fallback test here is
    really a claim that it had not happened yet.
    """

    def __init__(self, sink: ListSink | None = None) -> None:
        self.sink = sink if sink is not None else ListSink()
        self.calls: list[int] = []

    async def __call__(self, stream: UpstreamStream):
        self.calls.append(stream.status)
        return self.sink


class Rig:
    def __init__(self, *pids: str, clock: ManualClock | None = None, **by_host):
        self.clock = clock if clock is not None else ManualClock(start=0.0)
        self.catalog = catalog_for(*pids)
        self.routes = Routes(**by_host)
        self.upstream = Upstream(
            self.catalog, clock=self.clock, transport=httpx.MockTransport(self.routes)
        )
        self.executor = Executor(self.upstream, clock=self.clock)
        self.factory = Factory()

    def plan(self, *pids: str, budgets: Budgets | None = None) -> ExecutionPlan:
        return make_plan(self.catalog, *pids, budgets=budgets)

    def deadline(self, total: float = 600.0) -> Deadline:
        return Deadline(self.clock, total)

    async def run(
        self,
        *pids: str,
        budgets: Budgets | None = None,
        total: float | None = None,
        retry_policy: RetryPolicy | None = None,
        stream: bool = True,
        plan: ExecutionPlan | None = None,
        on_finish=None,
    ) -> ExecutionResult:
        plan = plan if plan is not None else self.plan(*pids, budgets=budgets)
        deadline = self.deadline(total if total is not None else plan.budgets.total)
        try:
            return await self.executor.execute(
                plan=plan,
                surface=OPENAI_CHAT,
                body=BODY,
                path=PATH,
                stream=stream,
                deadline=deadline,
                sink_factory=self.factory,
                retry_policy=retry_policy,
                on_finish=on_finish,
            )
        finally:
            await self.upstream.aclose()


async def drive(clock: ManualClock, task, *, step: float, limit: int = 200) -> None:
    """Advance manual time until the execution finishes, or `limit` steps."""
    for _ in range(limit):
        if task.done():
            return
        await clock.advance(step)


def codes(result: ExecutionResult) -> list[str]:
    return [a.outcome for a in result.attempts]


# ======================================================= pre-commit fallback


async def test_a_candidate_that_answers_500_falls_back_to_the_incumbent():
    """`precommit_fallback_on_5xx`. Nothing was said to the client, so nothing
    is broken: the 500 is invisible and the incumbent's answer is the answer."""
    rig = Rig(CANDIDATE, INCUMBENT, cand=status(500), inc=ok())
    result = await rig.run(CANDIDATE, INCUMBENT)

    assert result.error is None
    assert result.outcome is Outcome.COMPLETED
    assert result.served_by == rig.catalog.resolve(INCUMBENT)
    assert codes(result) == ["upstream_server_error", "success"]
    assert [a.target.provider.id for a in result.attempts] == [CANDIDATE, INCUMBENT]
    assert result.attempts[0].status == 500
    assert result.attempts[1].status == 200
    assert rig.factory.calls == [200], "the client saw exactly one status"
    assert rig.factory.sink.body == sse()


async def test_a_candidate_that_never_connects_falls_back_to_the_incumbent():
    """`precommit_fallback_on_connect_timeout`. The safest fallback there is:
    nothing was sent upstream, so no side effect can exist at the candidate."""
    rig = Rig(
        CANDIDATE, INCUMBENT,
        cand=raises(httpx.ConnectTimeout("no route")), inc=ok(),
    )
    result = await rig.run(CANDIDATE, INCUMBENT)

    assert codes(result) == ["connect_timeout", "success"]
    assert result.attempts[0].status is None, "a connect failure has no status"
    assert result.served_by.provider.id == INCUMBENT
    assert rig.factory.sink.body == sse()


async def test_the_incumbent_is_never_opened_when_the_candidate_succeeds():
    rig = Rig(CANDIDATE, INCUMBENT, cand=ok(), inc=ok())
    result = await rig.run(CANDIDATE, INCUMBENT)

    assert rig.routes.opened == [CANDIDATE]
    assert len(result.attempts) == 1


# ===================================================== the commitment boundary


async def test_a_candidate_that_dies_after_commitment_is_never_replaced():
    """`no_fallback_after_commit`, and the reason this whole file exists.

    The candidate answered 200, the pump wrote a prefix of a real answer, and
    then the provider vanished. The incumbent might well have served this
    request perfectly -- and asking it would splice the head of one answer onto
    the tail of another, which is undetectable from the outside and impossible
    to apologise for afterwards.
    """
    rig = Rig(CANDIDATE, INCUMBENT, cand=dies_after(3), inc=ok())
    result = await rig.run(CANDIDATE, INCUMBENT)

    assert rig.routes.opened == [CANDIDATE], "the incumbent was never opened"
    assert len(result.attempts) == 1
    record = result.attempts[0]
    assert record.target.provider.id == CANDIDATE
    assert record.outcome == "upstream_disconnected"
    assert record.status == 200
    assert record.committed is True

    assert result.served_by is None
    assert result.committed is True
    assert result.error is not None and result.error.code == "upstream_disconnected"
    # C3: interrupted, not failed. Bytes reached the client.
    assert result.outcome is Outcome.INTERRUPTED
    assert result.pump is not None and result.pump.committed is True
    # C2: the client keeps the partial body and there is no `data: [DONE]`.
    assert rig.factory.sink.body
    assert rig.factory.sink.body == b"".join(wire.openai_stream()[:3])
    assert wire.joined([wire.openai_done()]) not in rig.factory.sink.body


async def test_an_error_that_is_retryable_in_principle_is_not_after_commitment():
    """`UpstreamDisconnected` is `retry_same=True` AND `try_next=True` in the
    taxonomy. Post-commitment it is neither, and this file does not know that
    -- `decide()` does. The proof is that the eligible-in-principle flags are
    still set on the error we surfaced."""
    rig = Rig(CANDIDATE, INCUMBENT, cand=dies_after(2), inc=ok())
    result = await rig.run(CANDIDATE, INCUMBENT)

    err = result.error
    assert err.retry_same is True and err.try_next is True
    assert E.decide(err, committed=True).try_next is False
    assert len(result.attempts) == 1


async def test_the_sink_factory_is_never_called_when_every_target_fails_early():
    """No status was sent, so the caller is free to write a real error response
    -- which is the only reason a pre-commitment failure is worth having."""
    rig = Rig(CANDIDATE, INCUMBENT, cand=status(500), inc=status(503))
    result = await rig.run(CANDIDATE, INCUMBENT)

    assert rig.factory.calls == []
    assert result.committed is False
    assert result.attempts[-1].committed is False
    assert result.outcome is Outcome.FAILED


async def test_the_sink_factory_is_called_exactly_once_on_success():
    rig = Rig(CANDIDATE, INCUMBENT, cand=status(500), inc=ok())
    await rig.run(CANDIDATE, INCUMBENT)
    assert rig.factory.calls == [200]


async def test_a_two_hundred_with_an_empty_body_still_falls_back():
    """A clean EOF is not a completion. And because the status is held until
    the first byte, we have promised nothing -- so this is the best possible
    outcome for a provider that answered 200 and then said nothing at all."""
    rig = Rig(CANDIDATE, INCUMBENT, cand=status(200, body=b""), inc=ok())
    result = await rig.run(CANDIDATE, INCUMBENT)

    assert codes(result) == ["incomplete_stream", "success"]
    assert rig.factory.calls == [200]
    assert result.served_by.provider.id == INCUMBENT


# ============================================================ the same target


async def test_a_retryable_error_is_retried_on_the_same_target():
    """`UpstreamServerError` is `retry_same=True`: a 500 is plausibly transient
    at the target that produced it, so we ask it again before giving up on it."""
    seen: list[int] = []

    def flaky(_request: httpx.Request) -> httpx.Response:
        seen.append(1)
        if len(seen) == 1:
            return streamed(500, chunks=(b"boom",))
        return streamed(200, chunks=(sse(),),
                        headers={"content-type": "text/event-stream"})

    rig = Rig(CANDIDATE, INCUMBENT, cand=flaky, inc=ok())
    policy = RetryPolicy(max_attempts=3, base_delay=0.001, max_delay=0.002)
    task = asyncio.create_task(
        rig.run(CANDIDATE, INCUMBENT, retry_policy=policy)
    )
    await drive(rig.clock, task, step=0.01)
    result = await task

    assert rig.routes.opened == [CANDIDATE, CANDIDATE], "the SAME target, twice"
    assert codes(result) == ["upstream_server_error", "success"]
    assert result.served_by.provider.id == CANDIDATE


async def test_a_bad_request_is_not_retried_at_the_same_target_but_does_fall_back():
    """The row people are surprised by. Re-sending an identical body to a model
    that has already rejected its schema produces an identical 400 forever --
    but the incumbent may speak a different dialect, so `try_next` is True."""
    rig = Rig(
        CANDIDATE, INCUMBENT,
        cand=status(400, body=json.dumps(
            {"error": {"type": "invalid_request_error", "message": "bad tool schema"}}
        ).encode()),
        inc=ok(),
    )
    policy = RetryPolicy(max_attempts=5, base_delay=0.001, max_delay=0.002)
    task = asyncio.create_task(rig.run(CANDIDATE, INCUMBENT, retry_policy=policy))
    await drive(rig.clock, task, step=0.01)
    result = await task

    assert rig.routes.opened == [CANDIDATE, INCUMBENT], "no repetition, one fallback"
    assert codes(result) == ["invalid_request", "success"]


async def test_content_filtered_does_not_fall_back_at_all():
    """Shopping a refused prompt around providers until one answers is a
    compliance decision, not a reliability one, and it is not the gateway's to
    make silently."""
    rig = Rig(
        CANDIDATE, INCUMBENT,
        cand=status(400, body=json.dumps(
            {"error": {"type": "content_filter", "message": "refused"}}
        ).encode()),
        inc=ok(),
    )
    result = await rig.run(CANDIDATE, INCUMBENT)

    assert rig.routes.opened == [CANDIDATE]
    assert codes(result) == ["content_filtered"]
    assert result.error.code == "content_filtered"
    assert result.served_by is None
    assert rig.factory.calls == []


async def test_a_breaker_open_moves_on_without_spending_a_same_target_retry():
    """There is no breaker yet, so the error is constructed directly.

    `BreakerOpen` is `try_next=True, retry_same=False`: the circuit is open
    precisely because this target has been failing, so re-asking it is the one
    thing that cannot help. The assertion is that the candidate is opened once
    and the incumbent once -- a loop that read `try_next` as "retry" would open
    the candidate twice.
    """
    rig = Rig(CANDIDATE, INCUMBENT, cand=ok(), inc=ok())

    class BreakerUpstream:
        """Refuses the candidate before a socket exists; delegates otherwise."""

        def __init__(self, inner):
            self._inner = inner

        def open(self, request, *, deadline, budgets):
            if request.target.provider.id == CANDIDATE:
                raise E.BreakerOpen(
                    "circuit open for cand", provider=CANDIDATE, model=CANDIDATE
                )
            return self._inner.open(request, deadline=deadline, budgets=budgets)

    rig.executor = Executor(BreakerUpstream(rig.upstream), clock=rig.clock)
    policy = RetryPolicy(max_attempts=5, base_delay=0.001, max_delay=0.002)
    task = asyncio.create_task(rig.run(CANDIDATE, INCUMBENT, retry_policy=policy))
    await drive(rig.clock, task, step=0.01)
    result = await task

    assert rig.routes.opened == [INCUMBENT], "no socket was spent on the open circuit"
    assert codes(result) == ["breaker_open", "success"]
    assert result.attempts[0].status is None
    assert result.served_by.provider.id == INCUMBENT


# ================================================================== C5, retries


async def test_a_disabled_retry_policy_buys_zero_retries_and_keeps_the_fallback():
    """C5. `X-Gw-No-Retry: 1` says another layer owns the retries. It does not
    -- and cannot -- say another layer owns our fallback: the outer gateway has
    never seen this plan and does not know the incumbent exists."""
    rig = Rig(CANDIDATE, INCUMBENT, cand=status(500), inc=ok())
    disabled = RetryPolicy(max_attempts=5, enabled=False)
    result = await rig.run(CANDIDATE, INCUMBENT, retry_policy=disabled)

    assert rig.routes.opened == [CANDIDATE, INCUMBENT]
    assert codes(result) == ["upstream_server_error", "success"]
    assert rig.clock.now() == 0.0, "a disabled policy sleeps for nothing"


async def test_no_retry_policy_at_all_means_no_retries_at_all():
    """`retry_policy=None` is silence, and the safe reading of silence in a
    component that can amplify load is zero."""
    rig = Rig(CANDIDATE, INCUMBENT, cand=status(500), inc=status(500))
    result = await rig.run(CANDIDATE, INCUMBENT, retry_policy=None)

    assert rig.routes.opened == [CANDIDATE, INCUMBENT]
    assert len(result.attempts) == 2


async def test_the_attempt_budget_is_per_execution_and_not_per_target():
    """Three targets, `max_attempts=2`. A budget rebuilt per target would allow
    two attempts at each -- six requests to providers that are failing because
    they are overloaded. It is one budget, so the plan runs out of allowance
    before it runs out of targets."""
    rig = Rig(
        CANDIDATE, INCUMBENT, THIRD,
        cand=status(500), inc=status(500), third=status(500),
    )
    policy = RetryPolicy(max_attempts=2, base_delay=0.001, max_delay=0.002)
    task = asyncio.create_task(
        rig.run(CANDIDATE, INCUMBENT, THIRD, retry_policy=policy)
    )
    await drive(rig.clock, task, step=0.01)
    result = await task

    # Attempt 1 fails and is worth a repetition; attempt 2 is that repetition
    # and spends the budget. From there the budget refuses every further
    # delay, so the loop advances instead of repeating and stops when the plan
    # does: two attempts at the candidate, one each at the rest. A budget
    # rebuilt per target would allow two at every one of them -- six.
    assert rig.routes.opened == [CANDIDATE, CANDIDATE, INCUMBENT, THIRD]
    assert len(result.attempts) == 4


# ==================================================================== the clock


async def test_the_total_deadline_never_resets_across_attempts():
    """`total_deadline_never_resets`.

    Three targets, each stalling forever, each phase asking for a 3 s connect
    budget inside a 7 s total. Three full phases would be 9 s. The arithmetic
    in `Deadline.slice()` -- `min(remaining, budget)` -- is the only thing
    stopping that, and there is deliberately no way to opt out of it.
    """
    clock = ManualClock(start=0.0)
    rig = Rig(
        CANDIDATE, INCUMBENT, THIRD, clock=clock,
        cand=hangs(clock), inc=hangs(clock), third=hangs(clock),
    )
    budgets = make_budgets(total=7.0, connect=3.0, first_event=3.0,
                           progress=3.0)
    task = asyncio.create_task(
        rig.run(CANDIDATE, INCUMBENT, THIRD, budgets=budgets, total=7.0)
    )
    await drive(clock, task, step=0.25)
    result = await task

    assert clock.now() <= 7.0, "three attempts spent one total between them"
    assert len(result.attempts) == 3
    assert [a.target.provider.id for a in result.attempts] == [
        CANDIDATE, INCUMBENT, THIRD
    ]
    assert codes(result)[:2] == ["headers_timeout", "headers_timeout"]
    # The last attempt is clamped to the 1 s that was left, so it breaches the
    # TOTAL rather than its own phase -- which is a different disposition
    # (`try_next=False`) and therefore a different error class.
    assert codes(result)[2] == "total_deadline_exceeded"
    assert clock.pending_sleepers == 0, "a stalled attempt left a sleeper behind"


async def test_a_retry_after_larger_than_the_budget_is_never_slept_through():
    """`retry_after_is_floor` at this layer.

    `Retry-After: 300` is a floor on the delay, never a licence to sleep past
    the deadline: waiting buys nothing, because the attempt it enables would be
    cut off by the total before it could do anything. So the budget refuses,
    the plan advances immediately, and no time passes at all.
    """
    rig = Rig(
        CANDIDATE, INCUMBENT,
        cand=status(429, body=b'{"error":{"type":"rate_limit_error"}}',
                    **{"retry-after": "300"}),
        inc=status(429, body=b'{"error":{"type":"rate_limit_error"}}',
                   **{"retry-after": "300"}),
    )
    policy = RetryPolicy(max_attempts=5, base_delay=0.1, max_delay=2.0)
    result = await rig.run(CANDIDATE, INCUMBENT, total=10.0, retry_policy=policy)

    assert rig.clock.now() == 0.0, "we did not sleep into a guaranteed breach"
    assert rig.routes.opened == [CANDIDATE, INCUMBENT]
    # Rule 8: the client gets the 429 and its Retry-After, not our accounting.
    assert result.error.code == "rate_limited"
    assert result.error.retry_after == 300.0
    assert result.outcome is Outcome.FAILED


async def test_a_retry_after_inside_the_budget_is_honoured_as_a_floor():
    """The other half: when the wait fits, it is taken, and it is at least what
    the provider asked for. Undercutting it with a low jitter draw is the
    subtle form of ignoring it -- the request arrives early, is refused again,
    and has spent budget to learn nothing."""
    seen: list[int] = []

    def limited(_request: httpx.Request) -> httpx.Response:
        seen.append(1)
        if len(seen) == 1:
            return streamed(429, headers={"retry-after": "5"},
                            chunks=(b'{"error":{"type":"rate_limit_error"}}',))
        return streamed(200, chunks=(sse(),),
                        headers={"content-type": "text/event-stream"})

    rig = Rig(CANDIDATE, cand=limited)
    policy = RetryPolicy(max_attempts=3, base_delay=0.1, max_delay=0.2)
    task = asyncio.create_task(
        rig.run(CANDIDATE, total=600.0, retry_policy=policy)
    )
    await drive(rig.clock, task, step=0.5)
    result = await task

    assert result.error is None
    assert rig.clock.now() >= 5.0, "the provider's own instruction is a floor"


async def test_a_deadline_that_is_already_gone_opens_no_socket():
    """A request that is already doomed must not spend a handshake proving it.
    Nothing was attempted, so nothing is recorded as an attempt -- an attempt
    count inflated by requests that never left the building makes the
    amplification metric read high on exactly the incident it exists for."""
    rig = Rig(CANDIDATE, INCUMBENT, cand=ok(), inc=ok())
    plan = rig.plan(CANDIDATE, INCUMBENT)
    deadline = Deadline(rig.clock, 5.0)
    await rig.clock.advance(6.0)

    result = await rig.executor.execute(
        plan=plan, surface=OPENAI_CHAT, body=BODY, path=PATH, stream=True,
        deadline=deadline, sink_factory=rig.factory,
    )
    await rig.upstream.aclose()

    assert rig.routes.opened == []
    assert result.attempts == []
    assert result.error.code == "total_deadline_exceeded"
    assert result.outcome is Outcome.FAILED


# ============================================================ the final error


async def test_the_last_real_error_beats_a_summary():
    """A client handed `no_targets_available` when the provider actually said
    "429, come back in 30 seconds" has been robbed of the only actionable thing
    in the response."""
    rig = Rig(
        CANDIDATE, INCUMBENT,
        cand=status(500),
        inc=status(429, body=b'{"error":{"type":"rate_limit_error"}}',
                   **{"retry-after": "30"}),
    )
    result = await rig.run(CANDIDATE, INCUMBENT)

    assert result.error.code == "rate_limited"
    assert result.error.retry_after == 30.0
    assert result.error.passthrough is True
    assert result.error.upstream_status == 429


async def test_an_empty_plan_is_the_one_place_no_targets_available_belongs():
    rig = Rig(CANDIDATE, cand=ok())
    plan = ExecutionPlan(
        policy_id="pol_test", workload_id="chat", targets=(),
        budgets=make_budgets(), retry=None,
    )
    result = await rig.run(plan=plan)

    assert result.attempts == []
    assert result.error.code == "no_targets_available"
    assert result.error.workload == "chat"
    assert rig.routes.opened == []


async def test_a_policy_error_stops_the_plan_rather_than_shopping_it_around():
    """A missing credential is our misconfiguration, not the provider's fault.
    `try_next=False`: the next target would be opened with the same broken
    config, and the failure would be counted twice."""
    rig = Rig(CANDIDATE, INCUMBENT, cand=ok(), inc=ok())
    broken = ProviderConn(id=CANDIDATE, kind="openai",
                          base_url="https://cand.invalid", api_key_env="LLMGW_ABSENT")
    rig.catalog.providers[CANDIDATE] = broken
    result = await rig.run(CANDIDATE, INCUMBENT)

    assert codes(result) == ["policy_error"]
    assert rig.routes.opened == []
    assert result.error.health is E.Health.NEUTRAL


# ========================================================== the attempt record


async def test_attempt_records_are_complete_and_ordered():
    """`X-Gw-Attempts`, the capture record and `llmgw_attempts_total` all read
    this list, and all three are wrong in a different way if it is incomplete."""
    rig = Rig(
        CANDIDATE, INCUMBENT, THIRD,
        cand=raises(httpx.ConnectError("refused")), inc=status(503), third=ok(),
    )
    result = await rig.run(CANDIDATE, INCUMBENT, THIRD)

    assert len(result.attempts) == 3
    assert all(isinstance(a, AttemptRecord) for a in result.attempts)
    assert [a.target.provider.id for a in result.attempts] == [
        CANDIDATE, INCUMBENT, THIRD
    ]
    assert codes(result) == ["connection_failed", "upstream_overloaded", "success"]
    assert [a.status for a in result.attempts] == [None, 503, 200]
    assert [a.committed for a in result.attempts] == [False, False, True]
    for record in result.attempts:
        assert record.ended_at >= record.started_at
    starts = [a.started_at for a in result.attempts]
    assert starts == sorted(starts)
    # Every non-success outcome is a code the metrics vocabulary knows.
    assert {a.outcome for a in result.attempts} - {"success"} <= E.ERROR_CODES


async def test_a_successful_attempt_records_the_upstream_status_it_served():
    rig = Rig(CANDIDATE, cand=ok())
    result = await rig.run(CANDIDATE)

    assert result.attempts[0].outcome == "success"
    assert result.attempts[0].status == 200
    assert result.attempts[0].error is None
    assert result.attempts[0].committed is True
    assert result.pump is not None and result.pump.terminal_seen is True


# ============================================================== cancellation


async def test_cancellation_mid_attempt_propagates_and_leaves_nothing_behind():
    """C8. Cancellation is not evidence about a provider: it is never converted
    into a `GatewayError`, never recorded as a failed attempt, and never
    returned as a result. It also must not leak the pump's tasks -- the autouse
    fixture is what checks the second half."""
    clock = ManualClock(start=0.0)
    rig = Rig(CANDIDATE, INCUMBENT, clock=clock, cand=hangs(clock), inc=ok())

    task = asyncio.create_task(rig.run(CANDIDATE, INCUMBENT, total=600.0))
    for _ in range(20):
        await asyncio.sleep(0)
    assert rig.routes.opened == [CANDIDATE]

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert rig.routes.opened == [CANDIDATE], "cancellation did not trigger a fallback"
    assert rig.upstream.in_flight() == {}, "a cancelled attempt left a response open"


async def test_cancellation_after_commitment_is_not_a_provider_failure():
    """The stream had started; the request was cancelled from outside. Nothing
    here turns that into a class the breaker would count."""
    clock = ManualClock(start=0.0)

    def slow_stream(_request: httpx.Request) -> httpx.Response:
        async def gen():
            for frame in wire.openai_stream():
                yield frame
            await clock.sleep(1_000_000.0)

        return httpx.Response(200, headers={"content-type": "text/event-stream"},
                              content=gen())

    rig = Rig(CANDIDATE, INCUMBENT, clock=clock, cand=slow_stream, inc=ok())
    task = asyncio.create_task(rig.run(CANDIDATE, INCUMBENT, total=600.0))
    for _ in range(40):
        await asyncio.sleep(0)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert rig.routes.opened == [CANDIDATE]


# =============================================================== non-streaming


async def test_a_buffered_response_is_read_in_full_before_the_status_is_sent():
    """The buffered path has an even wider fallback window than the streaming
    one: nothing has been promised until the whole body is in hand, so a
    truncated or oversized body is still a fallback rather than a half-written
    JSON object on the client's socket."""
    payload = b'{"id":"cmpl-1","choices":[{"message":{"content":"hi"}}]}'
    rig = Rig(
        CANDIDATE, INCUMBENT,
        cand=status(500),
        inc=status(200, body=payload),
    )
    result = await rig.run(CANDIDATE, INCUMBENT, stream=False)

    assert codes(result) == ["upstream_server_error", "success"]
    assert rig.factory.sink.body == payload
    assert result.pump is None, "there is no pump on the buffered path"
    assert result.committed is True


async def test_an_oversized_buffered_body_is_refused_rather_than_shopped_around():
    """Our bound, our refusal. A second target would re-download an equally
    large body to hit the same wall, so `try_next=False`."""
    rig = Rig(CANDIDATE, INCUMBENT, cand=status(200, body=b"x" * 4096), inc=ok())
    plan = rig.plan(CANDIDATE, INCUMBENT)
    try:
        result = await rig.executor.execute(
            plan=plan, surface=OPENAI_CHAT, body=BODY, path=PATH, stream=False,
            deadline=rig.deadline(), sink_factory=rig.factory,
            max_response_bytes=512,
        )
    finally:
        await rig.upstream.aclose()

    assert codes(result) == ["response_too_large"]
    assert rig.routes.opened == [CANDIDATE]
    assert rig.factory.calls == []
    assert result.error.health is E.Health.NEUTRAL


# ================================================== the shape of the loop itself


def test_the_file_never_re_derives_the_commitment_invariant():
    """A grep, as a test.

    `decide()` is the only authority on what happens next, and the way that
    stops being true is not a rewrite -- it is one `if committed:` added during
    an incident because a case looked special. So the ban is mechanical: the
    word may appear as an argument to `decide()` and as a dataclass field,
    never as a branch condition.
    """
    import pathlib

    import llmgw.executor as module

    code = "\n".join(
        line
        for line in pathlib.Path(module.__file__).read_text().splitlines()
        if not line.lstrip().startswith("#")
    )
    for forbidden in ("if committed", "if not committed", "if commitment.started",
                      "if state.committed", "committed and ", "not committed and "):
        assert forbidden not in code, f"{forbidden!r} re-derives decide()"
    assert code.count("decide(") >= 2, "the loop consults decide() and obeys it"


async def test_the_commitment_flag_can_only_move_in_one_direction():
    """`_Commitment.started` is monotone and single-use. A second call is a bug
    that would otherwise present as a corrupt response body, and the loud
    version of that is very much cheaper."""
    from llmgw.executor import _Commitment

    async def factory(_stream):
        return ListSink()

    commitment = _Commitment(factory)
    assert commitment.started is False
    await commitment.open(None)
    assert commitment.started is True
    with pytest.raises(RuntimeError, match="already been started"):
        await commitment.open(None)
    assert commitment.started is True


# ==========================================================================
# P3 verification: the commitment invariant with a real second target
# ==========================================================================
#
# P2's `no_fallback_after_commit` was half a test -- there was no second target
# for the loop to reach for. There is now, so the question stops being "does
# `decide()` say no" and becomes "is there any shape of failure, at any instant
# of an attempt, that reaches the next target with a sink already in
# existence". These sweep for one.


class OnceFailingSink:
    """A sink that accepts `n` writes and then behaves badly.

    `n=0` is the interesting one: the client's response has been started, the
    pump has not written a byte the client kept, and the temptation to treat
    that as recoverable is exactly what `_Commitment` exists to remove.
    """

    def __init__(self, n: int = 0, exc: BaseException | None = None) -> None:
        self.n = n
        self.exc = exc if exc is not None else BrokenPipeError("client went away")
        self.chunks: list[bytes] = []

    async def send(self, chunk: bytes) -> None:
        if len(self.chunks) >= self.n:
            raise self.exc
        self.chunks.append(bytes(chunk))


class CountingFactory:
    """A `sink_factory` that counts, can fail, and records the opens so far.

    `opens_at_commit` is the assertion the sweep below turns on. Counting
    factory calls is not enough: on a pre-commitment fallback the factory IS
    called, just during the second target's attempt. The question is whether
    `routes.opened` grew AFTER it, and only a snapshot taken at the call can
    answer that.
    """

    def __init__(self, sink=None, *, raises: BaseException | None = None,
                 routes: Routes | None = None) -> None:
        self.sink = sink if sink is not None else ListSink()
        self.calls: list[int] = []
        self.raises = raises
        self.routes = routes
        self.opens_at_commit: int | None = None

    async def __call__(self, stream: UpstreamStream):
        self.calls.append(stream.status)
        if self.routes is not None:
            self.opens_at_commit = len(self.routes.opened)
        await asyncio.sleep(0)  # a real factory sends an ASGI message here
        if self.raises is not None:
            raise self.raises
        return self.sink


FAILURE_POINTS = {
    # name: (candidate handler factory, does it commit?)
    "connect-refused": lambda: raises(httpx.ConnectError("refused")),
    "connect-timeout": lambda: raises(httpx.ConnectTimeout("timed out")),
    "status-500": lambda: status(500),
    "status-503": lambda: status(503),
    "status-429": lambda: status(429, **{"retry-after": "0"}),
    "status-400": lambda: status(400, body=b'{"error":{"type":"invalid_request"}}'),
    "status-401": lambda: status(401),
    "status-404": lambda: status(404),
    "empty-200": lambda: status(200, body=b""),
    "protocol-error": lambda: raises(httpx.RemoteProtocolError("no status line")),
    "dies-at-1": lambda: dies_after(1),
    "dies-at-3": lambda: dies_after(3),
    "dies-at-6": lambda: dies_after(6),
}


@pytest.mark.parametrize("point", sorted(FAILURE_POINTS))
async def test_no_second_target_is_ever_opened_once_a_sink_exists(point: str):
    """The whole invariant, swept over every failure point an attempt has.

    Two claims, and the second is the one P2 could not make: the factory is
    awaited at most once per execution, and the instant it has been awaited the
    plan is over -- `routes.opened` may not grow again. A gateway that failed
    this would splice the incumbent's answer onto the tail of the candidate's,
    which is undetectable from outside and impossible to apologise for.
    """
    rig = Rig(CANDIDATE, INCUMBENT, cand=FAILURE_POINTS[point](), inc=ok())
    rig.factory = CountingFactory(routes=rig.routes)
    result = await rig.run(CANDIDATE, INCUMBENT)

    assert len(rig.factory.calls) <= 1, "HTTP has no second status"
    if rig.factory.calls:
        assert result.committed is True
        assert rig.factory.opens_at_commit == len(rig.routes.opened), (
            f"{point}: {len(rig.routes.opened) - rig.factory.opens_at_commit} "
            f"upstream(s) opened AFTER the client's response was started: "
            f"{rig.routes.opened}"
        )
    else:
        # No sink: the plan was still open, so the incumbent should have been
        # reached -- unless the class forbids it, which `decide()` decides.
        assert result.committed is False
        assert INCUMBENT in rig.routes.opened or not result.error.try_next


async def test_a_sink_that_fails_on_its_very_first_write_is_still_a_commitment():
    """Zero bytes reached the client and the plan is over anyway.

    `_Commitment.started` is set on the line BEFORE the factory is awaited, for
    the same reason `Pump._committed` is: a factory that raises halfway may
    still have put the status line on the wire. Treating "no byte was written"
    as recoverable would answer a second status into a response that already
    carries one.
    """
    rig = Rig(CANDIDATE, INCUMBENT, cand=ok(), inc=ok())
    rig.factory = CountingFactory(OnceFailingSink(0))
    result = await rig.run(CANDIDATE, INCUMBENT)

    assert rig.routes.opened == [CANDIDATE]
    assert result.committed is True
    assert result.served_by is None
    assert result.error.blame is E.Blame.CLIENT
    assert result.error.health is E.Health.NEUTRAL


async def test_a_factory_that_raises_ends_the_plan_rather_than_falling_back():
    """The status line may already be on the wire. There is no second one."""
    rig = Rig(CANDIDATE, INCUMBENT, cand=ok(), inc=ok())
    rig.factory = CountingFactory(raises=RuntimeError("send() failed halfway"))
    with pytest.raises(RuntimeError, match="halfway"):
        await rig.run(CANDIDATE, INCUMBENT)
    assert rig.routes.opened == [CANDIDATE]


async def test_the_buffered_path_commits_before_it_writes_and_never_falls_back():
    """`stream=False` opens the commitment and THEN writes.

    That ordering is what lets the buffered sink send an honest
    `content-length`, and it means the window between "committed" and "a byte
    moved" is real on this path in a way it is not on the streaming one. A
    fallback inside that window would be a second `http.response.start`.
    """
    rig = Rig(CANDIDATE, INCUMBENT, cand=ok(), inc=ok())
    rig.factory = CountingFactory(OnceFailingSink(0))
    result = await rig.run(CANDIDATE, INCUMBENT, stream=False)

    assert rig.routes.opened == [CANDIDATE]
    assert rig.factory.calls == [200]
    assert result.committed is True
    assert result.error.code == "client_disconnected"


async def test_the_buffered_path_still_falls_back_before_it_commits():
    """The control for the test above: nothing was promised, so a 500 on the
    buffered path is as recoverable as it is on the streaming one."""
    rig = Rig(CANDIDATE, INCUMBENT, cand=status(500), inc=ok())
    result = await rig.run(CANDIDATE, INCUMBENT, stream=False)

    assert rig.routes.opened == [CANDIDATE, INCUMBENT]
    assert rig.factory.calls == [200]
    assert result.served_by.provider.id == INCUMBENT


# ==========================================================================
# P3 verification: amplification
# ==========================================================================


@pytest.mark.parametrize("max_attempts", [1, 2, 3, 5, 9])
@pytest.mark.parametrize("n_targets", [1, 2])
async def test_upstream_requests_never_exceed_the_documented_bound(
    max_attempts: int, n_targets: int
):
    """`len(plan.targets) + (max_attempts - 1)`, asserted by counting sockets.

    Everything fails with a class that is BOTH `retry_same` and `try_next`,
    which is the worst case: repetition happens first and fallback happens
    anyway, so this is the largest number of upstream requests the loop can
    produce for one client request. The bound is the number the probe endpoint
    advertises to operators, so a gateway that exceeded it would be lying in
    the one place someone looks before an incident.
    """
    pids = (CANDIDATE, INCUMBENT)[:n_targets]
    rig = Rig(*pids, **{pid: status(500) for pid in pids})
    policy = RetryPolicy(max_attempts=max_attempts, base_delay=0.01, max_delay=0.02)
    task = asyncio.ensure_future(rig.run(*pids, retry_policy=policy))
    await drive(rig.clock, task, step=0.01)
    result = await task

    bound = n_targets + (max_attempts - 1)
    assert len(rig.routes.opened) <= bound, (
        f"{len(rig.routes.opened)} upstream requests exceeds the advertised "
        f"bound of {bound}: {rig.routes.opened}"
    )
    assert len(result.attempts) == len(rig.routes.opened)


async def test_a_shared_provider_between_two_targets_does_not_double_the_bound():
    """Two catalog entries on ONE provider is a legal plan -- `_reject_identical`
    only refuses the same wire endpoint -- and it must not buy extra attempts.
    The budget counts the request, not the provider."""
    catalog = Catalog(
        providers={"shared": provider("shared")},
        models={
            name: ModelSpec(id=name, provider="shared", api_model=f"wire-{name}",
                            input_per_m=1.0, output_per_m=2.0, priced_at="2026-09-09")
            for name in ("big", "small")
        },
    )
    routes = Routes(shared=status(500))
    clock = ManualClock(start=0.0)
    up = Upstream(catalog, clock=clock, transport=httpx.MockTransport(routes))
    plan = make_plan(catalog, "big", "small")
    factory = Factory()
    task = asyncio.ensure_future(
        Executor(up, clock=clock).execute(
            plan=plan, surface=OPENAI_CHAT, body=BODY, path=PATH, stream=True,
            deadline=Deadline(clock, 600.0), sink_factory=factory,
            retry_policy=RetryPolicy(max_attempts=3, base_delay=0.01, max_delay=0.02),
        )
    )
    await drive(clock, task, step=0.01)
    result = await task
    await up.aclose()

    assert len(routes.opened) <= 2 + (3 - 1)
    assert [r.target.model.id for r in result.attempts].count("big") >= 1
    assert factory.calls == []


async def test_a_tight_deadline_cannot_be_spent_into_by_a_generous_max_attempts():
    """A `max_attempts` of 200 against a total that is nearly gone.

    The budget refuses a delay it cannot follow with a useful attempt, so the
    loop stops on arithmetic rather than on the attempt counter -- which is the
    property that matters, because the attempt counter is the one an operator
    can misconfigure.
    """
    rig = Rig(CANDIDATE, INCUMBENT, cand=status(500), inc=status(500))
    policy = RetryPolicy(max_attempts=200, base_delay=0.05, max_delay=0.2,
                         min_attempt_time=0.05)
    task = asyncio.ensure_future(
        rig.run(CANDIDATE, INCUMBENT, retry_policy=policy, total=0.5)
    )
    await drive(rig.clock, task, step=0.01)
    result = await task

    assert rig.clock.now() <= 0.5 + 1e-9, "the loop outlived its own total"
    assert len(rig.routes.opened) < 20, (
        f"a 0.5s total bought {len(rig.routes.opened)} upstream requests"
    )
    assert result.error is not None


async def test_no_retry_never_costs_more_than_one_request_per_target():
    """C5's amplification half. `X-Gw-No-Retry: 1` arrives as `enabled=False`,
    and a disabled budget refuses every delay -- so the only upstream requests
    left are the plan's own breadth, one per target."""
    rig = Rig(CANDIDATE, INCUMBENT, cand=status(500), inc=status(500))
    result = await rig.run(
        CANDIDATE, INCUMBENT,
        retry_policy=RetryPolicy(max_attempts=9, base_delay=0.01, enabled=False),
    )
    assert rig.routes.opened == [CANDIDATE, INCUMBENT]
    assert len(result.attempts) == 2


async def test_an_explicit_max_attempts_of_one_still_reaches_the_incumbent():
    """C5's other direction, and the one a reader gets wrong.

    `max_attempts` is the REPETITION allowance and never the breadth: breadth
    belongs to the plan, which is finite and ordered. A budget of one therefore
    buys zero repeats and leaves the fallback exactly where it was -- which is
    what `RetryPolicy.max_attempts`' own bound (`len(targets) + max_attempts -
    1`) says, and what `/probe` advertises as `max_upstream_requests`.

    Pinned because the executor's source once claimed the opposite in a
    comment. A comment that disagrees with the code is a coin flip about which
    one the next reader believes.
    """
    rig = Rig(CANDIDATE, INCUMBENT, cand=status(500), inc=ok())
    result = await rig.run(
        CANDIDATE, INCUMBENT, retry_policy=RetryPolicy(max_attempts=1)
    )
    assert rig.routes.opened == [CANDIDATE, INCUMBENT]
    assert result.served_by.provider.id == INCUMBENT
    assert len(result.attempts) == 2


async def test_the_default_path_falls_back_without_spacing_the_attempt():
    """Measured, not assumed, because the source implies otherwise.

    `execute()` says "the delay is still requested on the fallback path, so a
    fleet that fails together does not arrive at the incumbent together". It is
    requested and it is always REFUSED: silence means `NO_RETRIES`, whose
    `enabled=False` makes `delay_for` raise before it computes a window. So the
    default path's fallback is unspaced, and a fleet that fails together does
    arrive at the incumbent together.

    Asserting the real behaviour rather than the comment's, so that changing
    either one is a decision somebody makes on purpose.
    """
    rig = Rig(CANDIDATE, INCUMBENT, cand=status(500), inc=ok())
    started = rig.clock.now()
    result = await rig.run(CANDIDATE, INCUMBENT)  # retry_policy=None

    assert rig.routes.opened == [CANDIDATE, INCUMBENT]
    assert rig.clock.now() == started, "the fallback slept on a ManualClock"
    assert result.served_by.provider.id == INCUMBENT


async def test_a_configured_policy_does_space_the_fallback():
    """The pair to the test above: with a retry table, the fallback IS jittered,
    which is the anti-synchronisation property the module argues for."""
    rig = Rig(CANDIDATE, INCUMBENT, cand=status(400), inc=ok())
    # 400 is `try_next` and NOT `retry_same`, so any delay here is spent on the
    # fallback rather than on a repetition.
    policy = RetryPolicy(max_attempts=2, base_delay=0.5, max_delay=1.0)
    task = asyncio.ensure_future(
        rig.run(CANDIDATE, INCUMBENT, retry_policy=policy)
    )
    await drive(rig.clock, task, step=0.01)
    result = await task

    assert rig.routes.opened == [CANDIDATE, INCUMBENT]
    assert rig.clock.now() > 0.0, "the fallback was not spaced at all"
    assert result.served_by.provider.id == INCUMBENT


# ==========================================================================
# P3 verification: classification under fallback
# ==========================================================================


async def test_each_targets_failure_is_recorded_against_that_target():
    """Three attempts, three different providers' worth of blame.

    The attempt records are what `llmgw_attempts_total{provider,result}` fans
    out over. A loop that attributed the candidate's 500 to the incumbent would
    open a breaker against the provider that saved the request.
    """
    rig = Rig(CANDIDATE, INCUMBENT, cand=status(429, **{"retry-after": "0"}),
              inc=status(503))
    result = await rig.run(CANDIDATE, INCUMBENT)

    assert [(r.target.provider.id, r.outcome) for r in result.attempts] == [
        (CANDIDATE, "rate_limited"), (INCUMBENT, "upstream_overloaded")
    ]
    assert result.attempts[0].error.provider == CANDIDATE
    assert result.attempts[1].error.provider == INCUMBENT
    assert result.attempts[0].error.health is E.Health.NEUTRAL   # 429 is busy
    assert result.attempts[1].error.health is E.Health.FAILURE   # 503 is sick
    # C4: the client gets the LAST real error, never a summary of the walk.
    assert result.error is result.attempts[-1].error


async def test_a_failure_at_the_first_target_is_not_reattributed_when_the_second_wins():
    rig = Rig(CANDIDATE, INCUMBENT, cand=status(500), inc=ok())
    result = await rig.run(CANDIDATE, INCUMBENT)

    assert result.error is None
    assert result.attempts[0].error.provider == CANDIDATE
    assert result.attempts[0].error.model == CANDIDATE
    assert result.attempts[1].error is None
    assert result.served_by.provider.id == INCUMBENT


async def test_a_client_fault_during_a_fallback_blames_no_provider():
    """The candidate really did fail, and then the client left. C8 says the
    second fact cannot be evidence about the second provider -- and it is not,
    because `blame` and `health` say so, and those are what the breaker and
    the metrics read.

    (P4 changed one assertion here. This test used to pin `provider is None`
    on the terminal error, which was an accident of construction -- the sink
    raised below the scope that knew the target -- being read as a design
    property. The executor now attributes every error to the target the
    attempt was on, because a `StallTimeout` out of the pump with no
    provider on it would key to a breaker nothing acquires on, and the
    breaker would never learn about a provider that stalls mid-stream. The
    property C8 actually needs is that WHERE it happened is not WHO is to
    blame, so the assertion is on the blame and health columns, with the
    location present.)
    """
    rig = Rig(CANDIDATE, INCUMBENT, cand=status(500), inc=ok())
    rig.factory = CountingFactory(OnceFailingSink(0))
    result = await rig.run(CANDIDATE, INCUMBENT)

    assert rig.routes.opened == [CANDIDATE, INCUMBENT]
    assert result.error.code == "client_disconnected"
    assert result.error.provider == INCUMBENT, "attributed to where it happened"
    assert result.error.blame is E.Blame.CLIENT, "but the client is to blame"
    assert result.error.health is E.Health.NEUTRAL, "and it is not evidence"
    assert E.decide(result.error, committed=True).health is E.Health.NEUTRAL
    # The candidate's own failure survives on its own record, correctly blamed.
    assert result.attempts[0].error.provider == CANDIDATE
    assert result.attempts[0].error.blame is E.Blame.PROVIDER


async def test_a_stalling_candidate_never_charges_the_incumbent_for_the_time_it_spent():
    """A P3-only failure shape, and the taxonomy decision it forced.

    Two targets, each of which stalls after its headers. The candidate burns
    half the total on its own `first_event` clock and is correctly blamed:
    `FirstEventTimeout`, `Health.FAILURE`, the phase budget was the binding
    one and the candidate had all of it. The incumbent is then opened with
    half a total left, so `Deadline.slice()` clamps ITS first-event phase to
    the remainder -- and `phase()` reports a breach of the total as
    `TotalDeadlineExceeded`.

    Before this test was flipped, that class carried `Health.FAILURE`, and the
    health signal recorded against the INCUMBENT was a failure for a window
    the CANDIDATE spent: an incumbent that would have answered in 300 ms took
    a breaker failure for being offered 40 ms of somebody else's budget. On the
    single-target path that was an opinion; the fallback chain gave it teeth,
    because the time was now provably another target's.

    What this proves now, with P4's breaker as the consumer: the incumbent's
    record still names the incumbent and still says the total expired -- the
    attribution of WHERE is kept -- but the error is `Health.NEUTRAL`, and
    `decide()` on it produces no FAILURE signal for the breaker to count. The
    candidate's own failure is untouched: a provider that genuinely stalls
    inside its own budget is still counted.
    """
    clock = ManualClock(start=0.0)

    def stalls_after_headers(_request: httpx.Request) -> httpx.Response:
        async def gen():
            await clock.sleep(1_000_000.0)
            yield b"never"  # pragma: no cover - the first-event clock fires

        return httpx.Response(200, headers={"content-type": "text/event-stream"},
                              content=gen())

    rig = Rig(CANDIDATE, INCUMBENT, clock=clock,
              cand=stalls_after_headers, inc=stalls_after_headers)
    budgets = make_budgets(total=1.0, connect=0.5, first_event=0.5, progress=0.5)
    task = asyncio.ensure_future(rig.run(CANDIDATE, INCUMBENT, budgets=budgets))
    await drive(rig.clock, task, step=0.05)
    result = await task

    assert rig.routes.opened == [CANDIDATE, INCUMBENT]
    assert rig.factory.calls == [], "nothing was ever promised to the client"
    assert rig.clock.now() <= 1.0 + 1e-9, "two attempts took more than one total"

    candidate, incumbent = result.attempts
    # The candidate had its whole phase budget and stalled through it. That IS
    # evidence, and it is still counted.
    assert candidate.outcome == "first_event_timeout"
    assert candidate.error.provider == CANDIDATE
    assert candidate.error.health is E.Health.FAILURE
    assert E.decide(candidate.error, committed=False).health is E.Health.FAILURE

    # The incumbent was cut off by the request's clock, not its own. The
    # record keeps the where (provider) and the what (total expired); the
    # breaker hears nothing.
    assert incumbent.outcome == "total_deadline_exceeded"
    assert isinstance(incumbent.error, E.TotalDeadlineExceeded)
    assert incumbent.error.provider == INCUMBENT
    assert incumbent.error.health is E.Health.NEUTRAL
    disposition = E.decide(incumbent.error, committed=False)
    assert disposition.health is E.Health.NEUTRAL
    assert disposition.health_key == (INCUMBENT, INCUMBENT)
    assert disposition.try_next is False, "arithmetic, not policy: no time is left"
    # The whole point: the incumbent's phase was never its own budget.
    assert result.error is incumbent.error


async def test_the_client_never_gets_a_summary_when_a_real_upstream_error_existed():
    """`_final_error` prefers the last error from an attempt that reached an
    upstream. Two targets fail with real provider errors and the client is
    handed the second one -- with its status, its body and its `Retry-After`
    -- rather than a `no_targets_available` summary of our walk. A client told
    503 when the incumbent said "429, come back in 30 seconds" has been handed
    a status in place of an instruction."""
    rig = Rig(CANDIDATE, INCUMBENT, cand=status(500),
              inc=status(429, **{"retry-after": "30"}))
    result = await rig.run(CANDIDATE, INCUMBENT)

    assert result.error.code == "rate_limited"
    assert result.error.retry_after == 30.0
    assert result.error.upstream_status == 429
    assert result.error.passthrough is True
    assert result.error is result.attempts[-1].error


def test_the_scope_that_can_start_a_response_cannot_see_the_plan():
    """The first of the three structural properties, as a grep.

    `_attempt()` is handed ONE `Target` and no reference to `plan.targets`, so
    the scope that is able to create a sink is structurally unable to choose a
    different target -- "no fallback after commitment" becomes a shape rather
    than a rule somebody has to remember. Monotonicity is asserted by
    `test_the_commitment_flag_can_only_move_in_one_direction` and delegation by
    `test_the_file_never_re_derives_the_commitment_invariant`; this is the one
    that had no test.
    """
    import inspect
    import pathlib

    import llmgw.executor as module

    signature = inspect.signature(module.Executor._attempt)
    assert "target" in signature.parameters
    assert "plan" not in signature.parameters, (
        "the scope that can start the client's response can now see the plan"
    )
    source = inspect.getsource(module.Executor._attempt)
    for forbidden in ("plan.", "targets", "index"):
        assert forbidden not in source, f"_attempt() references {forbidden!r}"

    # And the sink can be created in exactly one place in the package.
    code = pathlib.Path(module.__file__).read_text()
    assert code.count("commitment.open(") == 2, (
        "the number of places that can start a client response has changed"
    )


# ==========================================================================
# The accounting hook: `on_finish` is reached exactly once, on every exit
# ==========================================================================


def partial_stream(clock: ManualClock, frames: int = 3):
    """200, a few real frames, then silence for ever. The client is mid-answer.

    Fewer frames than the whole stream on purpose: the terminal marker must
    NOT arrive, or the pump completes and the cancellation lands after the
    result -- which is the completed path, and the test would prove nothing.
    """

    def make(_request: httpx.Request) -> httpx.Response:
        async def gen():
            for frame in wire.openai_stream()[:frames]:
                yield frame
            await clock.sleep(1_000_000.0)

        return httpx.Response(200, headers={"content-type": "text/event-stream"},
                              content=gen())

    return make


async def test_the_accounting_hook_sees_a_completed_result_exactly_once():
    """The happy path: the object the hook gets IS the object `execute()`
    returns, and it gets it once. A hook fed a copy, or fed twice, is a record
    written twice or a record that disagrees with the header the client saw."""
    seen: list[ExecutionResult] = []
    rig = Rig(CANDIDATE, INCUMBENT, cand=status(500), inc=ok())
    result = await rig.run(CANDIDATE, INCUMBENT, on_finish=seen.append)

    assert result.outcome is Outcome.COMPLETED
    assert seen == [result] and seen[0] is result
    assert codes(seen[0]) == ["upstream_server_error", "success"]


async def test_the_accounting_hook_sees_a_failed_result_exactly_once():
    """Every target failed, nothing was promised, and the hook still gets the
    same result the caller does -- with both attempts on it."""
    seen: list[ExecutionResult] = []
    rig = Rig(CANDIDATE, INCUMBENT, cand=status(500), inc=status(503))
    result = await rig.run(CANDIDATE, INCUMBENT, on_finish=seen.append)

    assert result.outcome is Outcome.FAILED
    assert seen == [result] and seen[0] is result
    assert rig.factory.calls == []


async def test_a_client_disconnect_mid_answer_still_reaches_the_accounting_hook():
    """Where C3 and C8 used to collide, at the executor level, on a manual clock.

    The candidate fails, the incumbent streams three frames and then goes
    quiet, and the execution is cancelled from outside -- which is exactly
    what `run_until_disconnect` does when the client hangs up. C8 says the
    `CancelledError` propagates and no provider is blamed; C3 says the tokens
    the incumbent generated are billed. Both, in one result:

      * the hook is called exactly once, from inside the cancellation;
      * `outcome` is CANCELED, `error` is None -- the executor does not know
        why it was cancelled and does not pretend to;
      * `attempts` is what had happened: the candidate's real failure and a
        `"canceled"` record for the incumbent, so `len(attempts)` agrees with
        the `X-Gw-Attempts` the client was already sent;
      * `committed` is True and `served_by` names the incumbent, because its
        bytes are what the client holds;
      * `pump` carries the partial usage, non-zero.

    And the `CancelledError` still comes out the other side.
    """
    clock = ManualClock(start=0.0)
    seen: list[ExecutionResult] = []
    rig = Rig(CANDIDATE, INCUMBENT, clock=clock,
              cand=status(500), inc=partial_stream(clock))

    task = asyncio.create_task(
        rig.run(CANDIDATE, INCUMBENT, total=600.0, on_finish=seen.append)
    )
    for _ in range(40):
        await asyncio.sleep(0)
    assert rig.routes.opened == [CANDIDATE, INCUMBENT]
    assert rig.factory.calls == [200], "the incumbent's stream had started"
    assert seen == [], "the hook fired before the execution ended"

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert len(seen) == 1, "the hook must be reached exactly once on a disconnect"
    result = seen[0]
    assert result.outcome is Outcome.CANCELED
    assert result.error is None
    assert codes(result) == ["upstream_server_error", "canceled"]
    assert result.attempts[1].target.provider.id == INCUMBENT
    assert result.attempts[1].status == 200
    assert result.attempts[1].error is None
    assert result.attempts[1].committed is True
    assert result.committed is True
    assert result.served_by is not None and result.served_by.provider.id == INCUMBENT
    assert result.pump is not None, "C3: the partial usage has to survive"
    assert result.pump.committed is True
    assert result.pump.events > 0 and result.pump.bytes_out > 0
    assert result.pump.terminal_seen is False, "the answer the client holds is truncated"
    assert rig.upstream.in_flight() == {}, "a cancelled attempt left a response open"


async def test_a_cancellation_before_commitment_reaches_the_hook_as_uncommitted():
    """The other window: the candidate never answered its connect, so nothing
    was promised. The hook still gets a result, and every field on it says so
    -- `committed` False, `served_by` None, no pump, and one `"canceled"`
    record for the attempt that was cut off."""
    clock = ManualClock(start=0.0)
    seen: list[ExecutionResult] = []
    rig = Rig(CANDIDATE, INCUMBENT, clock=clock, cand=hangs(clock), inc=ok())

    task = asyncio.create_task(
        rig.run(CANDIDATE, INCUMBENT, total=600.0, on_finish=seen.append)
    )
    for _ in range(20):
        await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert len(seen) == 1
    result = seen[0]
    assert result.outcome is Outcome.CANCELED
    assert result.committed is False
    assert result.served_by is None
    assert result.pump is None
    assert codes(result) == ["canceled"]
    assert result.attempts[0].status is None, "no status line ever arrived"
    assert result.attempts[0].committed is False
    assert rig.routes.opened == [CANDIDATE], "cancellation did not trigger a fallback"
    assert rig.factory.calls == []


async def test_a_cancellation_during_a_retry_sleep_records_no_phantom_attempt():
    """Cancelled while the budget was spacing a repeat: no attempt was in
    flight, so none is invented. The failed attempt's record is already there
    and the result is CANCELED on top of it -- `attempts` is what happened,
    not what was about to."""
    clock = ManualClock(start=0.0)
    seen: list[ExecutionResult] = []
    rig = Rig(CANDIDATE, INCUMBENT, clock=clock, cand=status(500), inc=ok())
    policy = RetryPolicy(max_attempts=3, base_delay=5.0, max_delay=5.0)

    task = asyncio.create_task(
        rig.run(CANDIDATE, INCUMBENT, total=600.0, retry_policy=policy,
                on_finish=seen.append)
    )
    for _ in range(40):
        await asyncio.sleep(0)
    assert rig.routes.opened == [CANDIDATE]
    assert not task.done(), "the budget should be sleeping on the manual clock"

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert len(seen) == 1
    result = seen[0]
    assert result.outcome is Outcome.CANCELED
    assert codes(result) == ["upstream_server_error"], "no phantom in-flight record"
    assert result.committed is False and result.served_by is None
    assert rig.routes.opened == [CANDIDATE], "the sleep was cancelled, not skipped"


async def test_a_second_cancellation_during_the_hook_cannot_lose_the_record_or_hang():
    """The `finally` has no suspension point between entry and the hook, so a
    cancellation that arrives WHILE the hook runs -- here, from the hook itself,
    which is the most hostile timing there is -- has nowhere to land. The hook
    runs once, the record is delivered, and the task ends as cancelled rather
    than hanging on a second wake-up nobody is waiting for."""
    clock = ManualClock(start=0.0)
    seen: list[ExecutionResult] = []
    holder: dict[str, asyncio.Task] = {}

    def hostile(result: ExecutionResult) -> None:
        seen.append(result)
        holder["task"].cancel()  # a second cancel, mid-finally

    rig = Rig(CANDIDATE, INCUMBENT, clock=clock,
              cand=status(500), inc=partial_stream(clock))
    holder["task"] = asyncio.create_task(
        rig.run(CANDIDATE, INCUMBENT, total=600.0, on_finish=hostile)
    )
    for _ in range(40):
        await asyncio.sleep(0)
    holder["task"].cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(holder["task"], timeout=2.0)

    assert len(seen) == 1
    assert seen[0].outcome is Outcome.CANCELED
    assert seen[0].pump is not None and seen[0].pump.events > 0
    assert rig.upstream.in_flight() == {}


async def test_a_hook_that_raises_neither_fails_the_request_nor_eats_the_cancellation(
    caplog: pytest.LogCaptureFixture,
):
    """Accounting observes; it does not get a vote.

    A hook that raises on the completed path would turn a stream the client
    already has into an error the client cannot see. On the cancelled path it
    would REPLACE the `CancelledError`, which is C8 quietly undone by a
    logging bug. Both are logged and neither changes the exit.
    """
    import logging

    def broken(_result: ExecutionResult) -> None:
        raise RuntimeError("accounting is down")

    with caplog.at_level(logging.ERROR, logger="llmgw.executor"):
        rig = Rig(CANDIDATE, INCUMBENT, cand=status(500), inc=ok())
        result = await rig.run(CANDIDATE, INCUMBENT, on_finish=broken)
        assert result.outcome is Outcome.COMPLETED

        clock = ManualClock(start=0.0)
        rig = Rig(CANDIDATE, INCUMBENT, clock=clock,
                  cand=status(500), inc=partial_stream(clock))
        task = asyncio.create_task(
            rig.run(CANDIDATE, INCUMBENT, total=600.0, on_finish=broken)
        )
        for _ in range(40):
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    raised = [r for r in caplog.records if "on_finish hook raised" in r.getMessage()]
    assert len(raised) == 2, [r.getMessage() for r in caplog.records]
    assert "outcome=completed" in raised[0].getMessage()
    assert "outcome=canceled" in raised[1].getMessage()


async def test_attempt_outcomes_stay_a_closed_vocabulary_on_the_cancelled_path():
    """`"canceled"` is the one word added to the attempt vocabulary, and it is
    `Outcome.CANCELED.value` rather than a new string, so the metric label it
    becomes is one the outcome label already has."""
    clock = ManualClock(start=0.0)
    seen: list[ExecutionResult] = []
    rig = Rig(CANDIDATE, INCUMBENT, clock=clock, cand=status(500),
              inc=partial_stream(clock))
    task = asyncio.create_task(
        rig.run(CANDIDATE, INCUMBENT, total=600.0, on_finish=seen.append)
    )
    for _ in range(40):
        await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    allowed = E.ERROR_CODES | {"success", Outcome.CANCELED.value}
    assert {a.outcome for a in seen[0].attempts} <= allowed


# ==========================================================================
# P4: the attempt gate -- breaker tickets and provider-key permits
# ==========================================================================
#
# Everything above ran with `breakers=None, limiter=None`, which is "no gate"
# and is why none of it changed. These run the same rig with a real
# `BreakerRegistry` and a real `ProviderKeyLimiter` on the same `ManualClock`,
# and ask the questions the wiring can get wrong: is a refusal an attempt,
# which circuit hears which failure, and does every ticket and permit come
# back on every exit.


from llmgw.admission import ProviderKeyLimiter  # noqa: E402
from llmgw.breaker import BreakerPolicy, BreakerRegistry, BreakerState  # noqa: E402
from llmgw.executor import credential_health_key  # noqa: E402

TRIP_AT_ONE = BreakerPolicy(failure_threshold=1, window=30.0, cooldown=10.0)


class GatedRig(Rig):
    """`Rig` plus the gate. One registry, one limiter, the rig's clock."""

    def __init__(self, *pids: str, policy: BreakerPolicy = TRIP_AT_ONE,
                 clock: ManualClock | None = None, **by_host):
        super().__init__(*pids, clock=clock, **by_host)
        self.breakers = BreakerRegistry(policy, clock=self.clock)
        self.limiter = ProviderKeyLimiter()
        self.executor = Executor(
            self.upstream, clock=self.clock,
            breakers=self.breakers, limiter=self.limiter,
        )

    def breaker(self, pid: str):
        return self.breakers.for_key((pid, pid))

    def cred_breaker(self, pid: str):
        return self.breakers.for_key(credential_health_key(self.catalog.resolve(pid)))

    def trip(self, key: tuple[str, str]) -> None:
        """Open a circuit the way traffic would: a real FAILURE disposition
        through `record()`, not a poke at private state."""
        self.trip_hard(key, self.breakers.for_key(key).policy.failure_threshold)

    def trip_hard(self, key: tuple[str, str], failures: int | None = None) -> None:
        breaker = self.breakers.for_key(key)
        n = breaker.policy.failure_threshold if failures is None else failures
        for _ in range(n):
            err = E.UpstreamServerError("500", provider=key[0], model=key[1])
            breaker.record(breaker.acquire(), E.decide(err, committed=False))
        assert breaker.state is BreakerState.OPEN


def refusals(result: ExecutionResult) -> list[str]:
    return [r.error.code for r in result.refusals]


def fails_once_then(ok_handler, *, code: int = 500):
    """A target that answers `code` on its first open and `ok_handler` after."""
    calls = {"n": 0}

    def make(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return status(code)(request)
        return ok_handler(request)

    return make


def test_the_credential_key_agrees_with_the_taxonomy():
    """`credential_health_key()` must produce the exact tuple a real
    `AuthenticationFailed` records under, or the executor holds a ticket on
    one key while `decide()` names another and the 401 is never counted.
    Pinned against the taxonomy's own `health_key()` rather than against a
    string literal, so the spelling has one owner."""
    target = catalog_for(CANDIDATE).resolve(CANDIDATE)
    err = E.AuthenticationFailed(
        "401", provider=target.provider.id, model=target.model.id,
        credential_id=target.credential_key,
    )
    assert credential_health_key(target) == err.health_key()
    assert credential_health_key(target) != target.health_key


async def test_an_open_circuit_is_refused_before_a_socket_and_is_not_an_attempt():
    """The breaker's whole value, from the loop's side.

    The candidate's circuit is OPEN, so `acquire()` raises before a socket:
    `routes.opened` shows the incumbent only. And the refusal is a `Refusal`,
    not an `AttemptRecord` -- `attempts` has one entry, the success -- because
    the candidate was never asked, and `X-Gw-Attempts` is a count of asking.
    """
    rig = GatedRig(CANDIDATE, INCUMBENT, cand=ok(), inc=ok())
    rig.trip((CANDIDATE, CANDIDATE))
    result = await rig.run(CANDIDATE, INCUMBENT)

    assert rig.routes.opened == [INCUMBENT], "the open circuit cost a socket"
    assert codes(result) == ["success"]
    assert refusals(result) == ["breaker_open"]
    assert result.refusals[0].target.provider.id == CANDIDATE
    assert result.refusals[0].error.retry_after is not None, "OPEN carries the cooldown"
    assert result.served_by.provider.id == INCUMBENT
    assert result.outcome is Outcome.COMPLETED


async def test_a_failure_lands_on_the_target_circuit_and_not_the_credential():
    """A 500 says the model is sick. It says nothing about the key."""
    rig = GatedRig(CANDIDATE, INCUMBENT, cand=status(500), inc=ok())
    result = await rig.run(CANDIDATE, INCUMBENT)

    assert codes(result) == ["upstream_server_error", "success"]
    assert rig.breaker(CANDIDATE).state is BreakerState.OPEN
    assert rig.cred_breaker(CANDIDATE).state is BreakerState.CLOSED
    assert rig.breaker(INCUMBENT).state is BreakerState.CLOSED


async def test_an_auth_failure_lands_on_the_credential_circuit_and_not_the_target():
    """FAILURE-MODES row 8, through the whole loop: the 401 is recorded on
    `(provider, "cred:<id>")` -- the ticket that key issued -- and the
    target's own ticket is released uncounted. The blast radius is the
    credential, exactly as `errors.HealthScope.CREDENTIAL` decided, and the
    executor did not re-decide it."""
    rig = GatedRig(CANDIDATE, INCUMBENT, cand=status(401), inc=ok())
    result = await rig.run(CANDIDATE, INCUMBENT)

    assert codes(result) == ["authentication_failed", "success"]
    assert rig.cred_breaker(CANDIDATE).state is BreakerState.OPEN
    assert rig.breaker(CANDIDATE).state is BreakerState.CLOSED, (
        "a credential failure opened the model's circuit"
    )


async def test_an_open_credential_circuit_refuses_before_a_socket():
    """The other half of holding two tickets: a known-bad key is refused,
    not re-tried. Without a ticket on the credential key, that circuit could
    be written to and never consulted."""
    rig = GatedRig(CANDIDATE, INCUMBENT, cand=ok(), inc=ok())
    rig.trip(credential_health_key(rig.catalog.resolve(CANDIDATE)))
    result = await rig.run(CANDIDATE, INCUMBENT)

    assert rig.routes.opened == [INCUMBENT]
    assert refusals(result) == ["breaker_open"]
    assert rig.breaker(CANDIDATE).state is BreakerState.CLOSED, (
        "the target ticket taken before the credential refusal was not released"
    )


async def test_a_success_closes_a_half_open_circuit_and_records_nothing_on_a_closed_one():
    """`record(ticket, None)`: the probe closes the circuit; a success on a
    CLOSED circuit changes nothing. Both tickets are settled with `None`
    because a response proves the model AND the credential work."""
    rig = GatedRig(CANDIDATE, INCUMBENT, cand=ok(), inc=ok())
    rig.trip((CANDIDATE, CANDIDATE))
    await rig.clock.advance(10.0)
    assert rig.breaker(CANDIDATE).state is BreakerState.HALF_OPEN

    result = await rig.run(CANDIDATE, INCUMBENT)

    assert rig.routes.opened == [CANDIDATE], "the probe was refused"
    assert codes(result) == ["success"]
    assert result.refusals == []
    assert rig.breaker(CANDIDATE).state is BreakerState.CLOSED
    assert rig.breaker(CANDIDATE).snapshot()["probes_in_flight"] == 0


async def test_a_cancelled_probe_returns_its_slot():
    """Permanent outage by a cancelled client. The half-open probe's
    client hangs up mid-connect; the loop's `finally` releases the ticket,
    the slot comes back, and the NEXT request is admitted as the probe. A
    `record()` here instead of a `release()` would leave `probes_in_flight`
    at one forever."""
    clock = ManualClock(start=0.0)
    rig = GatedRig(CANDIDATE, INCUMBENT, clock=clock, cand=hangs(clock), inc=ok())
    rig.trip((CANDIDATE, CANDIDATE))
    await clock.advance(10.0)

    task = asyncio.create_task(rig.run(CANDIDATE, INCUMBENT, total=600.0))
    for _ in range(20):
        await asyncio.sleep(0)
    assert rig.routes.opened == [CANDIDATE], "the probe did not go out"
    assert rig.breaker(CANDIDATE).snapshot()["probes_in_flight"] == 1

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    snap = rig.breaker(CANDIDATE).snapshot()
    assert snap["state"] == "half_open", "a cancellation moved the state"
    assert snap["probes_in_flight"] == 0, "the cancelled probe kept its slot"
    assert rig.limiter.total_in_use() == 0, "the cancelled attempt kept its permit"
    # And the slot is usable: the next acquire is the probe.
    assert rig.breaker(CANDIDATE).acquire().probe is True


async def test_ten_client_cancellations_leave_a_healthy_circuit_closed_with_no_failures():
    """`breaker_ignores_client_cancel` at the executor. Ten cancellations
    mid-attempt, zero counted failures, CLOSED, and nothing held."""
    clock = ManualClock(start=0.0)
    rig = GatedRig(CANDIDATE, INCUMBENT, clock=clock, cand=hangs(clock), inc=ok())
    for _ in range(10):
        task = asyncio.create_task(rig.run(CANDIDATE, INCUMBENT, total=600.0))
        for _ in range(20):
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    snap = rig.breaker(CANDIDATE).snapshot()
    assert snap["state"] == "closed"
    assert snap["failures_in_window"] == 0
    assert snap["transitions"] == 0
    assert rig.limiter.total_in_use() == 0


async def test_a_full_provider_key_is_refused_before_a_socket_and_falls_back():
    """Row 7 at the loop: the credential is at its cap (held by someone
    else), so the candidate is `ProviderKeyExhausted` -- `try_next`, no
    socket, a `Refusal` -- and the incumbent, on a different key, serves.
    The breaker tickets minted before the permit refusal are released: the
    candidate's circuit shows nothing in flight and nothing counted."""
    rig = GatedRig(CANDIDATE, INCUMBENT, cand=ok(), inc=ok())
    cap = rig.catalog.resolve(CANDIDATE).provider.max_concurrency
    held = [rig.limiter.acquire(CANDIDATE, cap) for _ in range(cap)]
    try:
        result = await rig.run(CANDIDATE, INCUMBENT)
        assert rig.limiter.in_use(CANDIDATE) == cap, "the refusal changed the count"
        assert rig.limiter.in_use(INCUMBENT) == 0, "the incumbent's permit leaked"
    finally:
        for permit in held:
            permit.release()

    assert rig.routes.opened == [INCUMBENT]
    assert refusals(result) == ["provider_key_exhausted"]
    assert result.refusals[0].error.credential_id == CANDIDATE
    assert codes(result) == ["success"]
    assert rig.breaker(CANDIDATE).snapshot()["failures_in_window"] == 0
    assert rig.limiter.total_in_use() == 0


async def test_permits_return_to_zero_on_success_failure_and_cancellation():
    """Row 19 at the loop, all three exits of the `async with`."""
    clock = ManualClock(start=0.0)
    rig = GatedRig(CANDIDATE, INCUMBENT, clock=clock, cand=status(500), inc=ok())
    await rig.run(CANDIDATE, INCUMBENT)
    assert rig.limiter.total_in_use() == 0, "after a failure and a success"

    rig = GatedRig(CANDIDATE, INCUMBENT, clock=clock, cand=hangs(clock), inc=ok())
    task = asyncio.create_task(rig.run(CANDIDATE, INCUMBENT, total=600.0))
    for _ in range(20):
        await asyncio.sleep(0)
    assert rig.limiter.total_in_use() == 1, "the permit was not held during the attempt"
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert rig.limiter.total_in_use() == 0, "after a cancellation"


async def test_a_stall_inside_the_pump_is_attributed_to_the_target_it_happened_at():
    """The gap that made attribution necessary. `Pump` raises `StallTimeout`
    with no provider or model -- it has no target in scope -- and a
    `Disposition` built from it would key to `("?", "?")`, a circuit nothing
    acquires on. The loop fills in the target it was on before `decide()`,
    so the failure reaches the circuit that admitted it. Post-commitment, so
    this is also the case where the breaker is the ONLY thing that can act
    on the failure: the client's response is already truncated."""
    clock = ManualClock(start=0.0)
    rig = GatedRig(CANDIDATE, INCUMBENT, clock=clock,
                   cand=partial_stream(clock), inc=ok())
    task = asyncio.create_task(
        rig.run(CANDIDATE, INCUMBENT, budgets=make_budgets(progress=1.0))
    )
    await drive(clock, task, step=0.5)
    result = await task

    assert codes(result) == ["stall_timeout"]
    assert result.error.provider == CANDIDATE, "the stall was not attributed"
    assert result.error.model == CANDIDATE
    assert result.error.workload == "chat"
    assert result.committed is True
    assert rig.breaker(CANDIDATE).state is BreakerState.OPEN, (
        "a mid-stream stall never reached the breaker"
    )
    assert rig.cred_breaker(CANDIDATE).state is BreakerState.CLOSED


async def test_an_error_that_already_names_its_target_is_not_renamed():
    """Attribution fills blanks and only blanks. An error the upstream layer
    raised with its identity intact keeps every field it set."""
    rig = GatedRig(CANDIDATE, INCUMBENT, cand=status(500), inc=ok())
    result = await rig.run(CANDIDATE, INCUMBENT)
    err = result.attempts[0].error
    assert (err.provider, err.model, err.credential_id) == (CANDIDATE, CANDIDATE, CANDIDATE)
    assert err.workload == "chat"


async def test_when_every_target_is_refused_the_client_sees_the_last_refusal():
    """Nothing reached an upstream, so there is no upstream error to pass
    through; the next most useful thing is the refusal, which carries the
    cooldown as `retry_after`. Not `NoTargetsAvailable`: that is a summary,
    and there is a real reason available."""
    rig = GatedRig(CANDIDATE, INCUMBENT, cand=ok(), inc=ok())
    rig.trip((CANDIDATE, CANDIDATE))
    rig.trip((INCUMBENT, INCUMBENT))
    result = await rig.run(CANDIDATE, INCUMBENT)

    assert rig.routes.opened == []
    assert result.attempts == []
    assert refusals(result) == ["breaker_open", "breaker_open"]
    assert result.error.code == "breaker_open"
    assert result.error.provider == INCUMBENT, "the LAST refusal"
    assert result.error.retry_after is not None
    assert result.outcome is Outcome.FAILED
    assert result.served_by is None


async def test_a_refusal_beats_a_summary_but_not_a_real_upstream_error():
    """Candidate refused, incumbent answered 500: the 500 is the error, as C4
    wants -- the provider spoke, and a client should hear the provider."""
    rig = GatedRig(CANDIDATE, INCUMBENT, cand=ok(), inc=status(500))
    rig.trip((CANDIDATE, CANDIDATE))
    result = await rig.run(CANDIDATE, INCUMBENT)

    assert refusals(result) == ["breaker_open"]
    assert codes(result) == ["upstream_server_error"]
    assert result.error.code == "upstream_server_error"


async def test_a_refusal_spends_neither_the_budget_nor_a_backoff():
    """`max_attempts=2` buys one repetition. If the candidate's refusal had
    been charged as an attempt, the incumbent's first 500 could not be
    retried and the request would fail; it is not, so the incumbent is
    asked twice and answers. And the fallback after the refusal is
    immediate: there was no load to space it from.

    Threshold 5 rather than the file's usual 1, because at 1 the incumbent's
    own 500 opens the incumbent's circuit and the repetition is REFUSED --
    which is the breaker being right, and a different test."""
    rig = GatedRig(CANDIDATE, INCUMBENT, policy=BreakerPolicy(failure_threshold=5),
                   cand=ok(), inc=fails_once_then(ok()))
    rig.trip_hard((CANDIDATE, CANDIDATE))
    policy = RetryPolicy(max_attempts=2, base_delay=0.01, max_delay=0.02)
    task = asyncio.create_task(rig.run(CANDIDATE, INCUMBENT, retry_policy=policy))
    await drive(rig.clock, task, step=0.01)
    result = await task

    assert rig.routes.opened == [INCUMBENT, INCUMBENT], "the refusal spent the retry"
    assert codes(result) == ["upstream_server_error", "success"]
    assert refusals(result) == ["breaker_open"]


async def test_the_accounting_hook_sees_refusals_on_a_cancelled_result():
    """`on_finish` on the cancellation path carries the refusals too, and a
    cancellation that lands BEFORE `upstream.open()` -- there is no such
    window with a real socket, but there is one between the two acquires
    -- is not written up as a cancelled attempt."""
    clock = ManualClock(start=0.0)
    rig = GatedRig(CANDIDATE, INCUMBENT, clock=clock, cand=ok(), inc=hangs(clock))
    rig.trip((CANDIDATE, CANDIDATE))
    seen: list[ExecutionResult] = []
    task = asyncio.create_task(
        rig.run(CANDIDATE, INCUMBENT, total=600.0, on_finish=seen.append)
    )
    for _ in range(20):
        await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert len(seen) == 1
    assert seen[0].outcome is Outcome.CANCELED
    assert refusals(seen[0]) == ["breaker_open"]
    assert codes(seen[0]) == ["canceled"], "the incumbent WAS opened"
    assert rig.breaker(INCUMBENT).snapshot()["failures_in_window"] == 0
    assert rig.limiter.total_in_use() == 0


def test_the_gate_is_inside_the_scope_that_holds_one_target():
    """The acquires live in `_attempt()`, alongside the open they guard, and
    nowhere else. The loop settles; it does not admit. A grep, like its
    neighbours, because this is the property an incident-time edit erodes."""
    import inspect

    import llmgw.executor as module

    def code_of(fn) -> str:
        # Docstrings describe the gate; the assertion is about the code.
        return inspect.getsource(fn).replace(fn.__doc__ or "", "")

    attempt = code_of(module.Executor._attempt)
    assert "_acquire_tickets(" in attempt
    assert "_acquire_permit(" in attempt
    assert "record_attempt()" in attempt, "the budget is charged after the gate"
    execute = code_of(module.Executor.execute)
    assert "acquire(" not in execute, "the loop admits; that is _attempt()'s job"
    assert "record_attempt()" not in execute, "the budget is charged before the gate"


async def test_two_providers_on_one_credential_share_one_auth_circuit():
    """Two routing configs over one credential, pinned.

    `openrouter` and `openrouter-toolsafe` are two routing configs over one
    real API key. An auth failure is about the KEY, so both must record on a
    single credential circuit -- otherwise a revoked key is hammered at twice
    the breaker threshold before both halves trip, and a circuit opened by
    traffic through one config never protects a fallback through the other.

    Proven at the key level (the breaker's dict key), which is where the split
    lived: the two targets' credential keys are identical and carry no
    provider entry, so they land on the same `Breaker`.
    """
    from llmgw.catalog import DEFAULT_CATALOG
    from llmgw.executor import credential_health_key

    plain = DEFAULT_CATALOG.resolve("openrouter.qwen-3.6-plus")     # entry "openrouter"
    toolsafe = DEFAULT_CATALOG.resolve("openrouter.deepseek-v4-pro")  # entry "…-toolsafe"
    assert plain.provider.id != toolsafe.provider.id               # genuinely two entries
    assert plain.credential_key == toolsafe.credential_key == "openrouter"
    assert credential_health_key(plain) == credential_health_key(toolsafe)
    assert credential_health_key(plain) == ("cred", "openrouter")
    # And it is NOT the model-scoped key, so an ordinary 5xx and a 401 never
    # share a circuit.
    assert credential_health_key(plain) != plain.health_key


def test_a_breaker_without_a_limiter_is_refused_at_construction():
    """A breaker without a limiter is a pool-timeout hazard.

    `Timeout(None)` disables httpx's pool timeout, so a request that finds a
    full connection pool waits in the queue until our connect `phase()` fires
    it as `HeadersTimeout` -- FAILURE health. With a breaker wired and no
    per-credential limiter to refuse the overflow first, that timeout opens a
    circuit against a provider that was busy, not broken: capacity erosion
    with no visible cause (FAILURE-MODES row 19). The limiter's cap is the
    pool's cap, so it refuses the overflow as NEUTRAL before the pool blocks.
    Requiring the two together is the guard, made at construction rather than
    hoped for at runtime.
    """
    from llmgw.breaker import BreakerPolicy, BreakerRegistry

    reg = BreakerRegistry(BreakerPolicy(), clock=ManualClock(start=0.0))
    with pytest.raises(ValueError, match="requires limiter"):
        Executor(object(), clock=ManualClock(start=0.0), breakers=reg)


def test_a_breaker_with_a_limiter_constructs():
    from llmgw.admission import ProviderKeyLimiter
    from llmgw.breaker import BreakerPolicy, BreakerRegistry

    clock = ManualClock(start=0.0)
    Executor(object(), clock=clock,
             breakers=BreakerRegistry(BreakerPolicy(), clock=clock),
             limiter=ProviderKeyLimiter())


def test_the_bare_executor_needs_neither():
    """The gateless executor -- what every pre-P4 test uses -- must still
    build with no breaker and no limiter."""
    Executor(object(), clock=ManualClock(start=0.0))
