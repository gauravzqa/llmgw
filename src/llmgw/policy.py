"""Policy: workloads, execution plans, and the immutable snapshot that pins them.

Layer 2 of the stack. It answers one question --
*given a workload name, which targets do we try, in what order, under what
budgets?* -- and it answers it from a value that cannot change while the
request is running.

--------------------------------------------------------------------------
The failure this module exists to close
--------------------------------------------------------------------------

FAILURE-MODES row 10, "config reload mid-request". Without a snapshot the
serving path reads live configuration:

    plan = config.workloads[name]        # v1: candidate=deepseek
    ... 40 seconds of streaming ...
    price = config.models[...]           # v2: someone reloaded; prices moved

The request was *routed* by v1 and *billed* by v2, and -- this is the part
that makes it expensive rather than merely wrong -- nothing in the capture
record can say so. There is one `policy_id` field and two policies took part.
You cannot reconstruct the decision, you cannot reproduce the bug, and the
cost report is confidently off by an amount nobody can bound.

So the request does not read configuration. It reads a `PolicySnapshot`:
a frozen value taken once at ingress and carried to the accounting record.
`PolicyStore.replace()` rebinds one attribute; a request that already called
`current()` holds the old object and is unaffected, because there is no
mutation for it to observe. Split-brain becomes structurally impossible
rather than merely unlikely.

That is also why `workloads` is typed `Mapping` and stored as a
`MappingProxyType`, and why `retry` config is frozen on the way in. A frozen
dataclass wrapping a mutable dict is a value that advertises immutability and
does not have it, which is worse than one that never claimed it.

--------------------------------------------------------------------------
Why the id is a content hash
--------------------------------------------------------------------------

`pol_1a2b3c4d` is a truncated SHA-256 over the canonicalised policy content.
The two obvious alternatives are both wrong, for the same underlying reason:

    a counter    -- `policy_v7` means "the seventh reload *this process* did".
                    Two machines that loaded the identical file disagree, one
                    machine that restarted forgets, and a rollback to the
                    previous file produces v8, not v6. The id then cannot be
                    used as a join key across a fleet, which is the only
                    thing anyone ever wants to use it for.
    a timestamp  -- names *when we read the file*, not *what the file said*.
                    Every instance in a rolling deploy gets a different id for
                    the same config, so "which requests ran under the old
                    policy" becomes unanswerable at precisely the moment it
                    is being asked.

A content hash is the same on every machine, survives restarts, and returns
to the old value when you roll back -- so `policy_id=pol_1a2b3c4d` in a
capture record from one box means the same thing as in one from another box,
and grouping by it is meaningful. That is the whole property.

Canonicalisation is over *meaning*, not bytes: the parsed, defaults-resolved
policy re-serialised as JSON with sorted keys. Reordering two workloads, or
moving a budget from an inline table to a sub-table, does not change what the
gateway does, so it must not change the id. Comments and whitespace do not
change it either -- documented rather than accidental, because "an id that
changes when you fix a typo in a comment" is an id that churns your
dashboards for no reason.

--------------------------------------------------------------------------
Validation happens here, at construction, once
--------------------------------------------------------------------------

Same argument as `Catalog._validate()`, one layer up. Every model id resolves,
every `Budgets` passes `validate()`, every workload is checked against the
catalog *before* the snapshot exists. A snapshot that was constructed is a
snapshot that routes; `plan_for()` does dictionary lookups and tuple
building and cannot fail on a config error.

This matters more than it does for the catalog, because a snapshot is built
on *reload* as well as on startup. A bad config that raises during
construction leaves the previous snapshot serving traffic. A bad config that
raises on the request path takes the traffic with it.

Every raise here is `errors.PolicyError` -- `Health.NEUTRAL`, `Blame.POLICY`
-- and not `ValueError`, deliberately: a reload failure is a countable event
with a `code` that fits in `llmgw_requests_total{code=...}`, not a stray
builtin. Punishing a provider's breaker for our own typo is how you end up
with a circuit open against a provider that was never called.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import tomllib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from .catalog import Catalog, Target
from .clocks import Budgets, Clock, SystemClock
from .errors import PolicyError

DEFAULT_BUDGETS = Budgets(
    total=600.0, connect=2.0, first_event=20.0, progress=15.0, client_stall=30.0,
)
"""The budgets a workload gets when it says nothing.

Deliberately identical to `server.config.ServerConfig`'s defaults. Two default
sets that drift apart is a gateway where the answer to "what is the timeout"
is "which file are you reading".
"""

_ID_PREFIX = "pol_"
_CATALOG_ID_PREFIX = "cat_"
_ID_CHARS = 8
"""Hex characters kept from the digest.

32 bits, sized for a log line and a header value rather than for adversarial
collision resistance -- nothing security-relevant keys off this id, and the
full digest is on `content_digest` for anyone who wants it. At the scale a
policy file changes (tens to hundreds of distinct versions in a system's
life) the birthday bound is not close.
"""

_BUDGET_KEYS = frozenset(f.name for f in dataclasses.fields(Budgets))
_WORKLOAD_KEYS = frozenset({"incumbent", "candidate", "budgets", "retry"})
_TOP_LEVEL_KEYS = frozenset({"default_workload", "defaults", "workloads"})


# ==========================================================================
# Freezing
# ==========================================================================


def _freeze(value: object) -> object:
    """Deep-freeze parsed config into something a snapshot can hold.

    `tomllib` hands back dicts and lists. Storing either inside a frozen
    dataclass produces a value that *looks* immutable and is not: nothing
    stops a caller from reaching through `plan.retry["max_attempts"] = 9` and
    changing the policy for every request that already pinned this snapshot.
    That is exactly the row-10 bug wearing a different hat, so the mutable
    containers do not survive the boundary.
    """
    if isinstance(value, Mapping):
        return MappingProxyType({str(k): _freeze(v) for k, v in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(v) for v in value)
    return value


def _plain(value: object) -> Any:
    """Inverse of `_freeze`, for hashing and for handing config to a
    constructor. `json.dumps` does not know what a `MappingProxyType` is."""
    if isinstance(value, Mapping):
        return {str(k): _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value


# ==========================================================================
# Workload
# ==========================================================================


@dataclass(frozen=True, slots=True)
class Workload:
    """One named routing decision: who to try, in what order, for how long.

    `incumbent` is required and `candidate` is not, because that is the shape
    of the question this abstraction is for. The incumbent is the model you
    are already running in production and would ship today. The candidate is
    the one you are evaluating. Making the *cheap* or *new* one optional and
    the *known-good* one mandatory means a workload can never be configured
    into a state where there is nothing to fall back to.
    """

    id: str
    incumbent: str
    """Model id in the Catalog. Tried LAST -- it is the safety net."""

    candidate: str | None = None
    """Model id tried FIRST when present. This is the A/B lever: traffic goes
    to the candidate, and the incumbent catches whatever the candidate drops.
    Absent, the workload is a plain single-target route."""

    budgets: Budgets = DEFAULT_BUDGETS
    retry: object | None = None
    """A `retry.RetryPolicy`, or the frozen mapping `from_toml` parsed for one.

    TEMPORARY LOOSENESS, and stated so it is not mistaken for design: the real
    type is `llmgw.retry.RetryPolicy`, and this field is `object | None` only
    because `retry.py` is being written concurrently. Importing it now would
    couple two in-flight modules and turn a merge into a rebase. When
    `retry.py` lands, this becomes `RetryPolicy | None` and `from_toml` builds
    one instead of passing a mapping through. The mapping form is frozen (see
    `_freeze`) so the intermediate state cannot be mutated mid-request either.
    """

    def __post_init__(self) -> None:
        object.__setattr__(self, "retry", _freeze(self.retry))

    def validate(self, catalog: Catalog) -> Workload:
        """Resolve every reference and reject the traps. Returns self.

        Called at snapshot construction, never on the request path. A model id
        that does not resolve is a `PolicyError` here, at reload, where the
        previous snapshot keeps serving -- rather than a `PolicyError` on the
        hot path, where it is a tenant's traffic.
        """
        if not self.id:
            raise PolicyError("workload id must not be empty")
        if not self.incumbent:
            raise PolicyError(f"workload {self.id!r}: incumbent is required")

        # `Catalog.resolve` already raises PolicyError for an unknown model;
        # re-raised with the workload name attached, because "unknown model
        # 'deepseek.v4'" without it sends the reader to the wrong file.
        incumbent = self._resolve(catalog, self.incumbent, "incumbent")
        if self.candidate is not None:
            candidate = self._resolve(catalog, self.candidate, "candidate")
            self._reject_identical(candidate, incumbent)
            self._reject_cross_dialect(candidate, incumbent)

        try:
            self.budgets.validate()
        except ValueError as exc:
            raise PolicyError(
                f"workload {self.id!r}: {exc}", model=self.incumbent, cause=exc
            ) from exc
        return self

    def _resolve(self, catalog: Catalog, model_id: str, role: str) -> Target:
        try:
            return catalog.resolve(model_id)
        except PolicyError as exc:
            raise PolicyError(
                f"workload {self.id!r}: {role} {model_id!r} is not in the catalog",
                model=model_id, cause=exc,
            ) from exc

    def _reject_cross_dialect(self, candidate: Target, incumbent: Target) -> None:
        """Both targets must speak the same wire protocol.

        A gateway attempt reuses ONE request: the client's original bytes, one
        path, one response parser, one surface's terminal marker. All of those
        are chosen per *execution*, while targets are chosen per *attempt*. So
        a plan pairing an `anthropic`-kind target with an `openai`-kind one
        does not fall back -- it POSTs an OpenAI chat-completions body to
        `api.anthropic.com/v1/messages`, and whatever comes back is parsed by
        a surface that was chosen for the other dialect.

        The shipped example config contained exactly this pairing (an
        Anthropic incumbent behind a DeepSeek candidate) and every test passed,
        because the fake upstreams answer whatever they are asked. It would
        have failed the first time a real fallback fired -- which is to say,
        during an incident, on the path that exists to survive one.

        Rejected at construction rather than translated, because translation
        is a feature with its own contract (a "semantic adapter",
        where byte-for-byte passthrough stops being true).
        Until that exists, a cross-dialect plan is a config error, and the
        honest place to fail is a reload that nobody's traffic depends on.
        """
        if candidate.provider.kind != incumbent.provider.kind:
            raise PolicyError(
                f"workload {self.id!r}: candidate {self.candidate!r} speaks "
                f"{candidate.provider.kind!r} but incumbent {self.incumbent!r} "
                f"speaks {incumbent.provider.kind!r}. One request cannot be sent "
                f"to both: the body schema, the path and the terminal marker all "
                f"differ. Pair targets of the same kind, or add a translating "
                f"surface first.",
                workload=self.id,
            )

    def _reject_identical(self, candidate: Target, incumbent: Target) -> None:
        """A "fallback" to the same place is a retry wearing a costume.

        It looks like redundancy in the config and behaves like a doubling of
        load: the same request goes to the same provider, the same wire model
        id, the same account, twice. Every failure that is deterministic at
        that endpoint -- a 400 the schema will always produce, a context
        overflow, a content filter -- fails twice, costs twice, and takes
        twice the budget to conclude what one attempt already knew.

        Worse, it hides. The dashboard shows two targets and a fallback rate,
        which reads as resilience right up until the day the target is down
        and you discover the second column was the first one again.

        If you genuinely want a second attempt at one target, that is
        `retry.RetryPolicy` -- which has backoff, jitter, and a budget check,
        none of which a duplicated plan entry gives you.

        The comparison is on the *wire destination* (provider id + the model
        id the provider's own API sees), not on our catalog key. Two catalog
        entries can be aliases for one endpoint, and the alias is the version
        of this mistake that survives review.
        """
        if candidate == incumbent:
            raise PolicyError(
                f"workload {self.id!r}: candidate and incumbent are both "
                f"{self.incumbent!r}; a fallback to the identical target is a "
                "retry, not a fallback -- it doubles load while looking like "
                "redundancy. Use a retry policy, or drop the candidate."
            )
        c_wire = (candidate.provider.id, candidate.model.api_model)
        i_wire = (incumbent.provider.id, incumbent.model.api_model)
        if c_wire == i_wire:
            raise PolicyError(
                f"workload {self.id!r}: candidate {self.candidate!r} and "
                f"incumbent {self.incumbent!r} are different catalog ids for the "
                f"same upstream endpoint {c_wire[0]}/{c_wire[1]}; that is a "
                "retry wearing a costume, not a fallback"
            )

    def _canonical(self) -> dict[str, Any]:
        """The part of this workload that changes what the gateway does.

        `id` is excluded because it is the key it is stored under, and
        including it would hash the same fact twice.
        """
        return {
            "incumbent": self.incumbent,
            "candidate": self.candidate,
            "budgets": dataclasses.asdict(self.budgets),
            "retry": _plain(self.retry),
        }


# ==========================================================================
# ExecutionPlan
# ==========================================================================


@dataclass(frozen=True, slots=True)
class ExecutionPlan:
    """Everything layer 5 needs to run one request, and nothing it can change.

    Handed to the attempt loop, which walks `targets` in order. It carries
    `policy_id` and `workload_id` rather than leaving the executor to look
    them up, so the identity written into the capture record is the identity
    that produced the plan -- the same object, not a second lookup that could
    land on a different snapshot.
    """

    policy_id: str
    workload_id: str
    targets: tuple[Target, ...]
    """ORDERED: candidate first, then incumbent. Never more than two.

    A third target is not an oversight. Each extra entry multiplies the
    worst-case latency and the worst-case spend of a failing request, and the
    marginal availability of a third provider is small next to the marginal
    confusion of a plan nobody can predict the cost of. Two is a decision:
    the one you are testing, and the one you trust.
    """

    budgets: Budgets
    retry: object | None
    """See `Workload.retry` -- loosely typed while `retry.py` is in flight."""

    def __len__(self) -> int:
        return len(self.targets)

    @property
    def primary(self) -> Target:
        """The target that will actually be tried first. Validation guarantees
        at least one, so this cannot raise on a constructed plan."""
        return self.targets[0]

    def __str__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"{self.workload_id}@{self.policy_id}["
            + " -> ".join(str(t) for t in self.targets)
            + "]"
        )


# ==========================================================================
# PolicySnapshot
# ==========================================================================


@dataclass(frozen=True, slots=True)
class PolicySnapshot:
    """An immutable, content-addressed view of policy, pinned for one request.

    Take it once at ingress with `PolicyStore.current()`, carry the same
    object to the accounting record. Nothing in a request's lifetime can
    observe a change, because there is nothing here that changes: the
    workloads are a `MappingProxyType` of frozen dataclasses, the budgets are
    frozen, and parsed retry config is frozen on the way in.
    """

    id: str
    """`pol_` + 8 hex of the content digest. See the module docstring for why
    this is a hash and not a counter."""

    created_at: float
    """MONOTONIC, from the clock this snapshot was built with.

    Monotonic and not wall clock for the reason `clocks.SystemClock` gives:
    NTP steps, VMs pause, and an age computed from wall clock can come out
    negative. The consequence is that `created_at` is meaningful only *inside
    this process* -- it is a duration origin, not a timestamp, and it must
    never be logged as if it were one. Cross-process identity is `id`'s job,
    and that is exactly the division of labour: content addresses identity,
    monotonic time addresses age.
    """

    workloads: Mapping[str, Workload]
    catalog: Catalog
    default_workload: str

    content_digest: str = ""
    """The full hex digest `id` is a prefix of. Present so that a collision in
    the short form is diagnosable rather than merely deniable."""

    def __post_init__(self) -> None:
        """Freeze, then validate. In that order, so that a snapshot which
        raises never leaves a half-usable object behind for anyone holding a
        reference to it."""
        object.__setattr__(self, "workloads", MappingProxyType(dict(self.workloads)))
        if not self.workloads:
            raise PolicyError("a policy snapshot needs at least one workload")
        for wid, workload in self.workloads.items():
            if wid != workload.id:
                raise PolicyError(
                    f"workload key {wid!r} does not match its id {workload.id!r}"
                )
            workload.validate(self.catalog)
        if self.default_workload not in self.workloads:
            raise PolicyError(
                f"default_workload {self.default_workload!r} is not a defined "
                f"workload; known: {sorted(self.workloads)}"
            )

    # ------------------------------------------------------------ planning

    def plan_for(
        self,
        workload_id: str | None = None,
        *,
        model: str | None = None,
        kind: str | None = None,
    ) -> ExecutionPlan:
        """The ordered plan for one request. Pure lookup; cannot fail on config.

        `workload_id=None` means the default workload -- the common case of a
        client that names no workload at all, which must route somewhere
        rather than 400.

        `model=` is the explicit-model path: a client that names a model gets
        that model and only that model. It replaces the plan rather than
        prepending to it, which is the surprising half and the correct one.
        Prepending would silently fall back to a workload's incumbent that the
        caller never asked for and may not be authorised for; and a caller who
        pinned `anthropic.sonnet-4-6` because they are comparing two systems
        does not want a quietly-substituted answer from something else. It is
        still resolved through the catalog, so an unknown model is a
        `PolicyError` and not a request to a provider that has never heard of
        it.

        `model` may also be a wire id or a declared alias (`Catalog.resolve`):
        the id a provider echoed in its last response is what an SDK sends on
        the next turn, and turn two must not 400. `kind=` is the dialect of
        the route the request arrived on (`"openai"` / `"anthropic"`); it is
        used only to break a tie when two providers of different dialects
        share a wire id, since only the same-dialect one could serve the body.
        """
        wid = workload_id if workload_id is not None else self.default_workload
        workload = self.workloads.get(wid)
        if workload is None:
            raise PolicyError(
                f"unknown workload {wid!r}; known: {sorted(self.workloads)}"
            )

        if model is not None:
            targets: tuple[Target, ...] = (
                self.catalog.resolve(model, kind=kind),  # type: ignore[arg-type]
            )
        else:
            targets = tuple(
                self.catalog.resolve(mid)
                for mid in (workload.candidate, workload.incumbent)
                if mid is not None
            )
        return ExecutionPlan(
            policy_id=self.id,
            workload_id=wid,
            targets=targets,
            budgets=workload.budgets,
            retry=workload.retry,
        )

    # ------------------------------------------------------------------ age

    def age(self, clock: Clock) -> float:
        """Seconds since this snapshot was built, on the same clock family.

        Row 10's residual risk is that snapshot age is *unbounded*: a 40-minute
        stream that pinned a snapshot at minute zero is still routing and
        pricing on 40-minute-old policy when it finishes. Two things that
        actually costs you:

          * an authorization change that has not landed. A model revoked for a
            tenant is still reachable by every request holding an older
            snapshot, for as long as that request runs.
          * a price change that has not landed. The record is *self-consistent*
            -- it names the policy it used -- but it is not *current*, and a
            reconciliation against the provider's invoice will show the gap.

        This method exposes the number. It deliberately does not act on it.
        Bounding age means choosing what to do at the bound, and both choices
        are bad in different directions: refusing a request because policy is
        stale turns a config-plumbing hiccup into an outage, while re-pinning
        mid-request re-introduces the exact split-brain the snapshot exists to
        prevent. The honest shape is to make the number visible -- emit it,
        alert on it, refuse to *start* new requests past a bound if you like --
        and to leave the decision with the operator who knows which of those
        two failures their business can absorb.
        """
        return clock.now() - self.created_at

    # ----------------------------------------------------------- identity

    @property
    def catalog_id(self) -> str:
        """A content hash of the CATALOG this snapshot resolves against.

        Separate from `id`, and that separation is the point. `id` names the
        policy *document* -- who routes where. Prices, api model ids and base
        URLs live in the catalog, and folding them into `id` would churn the
        policy id every time an unrelated model's price was re-verified,
        destroying its value as a join key across a fleet mid-rollout.

        But row 10 is about billing as much as routing, and a record that
        names only the policy cannot say which price table applied. So both
        ids are pinned in the same snapshot and both belong in the capture
        record. Two narrow ids beat one that is wrong at both jobs.
        """
        return _hash_id(_canonical_catalog(self.catalog), prefix=_CATALOG_ID_PREFIX)

    # ----------------------------------------------------------- builders

    @classmethod
    def from_toml(
        cls, text: str, *, catalog: Catalog, clock: Clock | None = None
    ) -> PolicySnapshot:
        """Parse a policy file. Every failure is a `PolicyError` naming the key.

        The schema is in `config/workloads.example.toml`. Shape:

            default_workload = "chat"

            [defaults.budgets]        # inherited by every workload
            total = 600.0
            first_event = 20.0

            [defaults.retry]          # likewise
            max_attempts = 2

            [workloads.chat]
            incumbent = "anthropic.sonnet-4-6"
            candidate = "deepseek.deepseek-v4-pro"
            [workloads.chat.budgets]  # overrides ONE key; the rest inherit
            first_event = 45.0

        Unknown keys are rejected rather than ignored. That is the one schema
        decision worth defending: a config parser that ignores what it does
        not recognise turns `candidat = "..."` into a silently disabled A/B
        test that reports 100% incumbent traffic and looks exactly like a
        candidate nobody sends traffic to. A typo must be an error at reload,
        where it is one line, not a metric anomaly a week later.

        `tomllib.TOMLDecodeError` and `KeyError` never escape. A reload has to
        be a classifiable event with a `code`; a bare parser exception from
        three frames down is neither countable nor actionable.
        """
        try:
            raw = tomllib.loads(text)
        except tomllib.TOMLDecodeError as exc:
            raise PolicyError(f"policy config is not valid TOML: {exc}", cause=exc) from exc

        _reject_unknown(raw, _TOP_LEVEL_KEYS, "top level")
        defaults = _table(raw, "defaults", default={})
        _reject_unknown(defaults, frozenset({"budgets", "retry"}), "[defaults]")
        base_budgets = _budgets_from(
            _table(defaults, "budgets", default={}), DEFAULT_BUDGETS, "defaults"
        )
        base_retry = _table(defaults, "retry", default=None)

        raw_workloads = _table(raw, "workloads", default=None)
        if not raw_workloads:
            raise PolicyError(
                "policy config defines no [workloads.*]; a snapshot with no "
                "workloads can route nothing"
            )

        workloads: dict[str, Workload] = {}
        for wid, body in raw_workloads.items():
            if not isinstance(body, dict):
                raise PolicyError(
                    f"[workloads.{wid}] must be a table, got {type(body).__name__}"
                )
            _reject_unknown(body, _WORKLOAD_KEYS, f"[workloads.{wid}]")
            if "incumbent" not in body:
                raise PolicyError(
                    f"[workloads.{wid}] is missing 'incumbent'; it is required "
                    "because it is the target every fallback ends at"
                )
            overrides = _table(body, "budgets", default={})
            retry = body.get("retry", base_retry)
            workloads[wid] = Workload(
                id=wid,
                incumbent=_string(body, "incumbent", f"[workloads.{wid}]"),
                candidate=(
                    _string(body, "candidate", f"[workloads.{wid}]")
                    if body.get("candidate") is not None
                    else None
                ),
                budgets=_budgets_from(overrides, base_budgets, f"workloads.{wid}"),
                retry=retry,
            )

        default_workload = raw.get("default_workload")
        if not isinstance(default_workload, str) or not default_workload:
            raise PolicyError(
                "policy config is missing a top-level string 'default_workload'; "
                "a request that names no workload has to route somewhere, and "
                "guessing 'the first one in the file' makes routing depend on "
                f"key order. Known workloads: {sorted(workloads)}"
            )
        return cls._build(workloads, default_workload, catalog=catalog, clock=clock)

    @classmethod
    def single_target(
        cls,
        model: str,
        *,
        catalog: Catalog,
        budgets: Budgets | None = None,
        clock: Clock | None = None,
        workload_id: str = "default",
    ) -> PolicySnapshot:
        """A one-workload, one-target snapshot. No config file required.

        This is the adoption path. `server/app.py` today routes with
        `config.default_model` and a static `policy_id="p2-static"` string,
        which is a policy layer written as two unrelated fields. It can move
        onto `PolicySnapshot` in one line and get a real content-addressed id,
        a pinned snapshot, and validated budgets -- before anyone has written
        a `workloads.toml`. A migration that requires a config file to exist
        is a migration that does not happen.
        """
        workload = Workload(id=workload_id, incumbent=model,
                            budgets=budgets or DEFAULT_BUDGETS)
        return cls._build({workload_id: workload}, workload_id,
                          catalog=catalog, clock=clock)

    @classmethod
    def _build(
        cls,
        workloads: Mapping[str, Workload],
        default_workload: str,
        *,
        catalog: Catalog,
        clock: Clock | None,
    ) -> PolicySnapshot:
        digest = _digest(_canonical_policy(default_workload, workloads))
        clock = clock or SystemClock()
        return cls(
            id=_ID_PREFIX + digest[:_ID_CHARS],
            created_at=clock.now(),
            workloads=workloads,
            catalog=catalog,
            default_workload=default_workload,
            content_digest=digest,
        )


# ==========================================================================
# PolicyStore
# ==========================================================================


class PolicyStore:
    """Holds the current snapshot. Hot-reloadable; one snapshot per request.

    The whole class is a box around one attribute, and the discipline is in
    what it does *not* offer: there is no way to edit a snapshot in place, so
    a reload can only ever be a whole-value swap. `replace()` rebinds the
    attribute -- a single bytecode store under the GIL, with no window in
    which a reader sees a torn value -- and every request that already called
    `current()` finishes on the object it took. No lock, because there is no
    critical section: nothing is read-modify-written.

    That is the mitigation for FAILURE-MODES row 10 stated as code. The
    property is not "we are careful to re-read config at consistent points";
    it is "there is no second read".

    Correct use is one `current()` per request, at ingress, threaded through.
    Calling `current()` twice in one request and using both results reopens
    exactly the split-brain this exists to close -- which is why
    `ExecutionPlan` carries `policy_id` rather than letting the accounting
    layer look it up again.

    --------------------------------------------------------------------
    `validate`: the swap is refused before it happens, not after
    --------------------------------------------------------------------

    A snapshot that was constructed is a snapshot that routes -- but routing
    is not everything a request needs from it. The server derives values the
    policy layer deliberately does not know how to build (a `RetryPolicy`
    from a `[retry]` table; this module does not import `retry.py`), and
    that derivation is FALLIBLE: `max_delay` below `base_delay` is a config
    error that `PolicySnapshot` cannot see. Without a check here, a reload of
    such a file succeeds, `current()` starts handing it out, and every
    subsequent request discovers the problem as a 500 on the hot path --
    which is precisely the "bad config that raises on the request path takes
    the traffic with it" this module's docstring argues against.

    So the store takes the server's derivation as a validator and runs it on
    the incoming snapshot BEFORE the rebind. A failure is a `PolicyError`
    raised from `replace()`, `current()` is untouched, and the previous
    snapshot keeps serving. That is the same argument as construction-time
    validation, one layer up: the honest place for a config error is the
    reload that nobody's traffic depends on.
    """

    __slots__ = ("_snapshot", "_validate")

    def __init__(
        self,
        snapshot: PolicySnapshot,
        *,
        validate: Callable[[PolicySnapshot], object] | None = None,
    ) -> None:
        self._validate = validate
        self._check(snapshot)
        self._snapshot = snapshot

    def current(self) -> PolicySnapshot:
        """The snapshot for one request. Take it once, at ingress."""
        return self._snapshot

    def replace(self, snapshot: PolicySnapshot) -> PolicySnapshot:
        """Validate, then swap in a new snapshot and return the OLD one.

        Returning the previous value rather than None is not a convenience:
        it is what lets a reload log `pol_1a2b3c4d -> pol_9f8e7d6c` in one
        line, and what lets a caller notice that a reload was a no-op because
        the content hash did not move. A reload that changed nothing and a
        reload that changed everything look identical from the outside
        otherwise.

        The validator runs first, on the incoming snapshot only. If it raises,
        nothing here has changed: `current()` still returns the snapshot it
        returned a moment ago, and the caller gets a `PolicyError` naming the
        reason. The rebind is still a single attribute store, so the property
        the class docstring promises -- no torn reads, no lock -- is intact;
        validation happens before the store, not around it.

        In-flight requests are unaffected by construction -- they hold the old
        object, and it is frozen, so there is nothing for them to observe.
        """
        self._check(snapshot)
        previous = self._snapshot
        self._snapshot = snapshot
        return previous

    def _check(self, snapshot: PolicySnapshot) -> None:
        """Run the validator, and make every refusal a `PolicyError`.

        A validator that raises `PolicyError` is passed through as-is. One
        that raises anything else is wrapped, cause chained, for the reason
        `from_toml` gives about `TOMLDecodeError`: a refused reload has to be
        a classifiable event with a `code`, not a stray builtin from three
        frames down. The chained cause keeps the real message.
        """
        if self._validate is None:
            return
        try:
            self._validate(snapshot)
        except PolicyError:
            raise
        except Exception as exc:  # noqa: BLE001 - see docstring
            raise PolicyError(
                f"policy snapshot {snapshot.id} was refused by the store's "
                f"validator: {exc}",
                cause=exc,
            ) from exc

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<PolicyStore {self._snapshot.id}>"


# ==========================================================================
# TOML helpers. Small, and every one of them exists to convert a builtin
# exception into a PolicyError that names the key.
# ==========================================================================


def _reject_unknown(table: Mapping[str, Any], allowed: frozenset[str], where: str) -> None:
    unknown = sorted(set(table) - allowed)
    if unknown:
        raise PolicyError(
            f"{where}: unknown key(s) {unknown}; allowed: {sorted(allowed)}. "
            "Unknown keys are refused rather than ignored -- an ignored typo is "
            "a setting that silently is not applied"
        )


def _table(source: Mapping[str, Any], key: str, *, default: Any) -> Any:
    value = source.get(key, default)
    if value is None:
        return None
    if not isinstance(value, dict):
        raise PolicyError(f"{key!r} must be a table, got {type(value).__name__}")
    return value


def _string(source: Mapping[str, Any], key: str, where: str) -> str:
    value = source.get(key)
    if not isinstance(value, str) or not value:
        raise PolicyError(f"{where}: {key!r} must be a non-empty string, got {value!r}")
    return value


def _budgets_from(
    overrides: Mapping[str, Any], base: Budgets, where: str
) -> Budgets:
    """Apply per-workload budget overrides onto an inherited base.

    Inheritance rather than replacement: a workload that tightens
    `first_event` keeps the total, the connect budget and the client-stall
    budget it never mentioned. The alternative -- a budgets table means "these
    are ALL my budgets" -- makes every workload restate five numbers, and a
    config where five numbers are copied five times is a config where they
    disagree within a quarter.
    """
    _reject_unknown(overrides, _BUDGET_KEYS, f"[{where}.budgets]")
    values: dict[str, Any] = {}
    for key, value in overrides.items():
        if key == "liveness" and value is None:
            values[key] = None
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise PolicyError(
                f"[{where}.budgets] {key}={value!r} must be a number of seconds"
            )
        values[key] = float(value)
    try:
        return dataclasses.replace(base, **values)
    except TypeError as exc:  # pragma: no cover - _reject_unknown covers it
        raise PolicyError(f"[{where}.budgets]: {exc}", cause=exc) from exc


# ==========================================================================
# Canonicalisation and hashing
# ==========================================================================


def _digest(canonical: str) -> str:
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _hash_id(canonical: str, *, prefix: str) -> str:
    return prefix + _digest(canonical)[:_ID_CHARS]


def _dump(doc: Any) -> str:
    """One canonical serialisation, used by both hashes.

    `sort_keys=True` is the whole trick: it is what makes the id independent
    of the order keys appeared in the TOML, which is what "canonicalised" has
    to mean for the id to be stable across two humans editing the same file.
    `ensure_ascii=True` keeps the bytes identical regardless of the writer's
    locale; compact separators keep the string short for no reason other than
    that it occasionally ends up in a debug log.
    """
    return json.dumps(doc, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _canonical_policy(
    default_workload: str, workloads: Mapping[str, Workload]
) -> str:
    """The policy's meaning, as a string.

    Hashes the RESOLVED workloads -- budgets after inheritance, retry after
    defaults -- rather than the raw file. Two files that produce identical
    routing therefore produce identical ids even if one spells out what the
    other inherits, which is the property that makes the id survive a config
    refactor. It also means comments, whitespace and key order do not move it.
    """
    return _dump({
        "default_workload": default_workload,
        "workloads": {wid: w._canonical() for wid, w in workloads.items()},
    })


def _canonical_catalog(catalog: Catalog) -> str:
    """The catalog's meaning: everything that changes where a call goes or
    what it costs. `stale_prices()` thresholds and other derived views are
    excluded because they are computed from these fields, not inputs to them."""
    return _dump({
        "models": {
            mid: {
                "provider": m.provider,
                "api_model": m.api_model,
                "input_per_m": m.input_per_m,
                "output_per_m": m.output_per_m,
                "cached_input_per_m": m.cached_input_per_m,
                "cache_write_per_m": m.cache_write_per_m,
                "context_window": m.context_window,
                "max_output": m.max_output,
                "priced_at": m.priced_at,
                "aliases": list(m.aliases),
            }
            for mid, m in catalog.models.items()
        },
        "providers": {
            pid: {
                "kind": p.kind,
                "base_url": p.base_url,
                "api_key_env": p.api_key_env,
                "credential_id": p.key(),
                "max_concurrency": p.max_concurrency,
                "extra_headers": dict(p.extra_headers),
                "extra_body": _plain(p.extra_body),
            }
            for pid, p in catalog.providers.items()
        },
    })


__all__ = [
    "DEFAULT_BUDGETS",
    "ExecutionPlan",
    "PolicySnapshot",
    "PolicyStore",
    "Workload",
]
