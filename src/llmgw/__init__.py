"""llmgw -- a streaming LLM gateway.

Public surface is intentionally small. Everything else is an implementation
detail that the tests are allowed to reach into and consumers are not.
"""

from .admission import AdmissionController, Permit, ProviderKeyLimiter, TenantLimits
from .breaker import Breaker, BreakerPolicy, BreakerRegistry, BreakerState, Ticket
from .catalog import Catalog, ModelSpec, ProviderConn, Target
from .clocks import Budgets, Deadline, ManualClock, StallClock, SystemClock
from .errors import Blame, Disposition, GatewayError, Health, Outcome, decide
from .executor import AttemptRecord, ExecutionResult, Executor, Refusal
from .policy import ExecutionPlan, PolicySnapshot, PolicyStore, Workload
from .retry import RetryBudget, RetryPolicy

__version__ = "0.1.0"

__all__ = [
    "AdmissionController", "AttemptRecord", "Blame", "Breaker", "BreakerPolicy",
    "BreakerRegistry", "BreakerState", "Budgets", "Catalog", "Deadline",
    "Disposition", "ExecutionPlan", "ExecutionResult", "Executor",
    "GatewayError", "Health", "ManualClock", "ModelSpec", "Outcome", "Permit",
    "PolicySnapshot", "PolicyStore", "ProviderConn", "ProviderKeyLimiter",
    "Refusal", "RetryBudget", "RetryPolicy", "StallClock", "SystemClock",
    "Target", "TenantLimits", "Ticket", "Workload", "decide", "__version__",
]
