"""Tier 3: randomized fault injection.

This tier asserts *invariants*, never cases. A case test says "mode X produces
error Y"; an invariant test says "whatever happened, nothing leaked". The two
find different bugs, and only the second one finds the bug nobody thought to
write a case for -- which, for a gateway, is every resource leak that shows up
as capacity ratcheting to zero over a week.
"""
