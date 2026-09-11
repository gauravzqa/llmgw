"""A significant input for the live-overhead bench: a real ~6 KB technical
document the model is asked to summarise. Kept in its own module so the bench
file stays readable and so the byte/approx-token size is easy to assert."""

DOCUMENT = """\
Congestion control on the Internet is the discipline of matching the rate at
which a sender injects packets into the network to the rate the network can
actually carry, without any single sender being told that rate in advance.
No router hands the endpoints a number. The path's capacity is a moving target
set by every other flow sharing the same queues, by the physical links, and by
cross traffic that appears and vanishes on millisecond timescales. TCP's answer,
refined across four decades, is to treat loss and delay as the only available
signals and to probe continuously: increase the sending rate until the network
pushes back, then back off, then probe again. The genius and the frustration of
the design both live in that single idea.

The first widely deployed algorithm, Tahoe, introduced slow start and
congestion avoidance. Slow start is a misnomer: a connection opens by doubling
its congestion window every round trip, which is exponential growth, the fastest
ramp the protocol ever uses. The point of starting small is not caution for its
own sake but ignorance — a new flow knows nothing about the path, so it spends a
few round trips learning the rough scale of the available capacity before it can
do anything smarter. When a loss is detected, Tahoe collapses the window to one
segment and slow-starts again. Reno improved on this with fast recovery: a single
loss, signalled by three duplicate acknowledgements rather than a timeout, halves
the window instead of resetting it, on the theory that one lost packet in an
otherwise healthy stream is a mild signal, not a catastrophe. This additive-
increase/multiplicative-decrease rule — grow by a constant each round trip, cut
by half on loss — is the behaviour most people mean when they say "TCP backs off."

AIMD has a property that is easy to state and surprisingly deep: among flows
sharing a bottleneck, it converges toward a fair and efficient allocation.
Chiu and Jain's analysis draws it as a trajectory in a plane whose axes are the
two flows' sending rates. Additive increase moves the system along a 45-degree
line toward higher utilisation; multiplicative decrease moves it back along a
line through the origin. The combination spirals in on the fair-share point
where both flows send equally and the link is fully used. Crucially, no other
linear control law has both properties. Multiplicative increase overshoots and
oscillates; additive decrease fails to converge to fairness. The specific pairing
TCP uses is not an accident of engineering taste — it is the control law the
mathematics singles out.

The trouble is that AIMD was tuned for a network that no longer exists. On a
path with a large bandwidth-delay product — a fast link with a long round trip,
say a transcontinental or satellite hop — the additive increase of one segment
per round trip is agonisingly slow to fill the pipe, and the multiplicative halving
on a single loss is brutally large. A flow can spend minutes crawling back to the
rate it gave up in one round trip. Worse, losses on modern networks are frequently
not congestion signals at all: a wireless link drops a packet to interference, and
classic TCP reads that as a full bottleneck and halves its rate, punishing the flow
for the medium's noise rather than for any real congestion.

A second problem is bufferbloat. For years the instinct of equipment vendors was
that memory is cheap, so a bigger buffer must be better — it absorbs bursts and
prevents loss. But a loss-based congestion controller only learns to slow down
when a buffer overflows. Put a very large buffer in front of the bottleneck and the
controller keeps filling it, because nothing has been dropped yet, and the buffer
sits persistently full. A full buffer is pure latency: every packet now waits behind
a queue that never drains. The counterintuitive result is that oversized buffers,
deployed to improve the network, instead produce seconds of delay on exactly the
interactive traffic — voice, video, gaming, typing in a terminal — that latency
hurts most. The fix required rethinking both queue management in the routers, with
schemes like CoDel that measure how long packets dwell rather than how many are
queued, and congestion control at the endpoints.

BBR, developed at Google, is the most prominent attempt to escape the loss-equals-
congestion assumption. Instead of treating a dropped packet as the signal, BBR
builds an explicit model of the path: it continuously estimates the bottleneck
bandwidth and the minimum round-trip time, and it paces its sending rate to sit
right at the product of the two — the point where the pipe is full but no standing
queue has formed. This is a fundamentally different philosophy. AIMD reacts to
congestion after it happens; BBR tries to operate at the knee of the curve and
never cause it. In practice BBR delivers dramatically higher throughput on lossy
long-haul paths and far lower latency through bloated buffers, which is why it now
carries a large fraction of the traffic leaving Google's network. It is not without
controversy: early versions could be unfair to loss-based flows sharing the same
link, claiming more than their share of a bottleneck, and tuning the coexistence
has been an active area of work ever since.

The through-line across all of this is that congestion control is a distributed
agreement reached with almost no communication. Millions of independent senders,
each acting only on the faint evidence of its own acknowledgements and timers,
collectively arrive at an allocation of a shared resource none of them can see.
That it works at all — that the Internet does not routinely collapse under its own
load, as it briefly did in 1986 before Van Jacobson's fixes — is one of the quiet
triumphs of systems engineering, and every refinement since has been an argument
about what signal to trust and how hard to react when you see it.
"""
