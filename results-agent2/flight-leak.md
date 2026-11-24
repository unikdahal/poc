# Flight SQL ticket lifecycle and Connect interrupts

Run `python -m harness.agent2.flight_leak` using the image at
`790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. The Flight SQL server runs with
`SAIL_FLIGHT__SESSION_TIMEOUT_SECS=120`. The suite issues 300 tickets for
the same synthetic 20,000-row result and samples cgroup memory every ten
seconds through six minutes after issuance. A late fetch and fresh control
test whether the ticket remains usable. Separate runs compare idle and
post-cancellation CPU windows for the same long query.

| Probe | Observations |
| --- | --- |
| Ticket issuance and expiry | Pending runner |
| Flight SQL cancel | Pending runner |
| Spark Connect interruptAll | Pending runner |

Memory samples, fetch outcomes, CPU windows and log excerpts are pending.
[Raw results](https://github.com/unikdahal/poc/tree/results/agent2-flight-leak/agent2-flight-leak)

The first Flight connection hit `Connection reset by peer` during startup.
In the separate Connect run, `interruptAll()` timed out at 10 seconds with
the query still running. The cgroup used 35.618 CPU-seconds in the first
10-second window after that deadline and 39.374 in the window one minute
later, compared with 0.001 during the idle control. The workload was still
active at the last sample. A rerun with a readiness probe is collecting the
ticket series and Flight cancellation control.
[Initial raw results](https://github.com/unikdahal/poc/blob/afe03a6b5d75e77f2358997215cfe23df39e3b3d/agent2-flight-leak/flight-leak.json)

Run 45492104693 ended without publishing its raw results. [Workflow run](https://github.com/unikdahal/poc/actions/runs/45492104693)
