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
