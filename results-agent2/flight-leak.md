# Flight SQL ticket lifecycle and Connect interrupts

Command `/opt/hostedtoolcache/Python/3.11.18/x64/bin/python -m harness.agent2.flight_leak`.

Workflow source revision `5f437acf046a9f04413f9d105fd0ff7935e79e23`. Sail image revision `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`.

Recorded versions include spark 3.5.10, iceberg 1.12.0, comet 1.2.0, python 3.11.18, adbc 1.13.0.

The raw JSON records engine settings, runner details and individual attempts.

Flight SQL uses a 120-second session timeout. The synthetic query returns 20,000 rows. A fetched control was compared with Spark before issuing the unfetched tickets.

Issued 300 tickets, of which 300 had distinct ticket bytes. The initial and fresh controls matched Spark `True` and `—`. The late fetch status was `ok`.

Memory rose from 68.0 MiB before issuance to 412.5 MiB at the first
ten-second sample and remained at 412.3 MiB around six minutes. The
oldest ticket still returned the 20,000 rows matching Spark. A fresh
ticket request failed with `session flight-default is not running`.
The pinned server stores query streams in a service-wide map that
`DoGet` removes by ticket. This run tested one late fetch, and memory
sampling measures all container allocations.
[Flight query state](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-flight/src/state.rs#L42)

| Seconds from issuance start | Issuance complete | Cgroup MiB |
| --- | --- | --- |
| 0.0 | False | 68.0 |
| 10.0 | True | 412.5 |
| 20.0 | True | 412.3 |
| 30.0 | True | 412.3 |
| 40.0 | True | 412.3 |
| 50.0 | True | 412.3 |
| 60.0 | True | 412.3 |
| 70.0 | True | 412.3 |
| 80.0 | True | 412.3 |
| 90.0 | True | 412.3 |
| 100.0 | True | 412.3 |
| 110.0 | True | 412.3 |
| 120.0 | True | 412.3 |
| 130.0 | True | 412.6 |
| 140.0 | True | 412.6 |
| 150.0 | True | 412.3 |
| 160.0 | True | 412.3 |
| 170.0 | True | 412.3 |
| 180.0 | True | 412.3 |
| 190.0 | True | 412.6 |
| 200.0 | True | 412.3 |
| 210.0 | True | 412.3 |
| 220.0 | True | 412.3 |
| 230.0 | True | 412.3 |
| 240.0 | True | 412.3 |
| 250.0 | True | 412.3 |
| 260.0 | True | 412.6 |
| 270.0 | True | 412.6 |
| 280.0 | True | 412.6 |
| 290.0 | True | 412.6 |
| 300.0 | True | 412.3 |
| 310.0 | True | 412.3 |
| 320.0 | True | 412.6 |
| 330.0 | True | 412.6 |
| 340.0 | True | 412.3 |
| 350.0 | True | 412.3 |
| 360.0 | True | 412.3 |

| Protocol | Query active | Cancel RPC | Idle CPU s/10 s | Before CPU s/5 s | After CPU s/10 s | Later CPU s/10 s | Client active at last sample |
| --- | --- | --- | --- | --- | --- | --- | --- |
| flight_cancel | — | — | — | — | — | — | — |
| connect_interrupt | True | timeout | 0.0 | 19.682 | 38.714 | 39.247 | True |

Cgroup memory contains all server allocations and cache. Ticket expiry and memory returning to baseline address different aspects of result cleanup. The test observes one six-minute window.
## Anomalies and log excerpts

```text
TimeoutError: deadline of 10s exceeded (client still active: True)
sail-flight | FlightInternalError: Flight returned internal error, with message: session error: invalid argument: session flight-default is not running
```

[Raw JSON](https://github.com/unikdahal/poc/blob/3b375885d9239626d31e90b83e0e39d81b38a434/agent2-flight-leak/flight-leak.json) · [Result files](https://github.com/unikdahal/poc/tree/3b375885d9239626d31e90b83e0e39d81b38a434/agent2-flight-leak) · [Workflow run](https://github.com/unikdahal/poc/actions/runs/45492104693)
