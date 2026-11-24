# Flight SQL ticket lifecycle and Connect interrupts

Command `/opt/hostedtoolcache/Python/3.11.18/x64/bin/python -m harness.agent2.flight_leak`.

Workflow source revision `ce63ff3b49ef24444b790dd4a2d64a017b0ca5fa`. Sail image revision `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`.

Recorded versions include spark 3.5.10, iceberg 1.12.0, comet 1.2.0, python 3.11.18, adbc 1.13.0.

The raw JSON records engine settings, runner details and individual attempts.

Flight SQL uses a 120-second session timeout. The synthetic query returns 20,000 rows. A fetched control was compared with Spark before issuing the unfetched tickets.

Issued 300 tickets, of which 300 had distinct ticket bytes. The initial and fresh controls matched Spark `True` and `—`. The late fetch status was `ok`.

| Seconds from issuance start | Issuance complete | Cgroup MiB |
| --- | --- | --- |
| 0.0 | False | 63.6 |
| 10.0 | True | 416.3 |
| 20.0 | True | 416.3 |
| 30.0 | True | 416.3 |
| 40.0 | True | 416.3 |
| 50.0 | True | 416.3 |
| 60.0 | True | 416.3 |
| 70.0 | True | 416.5 |
| 80.0 | True | 416.5 |
| 90.0 | True | 416.5 |
| 100.0 | True | 416.5 |
| 110.0 | True | 416.3 |
| 120.0 | True | 416.3 |
| 130.0 | True | 416.5 |
| 140.0 | True | 416.3 |
| 150.0 | True | 416.3 |
| 160.0 | True | 416.3 |
| 170.0 | True | 416.3 |
| 180.0 | True | 416.3 |
| 190.0 | True | 416.5 |
| 200.0 | True | 416.5 |
| 210.0 | True | 416.5 |
| 220.0 | True | 416.5 |
| 230.0 | True | 416.5 |
| 240.0 | True | 416.5 |
| 250.0 | True | 416.3 |
| 260.0 | True | 416.5 |
| 270.0 | True | 416.5 |
| 280.0 | True | 416.3 |
| 290.0 | True | 416.3 |
| 300.0 | True | 416.3 |
| 310.0 | True | 416.3 |
| 320.0 | True | 416.5 |
| 330.0 | True | 416.5 |
| 340.0 | True | 416.5 |
| 350.0 | True | 416.5 |
| 360.0 | True | 416.3 |

| Protocol | Query active | Cancel RPC | Idle CPU s/10 s | Before CPU s/5 s | After CPU s/10 s | Later CPU s/10 s | Client active at last sample |
| --- | --- | --- | --- | --- | --- | --- | --- |
| flight_cancel | True | ok | 0.0 | 19.461 | 32.201 | 39.004 | False |
| connect_interrupt | True | timeout | 0.0 | 19.475 | 37.405 | 37.273 | True |

Cgroup memory contains all server allocations and cache. Ticket expiry and memory returning to baseline address different aspects of result cleanup. The test observes one six-minute window.
## Anomalies and log excerpts

```text
OperationalError: CANCELLED: [FlightSQL] context canceled (Canceled; DoGet: endpoint 0: []). Vendor code: 1
TimeoutError: deadline of 10s exceeded (client still active: True)
FlightInternalError: Flight returned internal error, with message: session error: invalid argument: session flight-default is not running
```

[Raw JSON](https://github.com/unikdahal/poc/blob/5970f2afac93e2cbbaba88b74212c53c84c7cd07/agent2-flight-leak/flight-leak.json) · [Result files](https://github.com/unikdahal/poc/tree/5970f2afac93e2cbbaba88b74212c53c84c7cd07/agent2-flight-leak) · [Workflow run](https://github.com/unikdahal/poc/actions/runs/45494537059)
