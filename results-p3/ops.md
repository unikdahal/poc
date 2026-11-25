# Operator-visible diagnostics and lifecycle

Reproduce with `p3.yml`, `tasks=ops`. The suite command is `python -m harness.p3.ops` with surface, term, int and hup variants.
The pinned Sail commit is `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. The correctness reference is Spark 3.5.10 with Iceberg 1.12.0. Each raw file records the job revision, image digests, configuration and engine exit state where available.

**FACT.** The local image occupies 776702024 image bytes as reported by Docker. The checked warm median of fresh launches through the first correct `SELECT 1` was 1.301 s. [Raw JSON](https://github.com/unikdahal/poc/blob/f8ad1209e0432cda6dc48b313a8bdde16676b805/operator/result.json) · [Run 45554682627](https://github.com/unikdahal/poc/actions/runs/45554682627)

### Listener and SQL probes

| Probe | Status | Observed output or error excerpt |
| --- | --- | --- |
| spark EXPLAIN | ok | [['== Physical Plan ==\nAdaptiveSparkPlan isFinalPlan=false\n+- HashAggregate(keys=[_groupingexpression#16L], functions=[sum(id#13L)])\n   +- Exchange hashpartitioning(_groupingexp |
| spark EXPLAIN FORMATTED | ok | [['== Physical Plan ==\nAdaptiveSparkPlan (6)\n+- HashAggregate (5)\n   +- Exchange (4)\n      +- HashAggregate (3)\n         +- Project (2)\n            +- Range (1)\n\n\n(1) Rang |
| spark EXPLAIN ANALYZE | error | ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'SELECT'.(line 1, pos 16)  == SQL == EXPLAIN ANALYZE SELECT id % 2 AS g, sum(id) FROM range(1000) GROUP BY 1 --------- |
| flight-cleartext | ok | [[1]] |
| flight-invalid-bearer | ok | [[1]] |
| flight-tls | error | SSLError: [SSL: WRONG_VERSION_NUMBER] wrong version number (_ssl.c:1018) |
| signal-hup-0-subsequent-query | ok | [[1]] |

### OTLP and lifecycle

| Signal type | Requests | Records | Sample names / bodies |
| --- | --- | --- | --- |

The evaluator captures OTLP protobuf messages and records exporter transport in the raw configuration.

| Signal trial | Active when sent | Client state | Rows match | Container running | Exit code | OOMKilled |
| --- | --- | --- | --- | --- | --- | --- |
| signal-term-0 | True | ok | True | True | 0 | False |
| signal-term-1 | True | ok | True | True | 0 | False |
| signal-term-2 | True | ok | True | True | 0 | False |
| signal-int-0 | True | ok | True | False | 0 | False |
| signal-int-1 | True | ok | True | False | 0 | False |
| signal-int-2 | True | ok | True | True | 0 | False |
| signal-hup-0 | True | ok | True | True | 0 | False |

HUP signal evidence. [Raw JSON](https://github.com/unikdahal/poc/blob/f8ad1209e0432cda6dc48b313a8bdde16676b805/hup/result.json) · [Run 45558417190](https://github.com/unikdahal/poc/actions/runs/45558417190)

**FACT.** The HTTP exporter treatment exited with `HTTP protocol is not compatible with gRPC transport` before accepting a SQL request. Its configured transport is built with the gRPC builder. This behavior is tracked in [#2973](https://github.com/lakehq/sail/issues/2973). [Pinned exporter](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-telemetry/src/telemetry.rs#L126-L135) · [Raw JSON](https://github.com/unikdahal/poc/blob/f8ad1209e0432cda6dc48b313a8bdde16676b805/operator/result.json) · [Run 45554682627](https://github.com/unikdahal/poc/actions/runs/45554682627)

**FACT from code.** The server loads defaults and process environment at startup. The CLI's shutdown future for both protocols awaits SIGINT through Tokio. Spark Connect binds a plain TCP listener. The Flight SQL standard CLI creates the service without a per-user auth mapping. [Config](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-common/src/config/application.rs#L76-L84) · [Connect](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-cli/src/spark/server.rs#L85-L100) · [Flight](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-flight/src/entrypoint.rs#L25-L41)

Upstream SIGTERM handling is tracked in [#2611](https://github.com/lakehq/sail/issues/2611) and [#3098](https://github.com/lakehq/sail/pull/3098).

### Coverage limits

The listener checks cover the built-in servers of the pinned CLI. Gateway TLS termination and externally enforced authentication were outside these treatments. A SIGTERM outcome reports the container and client states directly. Config reload is assessed through SIGHUP and the startup config loader.
