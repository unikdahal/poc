"""Observe idle Flight SQL tickets and compare cancellation CPU windows."""

import math
import threading
import time

import adbc_driver_flightsql.dbapi as flightsql
import pyarrow.flight as flight

from harness.agent2.common import attempt, begin, bounded, collect, engine, error, fingerprint, save
from harness.suite_flight import any_cmd_statement_query

TASK = "flight-leak"
TICKET_SQL = "SELECT id, repeat(CAST(id AS STRING), 8) AS payload FROM range(20000)"
LONG_SQL = ("SELECT sum(pmod(a.id + b.id, 1000)) "
            "FROM range(0, 200000, 1, 8) a CROSS JOIN range(0, 200000, 1, 8) b")


def read_ticket(client, ticket, options):
    table = client.do_get(ticket, options).read_all()
    return fingerprint(zip(table["id"].to_pylist(), table["payload"].to_pylist()))


def ticket_lifecycle(result, client, stats, reference):
    options = flight.FlightCallOptions(timeout=15)
    desc = flight.FlightDescriptor.for_command(any_cmd_statement_query(TICKET_SQL))
    control = client.get_flight_info(desc, options).endpoints[0].ticket
    observation = result["unfetched"] = {
        "query": TICKET_SQL, "configured_timeout_s": 120, "requested_tickets": 300,
        "control_matches_spark": read_ticket(client, control, options) == reference,
        "sampling_interval_s": 10, "watch_after_last_issue_s": 360,
        "errors": [],
    }
    start = time.monotonic()
    issuance_done = threading.Event()
    samples = observation["memory"] = []

    def monitor():
        while True:
            samples.append({"elapsed_s": round(time.monotonic() - start, 3), "bytes": stats.mem_current(),
                            "issuance_done": issuance_done.is_set()})
            if stop_monitor.wait(10):
                return

    stop_monitor = threading.Event()
    watcher = threading.Thread(target=monitor, daemon=True)
    watcher.start()
    tickets = []
    try:
        for index in range(300):
            try:
                tickets.append(client.get_flight_info(desc, options).endpoints[0].ticket)
            except Exception as exc:
                observation["errors"].append({"index": index, "error": error(exc)})
                if isinstance(exc, (flight.FlightTimedOutError, flight.FlightUnavailableError)):
                    break
        observation["issued"] = len(tickets)
        observation["issuance_finished_elapsed_s"] = round(time.monotonic() - start, 3)
        issuance_done.set()
        for _ in range(36):
            time.sleep(10)
            save(TASK, result)
        if tickets:
            record, late = attempt(lambda: read_ticket(client, tickets[0], options))
            observation["late_fetch"] = record | {"matches_spark": late == reference if late else None}
        fresh = client.get_flight_info(desc, options).endpoints[0].ticket
        observation["fresh_fetch_matches_spark"] = read_ticket(client, fresh, options) == reference
    finally:
        stop_monitor.set()
        watcher.join()


def cpu_window(stats, seconds):
    started = time.monotonic()
    initial = stats.cpu_seconds()
    time.sleep(seconds)
    cpu = stats.cpu_seconds() - initial
    return {"wall_s": round(time.monotonic() - started, 3),
            "cpu_s": round(cpu, 3) if math.isfinite(cpu) and cpu >= 0 else None}


def cancel_probe(stats, run, cancel):
    result = {"query": LONG_SQL}
    result["idle"] = cpu_window(stats, 10)
    query = result["query_result"] = {}

    def target():
        record, _ = attempt(run)
        query.update(record)

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    result["query_before_cancel"] = cpu_window(stats, 5)
    result["query_active_at_cancel"] = thread.is_alive()
    result["cancel_call"], response = attempt(lambda: bounded(cancel, 10))
    if response is not None:
        result["cancel_response"] = str(response)
    result["after_cancel"] = cpu_window(stats, 10)
    result["client_active_after_10s"] = thread.is_alive()
    time.sleep(50)
    result["after_one_minute"] = cpu_window(stats, 10)
    result["client_active_after_70s"] = thread.is_alive()
    return result


def main():
    result = begin(TASK)
    reference = None
    with engine(result, "spark", label="spark-reference", output_name=TASK) as (_, spark, _, _):
        reference = fingerprint(collect(spark, TICKET_SQL))
    with engine(result, "sail", label="sail-flight", server="flight", memory="8g",
                extra={"SAIL_FLIGHT__SESSION_TIMEOUT_SECS": "120"}, output_name=TASK) as (_, _, stats, port):
        uri = f"grpc://localhost:{port}"
        if reference:
            with flight.FlightClient(uri) as client:
                ticket_lifecycle(result, client, stats, reference)
        try:
            conn = flightsql.connect(uri)
            cursor = conn.cursor()

            def run():
                cursor.execute(LONG_SQL)
                return cursor.fetchall()

            result["flight_cancel"] = cancel_probe(stats, run, cursor.adbc_cancel)
        except Exception as exc:
            result["flight_cancel"] = {"error": error(exc)}
    with engine(result, "sail", label="sail-connect", memory="8g", output_name=TASK) as (_, spark, stats, _):
        result["connect_interrupt"] = cancel_probe(stats, lambda: collect(spark, LONG_SQL, 120), spark.interruptAll)
    save(TASK, result)


if __name__ == "__main__":
    main()
