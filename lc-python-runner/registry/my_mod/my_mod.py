"""Example / test functions. Every function receives Arrow data and returns structured records."""
import sys
import time

import pyarrow as pa
import pyarrow.compute as pc


def score(batch: pa.RecordBatch):
    """1-to-1. args: (numeric, numeric, string)."""
    a = pc.cast(batch.column(0), pa.float64())
    b = pc.cast(batch.column(1), pa.float64())
    s = pc.add(pc.fill_null(a, 0.0), pc.fill_null(b, 0.0))
    label = pc.utf8_upper(pc.cast(batch.column(2), pa.string()))
    return pa.RecordBatch.from_arrays([label, s, pc.greater(s, 10.0)],
                                      names=["label", "score", "valid"])


def inner(batch):
    return {"x": None} if batch.num_rows == 0 else pa.RecordBatch.from_arrays(
        [pc.multiply(pc.cast(batch.column(0), pa.int64()), 2)], names=["x"])


def outer(batch):
    return pa.RecordBatch.from_arrays(
        [pc.add(pc.cast(batch.column(0), pa.int64()), 100)], names=["z"])


def clean_dataset(batch):
    """Zero-arg call: receives every column of the child relation."""
    n = batch.num_rows
    names = ",".join(batch.schema.names)
    return pa.RecordBatch.from_arrays(
        [pa.array([batch.num_columns] * n, pa.int32()), pa.array([names] * n)],
        names=["ncols", "cols"])


def arg_names(batch):
    n = batch.num_rows
    return [{"names": ",".join(batch.schema.names)}] * n


def summarize(table: pa.Table):
    """N-to-1: one summary row per group."""
    v = table.column(0)
    first = table.column(1)[0].as_py() if table.num_columns > 1 and table.num_rows else None
    total = pc.sum(pc.cast(v, pa.float64())).as_py() if table.num_rows else 0.0
    return {"n": table.num_rows, "total": total, "first_key": first}


def count_rows(table: pa.Table):
    return {"n": table.num_rows}


def boom(batch):
    print("about to fail: boom-stdout-marker")          # goes to stderr, must survive the crash
    sys.stderr.write("boom-stderr-marker\n")
    raise ValueError("boom marker from user code")


def drop_rows(batch):
    return pa.RecordBatch.from_arrays([pa.array([1] * max(0, batch.num_rows - 1), pa.int32())],
                                      names=["x"])


def slow_echo(batch):
    time.sleep(0.05)
    return pa.RecordBatch.from_arrays([pc.cast(batch.column(0), pa.int64())], names=["x"])
