"""Run the 22 TPC-H-derived queries on an engine reachable over Spark Connect."""

import re
import time
from pathlib import Path

from harness.common import canon

QDIR = Path(__file__).parent / "queries" / "tpch"


def statements(q: int) -> list[str]:
    text = (QDIR / f"q{q}.sql").read_text()
    text = "\n".join(l for l in text.splitlines() if not l.strip().startswith("--"))
    return [s.strip() for s in text.split(";") if s.strip()]


_USE_FALLBACK: dict = {}


def use(spark, namespace: str) -> None:
    """Spark accepts `USE catalog.namespace`; fall back to the catalog API where that fails."""
    key = id(spark)
    if not _USE_FALLBACK.get(key):
        try:
            spark.sql(f"USE polaris.{namespace}")
            return
        except Exception:  # noqa: BLE001
            _USE_FALLBACK[key] = True
    spark.catalog.setCurrentCatalog("polaris")
    spark.catalog.setCurrentDatabase(namespace)


def run_query(spark, q: int, namespace: str):
    """Returns (rows, columns, seconds). Seconds covers planning + execution + full collect."""
    use(spark, namespace)
    stmts = statements(q)
    rows, cols = None, None
    t0 = time.perf_counter()
    for s in stmts:
        df = spark.sql(s)
        if re.match(r"(?is)^\s*(select|with)\b", s):
            rows = df.collect()
            cols = df.columns
    secs = time.perf_counter() - t0
    return rows, cols, secs


def ordered(q: int) -> bool:
    return "order by" in (QDIR / f"q{q}.sql").read_text().lower()


def canon_rows(rows):
    return [[canon(v) for v in r] for r in rows]
