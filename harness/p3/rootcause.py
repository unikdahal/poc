"""Replay the previously divergent outputs and test small causal discriminators."""

from collections import Counter
from decimal import localcontext
from itertools import combinations
from pathlib import Path

from harness import tpch
from harness.compat_queries import Q
from harness.p3.common import (PIN, UPSTREAM, begin, bounded, collect, group, load_tpcds, load_tpch,
                               normalized, numeric, query_file, row_key, save, timed, typed)

DS = (7, 9, 12, 18, 20, 26, 27, 28, 31, 36, 47, 49, 53, 57, 59, 63, 66, 77, 83, 89, 98)
H = (1, 8, 14, 17)
AVG = "SELECT avg(x) FROM VALUES (CAST(0.01 AS DECIMAL(12,2))), (0.01), (0.00) AS t(x)"
DIV = "SELECT CAST(10 AS DECIMAL(20,2)) / CAST(3 AS DECIMAL(20,2)) AS v"
SUM_DIV = "SELECT sum(x) / sum(y) FROM VALUES (CAST(10 AS DECIMAL(12,2)), CAST(3 AS DECIMAL(12,2))) AS t(x,y)"
WINDOW_DIV = "SELECT x / sum(x) OVER () FROM VALUES (CAST(1 AS DECIMAL(12,2))), (2.00) AS t(x) ORDER BY x"
DECIMAL = "crates/sail-plan/src/resolver/expression/predicate.rs#L832"
AGGREGATE = "crates/sail-plan/src/function/aggregate.rs#L67"
WINDOW = "crates/sail-plan/src/function/window.rs#L102"
CONVERSION = "crates/sail-plan/src/resolver/expression/cast.rs#L399"
STRING = "crates/sail-plan/src/function/scalar/string.rs"
DATETIME = "crates/sail-plan/src/function/scalar/datetime.rs"
COMPAT = {
    "dec_div": ("decimal precision or scale", DIV, DECIMAL, "decimal division"),
    "dec_sum_avg": ("rounding mode", AVG, AGGREGATE, "decimal avg"),
    "dec_overflow_38": ("ANSI behaviour", "SELECT CAST(9999999999999999999999999999.99 AS DECIMAL(30,2)) * CAST(99999999.99 AS DECIMAL(10,2))", DECIMAL, "decimal overflow"),
    "bround": ("rounding mode", "SELECT bround(2.5), bround(-2.5)", "crates/sail-function/src/scalar/math/bround.rs#L138", "bround"),
    "round_double": ("rounding mode", "SELECT round(CAST(1.005 AS DOUBLE), 2)", "crates/sail-plan/src/function/scalar/math.rs#L750", "round double"),
    "double_nan_inf": ("a function semantic", "SELECT log(0)", "crates/sail-plan/src/function/scalar/math.rs#L879", "log function"),
    "cast_str_decimal": ("a type coercion", "SELECT CAST('1e3' AS DECIMAL(10,2))", CONVERSION, "decimal cast"),
    "cast_double_string": ("a type coercion", "SELECT CAST(1.0E10 AS STRING)", CONVERSION, "float cast string"),
    "cast_float_string": ("a type coercion", "SELECT CAST(CAST(1e20 AS FLOAT) AS STRING)", CONVERSION, "float cast string"),
    "cast_decimal_to_int": ("ANSI behaviour", "SELECT CAST(CAST(3e9 AS DOUBLE) AS INT)", CONVERSION, "cast overflow"),
    "implicit_coercion": ("a type coercion", "SELECT 1 + '2'", DECIMAL, "type coercion"),
    "typeof_literals": ("decimal precision or scale", "SELECT typeof(CAST(1 AS DECIMAL(5,2)) + 1)", DECIMAL, "decimal precision"),
    "str_basic": ("a function semantic", "SELECT initcap('foo-bar')", f"{STRING}#L538", "initcap"),
    "regexp_lookahead": ("a function semantic", "SELECT regexp_replace('price100usd', '[0-9]+(?=usd)', 'N')", f"{STRING}#L79", "regexp lookahead"),
    "date_trunc": ("a function semantic", "SELECT trunc(DATE'2024-05-17', 'Q')", f"{DATETIME}#L156", "trunc date"),
    "make_date_ts": ("ANSI behaviour", "SELECT make_date(2023, 2, 29)", f"{DATETIME}#L397", "make_date"),
    "null_agg": ("rounding mode", AVG, AGGREGATE, "decimal avg"),
    "pivot": ("a function semantic", "SELECT * FROM (SELECT 1 AS id) PIVOT (count(*) FOR id IN (1, 2))",
              "crates/sail-plan/src/resolver/query/pivoting.rs#L259", "pivot"),
    "conditional_funcs": ("a function semantic", "SELECT decode(2, 1, 'one', 2, 'two', 'other')",
                          "crates/sail-function/src/scalar/string/encode_decode.rs#L290", "decode"),
}
TZ = [
    "CREATE NAMESPACE IF NOT EXISTS polaris.p3_timezone",
    "DROP TABLE IF EXISTS polaris.p3_timezone.case_table",
    "CREATE TABLE polaris.p3_timezone.case_table USING iceberg AS SELECT TIMESTAMP '2024-01-01 12:00:00' AS ts",
    "SELECT CAST(ts AS STRING) AS ts FROM polaris.p3_timezone.case_table",
]
QUERY_DIR = Path(__file__).with_name("queries")


def cases():
    all_cases = {}
    for q in DS:
        if q in (7, 9, 18, 26, 27, 28):
            expr, category, code, search = AVG, "rounding mode", AGGREGATE, "decimal avg"
        elif q in (12, 20, 47, 53, 57, 63, 89, 98):
            expr, category, code, search = WINDOW_DIV, "decimal precision or scale", WINDOW, "window decimal"
        elif q == 77:
            expr = "SELECT id, x FROM VALUES (1, 0), (2, 0) AS t(id,x) ORDER BY x"
            category, code, search = "tie ordering", "crates/sail-plan/src/resolver/expression/predicate.rs#L832", "sort tie"
        else:
            expr, category, code, search = SUM_DIV, "decimal precision or scale", DECIMAL, "decimal division"
        all_cases[f"tpcds-{q}"] = (query_file(QUERY_DIR / f"q{q}.sql"), [expr], category, code, search)
    for q in H:
        all_cases[f"tpch-{q}"] = (tpch.statements(q), [AVG if q == 1 else SUM_DIV],
                                   "rounding mode" if q in (1, 17) else "decimal precision or scale",
                                   AGGREGATE if q == 1 else DECIMAL, "decimal avg" if q == 1 else "decimal division")
    for key, _, sql in Q:
        if key in COMPAT:
            category, reduced, code, search = COMPAT[key]
            all_cases[key] = ([sql], [reduced], category, code, search)
    all_cases["timestamptz"] = (TZ, TZ, "a type coercion",
        "crates/sail-iceberg/src/datasource/type_converter.rs#L424", "Etc/UTC Iceberg")
    return all_cases


def output(session, expressions):
    def run():
        results = []
        for statement in expressions:
            df = session.sql(statement)
            rows = [[typed(item) for item in row] for row in df.collect()]
            results.append({"rows": rows, "names": df.columns,
                            "types": [field.dataType.simpleString() for field in df.schema.fields]})
        return results

    observation, value = timed(lambda: bounded(run, 210, session.interruptAll))
    observation.pop("elapsed_s")
    if value is not None:
        observation["outputs"] = value
    return observation


def aligned_pairs(left, right):
    if not left or not right or len(left[0]) != len(right[0]):
        return [], "no comparable positions"
    width = len(left[0])
    candidates = []
    if min(len(left), len(right)) > 1:
        for size in range(1, min(3, width) + 1):
            for cols in combinations(range(min(width, 16)), size):
                a = {row_key([row[i] for i in cols]): index for index, row in enumerate(left)}
                b = {row_key([row[i] for i in cols]): index for index, row in enumerate(right)}
                if len(a) == len(left) and len(b) == len(right):
                    shared = a.keys() & b.keys()
                    if len(shared) >= .8 * max(len(a), len(b)):
                        candidates.append((len(shared), cols, a, b))
            if candidates and max(c[0] for c in candidates) == min(len(left), len(right)):
                break
    if candidates:
        _, cols, a, b = max(candidates, key=lambda c: (c[0], -len(c[1])))
        return [(a.get(key), b.get(key)) for key in sorted(a.keys() | b.keys())], {"key_columns": list(cols)}
    return [(i if i < len(left) else None, i if i < len(right) else None)
            for i in range(max(len(left), len(right)))], "row ordinal"


def numeric_error(a, b):
    x, y = numeric(a), numeric(b)
    if x is None or y is None or not x.is_finite() or not y.is_finite():
        return None, None
    with localcontext() as context:
        context.prec = 80
        absolute = abs(x - y)
        relative = None if x == 0 else str(absolute / abs(x))
    return str(absolute), relative


def compare(left, right, expressions):
    if left["status"] != "ok" or right["status"] != "ok":
        return {"verdict": f"spark_{left['status']}" if left["status"] != "ok" else f"sail_{right['status']}"}
    statements = []
    for a, b, expr in zip(left["outputs"], right["outputs"], expressions):
        rows_a, rows_b = a["rows"], b["rows"]
        codes_a, codes_b = [row_key(row) for row in rows_a], [row_key(row) for row in rows_b]
        same_bag = Counter(codes_a) == Counter(codes_b)
        ordered = "order by" in expr.lower()
        verdict = ("match" if (codes_a == codes_b or (same_bag and not ordered))
                   else "order differs" if same_bag else "values differ")
        pairs, alignment = aligned_pairs(rows_a, rows_b) if verdict != "match" else ([], None)
        cells, missing_rows = [], []
        for i, j in pairs:
            if i is None or j is None:
                missing_rows.append({"spark_row": i, "sail_row": j})
                continue
            for col, (x, y) in enumerate(zip(rows_a[i], rows_b[j])):
                if normalized(x) != normalized(y):
                    absolute, relative = numeric_error(x, y)
                    cells.append({"spark_row": i, "sail_row": j, "column_index": col,
                                  "spark_column": a["names"][col], "sail_column": b["names"][col],
                                  "spark": x, "sail": y, "absolute_error": absolute, "relative_error": relative})
        statements.append({"verdict": verdict, "sail_schema": b["types"], "reference_schema": a["types"],
                           "schema_differs": a["types"] != b["types"], "alignment": alignment,
                           "row_counts": [len(rows_a), len(rows_b)], "positionally_equal": codes_a == codes_b,
                           "unmatched_rows": missing_rows, "cells": cells})
    verdict = next((item["verdict"] for item in statements if item["verdict"] != "match"), "match")
    return {"verdict": verdict, "statements": statements}


def run_suite(result, suite, name, spark, *_):
    collect(spark, "SET spark.sql.ansi.enabled=false")
    for case, (full, repro, category, code, search) in suite.items():
        rec = result["cases"].setdefault(case, {"original_sql": full, "repro_sql": repro,
            "candidate_category": category, "candidate_source": f"{UPSTREAM}/blob/{PIN}/{code}",
            "upstream_search": search})
        if case == "timestamptz":
            spark.conf.set("spark.sql.session.timeZone", "Etc/UTC")
        elif case.startswith("tpcds"):
            bounded(lambda: tpch.use(spark, "p3_tpcds_sf1"), 45, spark.interruptAll)
        elif case.startswith("tpch"):
            bounded(lambda: tpch.use(spark, "tpch_sf1"), 45, spark.interruptAll)
        if case == "timestamptz":
            original = [sql.replace("case_table", f"case_{name}_etc") for sql in full]
            repro_result = output(spark, original)
            rec[name] = {"original": repro_result, "repro": repro_result}
            spark.conf.set("spark.sql.session.timeZone", "UTC")
            control = [sql.replace("case_table", f"case_{name}_utc") for sql in repro]
            rec[name]["utc_control"] = output(spark, control)
        else:
            rec[name] = {"original": output(spark, full), "repro": output(spark, repro)}
        if name == "sail":
            for kind, expressions in (("original", full), ("repro", repro)):
                rec[kind + "_diff"] = compare(rec["spark"][kind], rec[name][kind], expressions)
        save(result)


def main():
    result = begin("rootcause")
    suite = cases()

    def reference(spark, *rest):
        result["data"] = {"tpch": load_tpch(spark, 1), "tpcds": load_tpcds(spark, 1)}
        run_suite(result, suite, "spark", spark, *rest)

    group(result, "spark-reference", reference, "spark")
    if result.get("data"):
        group(result, "sail", lambda *args: run_suite(result, suite, "sail", *args), "sail")
    save(result)


if __name__ == "__main__":
    main()
