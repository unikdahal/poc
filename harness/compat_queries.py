"""Spark SQL compatibility corpus. Each entry: (id, category, sql).

Results are compared value-by-value and by result schema against Spark 3.5. Multi-row results
carry ORDER BY so row order is part of the comparison. Queries reference only inline data or the
synthetic TPC-H sf1 tables.
"""

T = "polaris.tpch_sf1"
V = "VALUES (1, 'a', 10.5, DATE'2024-01-31'), (2, NULL, -3.25, DATE'2024-02-29'), (3, 'c', NULL, NULL), (4, 'a', 0.0, DATE'2023-12-31') AS v(id, s, x, d)"

Q = [
    # ---- decimal arithmetic & rounding (spreadsheet-style formulas)
    ("dec_div", "decimal", "SELECT CAST(10 AS DECIMAL(10,2)) / CAST(3 AS DECIMAL(10,2)) AS r"),
    ("dec_mul_scale", "decimal", "SELECT CAST(1.10 AS DECIMAL(3,2)) * CAST(123.45 AS DECIMAL(18,2)) AS r"),
    ("dec_sum_avg", "decimal", f"SELECT sum(l_extendedprice), avg(l_discount), sum(l_extendedprice * (1 - l_discount)) FROM {T}.lineitem WHERE l_orderkey < 1000"),
    ("dec_overflow_38", "decimal", "SELECT CAST(9999999999999999999999999999.99 AS DECIMAL(30,2)) * CAST(99999999.99 AS DECIMAL(10,2)) AS r"),
    ("round_half_up", "rounding", "SELECT round(2.5), round(3.5), round(-2.5), round(0.125, 2), round(1.005, 2), round(CAST(2.5 AS DOUBLE))"),
    ("bround", "rounding", "SELECT bround(2.5), bround(3.5), bround(-2.5), bround(0.125, 2)"),
    ("round_neg_scale", "rounding", "SELECT round(12345.678, -2), round(CAST(12345 AS INT), -2), round(CAST(155 AS BIGINT), -1)"),
    ("round_double", "rounding", "SELECT round(CAST(1.005 AS DOUBLE), 2), round(CAST(2.675 AS DOUBLE), 2), round(CAST(-0.5 AS DOUBLE))"),
    ("floor_ceil", "rounding", "SELECT floor(-1.5), ceil(-1.5), floor(CAST(2.7 AS DECIMAL(5,1))), ceil(CAST(2.1 AS DOUBLE))"),
    ("int_div_mod", "arithmetic", "SELECT 7 DIV 2, -7 DIV 2, 7 % 3, -7 % 3, pmod(-7, 3), 7 / 2"),
    ("div_by_zero", "arithmetic", "SELECT 1 / 0, 1.0 / 0, CAST(1 AS DOUBLE) / 0, 5 % 0"),
    ("int_overflow_wrap", "arithmetic", "SELECT CAST(2147483647 AS INT) + 1, CAST(9223372036854775807 AS BIGINT) + 1"),
    ("double_nan_inf", "arithmetic", "SELECT CAST('NaN' AS DOUBLE) = CAST('NaN' AS DOUBLE), sqrt(-1), log(0), CAST('Infinity' AS DOUBLE) > 1e308"),
    ("power_exp", "arithmetic", "SELECT power(2, 10), power(2.0, -2), exp(1), ln(10), log10(1000), log(2, 8)"),
    # ---- casts
    ("cast_str_int_invalid", "cast", "SELECT CAST('12a' AS INT), CAST(' 42 ' AS INT), CAST('3.9' AS INT), CAST('' AS INT)"),
    ("cast_str_decimal", "cast", "SELECT CAST('1.2345' AS DECIMAL(10,2)), CAST('abc' AS DECIMAL(10,2)), CAST('1e3' AS DECIMAL(10,2))"),
    ("cast_double_string", "cast", "SELECT CAST(1.0E10 AS STRING), CAST(0.1 + 0.2 AS STRING), CAST(CAST(1e-5 AS DOUBLE) AS STRING), CAST(CAST(123.0 AS DOUBLE) AS STRING)"),
    ("cast_float_string", "cast", "SELECT CAST(CAST(3.14 AS FLOAT) AS STRING), CAST(CAST(1e20 AS FLOAT) AS STRING)"),
    ("cast_bool", "cast", "SELECT CAST('true' AS BOOLEAN), CAST('Y' AS BOOLEAN), CAST('1' AS BOOLEAN), CAST('no' AS BOOLEAN), CAST(2 AS BOOLEAN)"),
    ("cast_date_str", "cast", "SELECT CAST('2024-02-30' AS DATE), CAST('2024-1-5' AS DATE), CAST('2024-01-05 10:11:12' AS DATE), CAST('20240105' AS DATE)"),
    ("cast_ts_str", "cast", "SELECT CAST('2024-01-05 10:11:12.123456' AS TIMESTAMP), CAST('2024-01-05T10:11:12Z' AS TIMESTAMP), CAST('2024-01-05' AS TIMESTAMP)"),
    ("cast_decimal_to_int", "cast", "SELECT CAST(CAST(3.99 AS DECIMAL(5,2)) AS INT), CAST(CAST(-3.99 AS DECIMAL(5,2)) AS INT), CAST(CAST(3e9 AS DOUBLE) AS INT)"),
    ("implicit_coercion", "cast", "SELECT 1 + '2', '3' * '4', 1 = '1', '10' > 9, concat(1, 2.5)"),
    ("typeof_literals", "types", "SELECT typeof(1), typeof(1.5), typeof(1e1), typeof(2147483648), typeof(CAST(1 AS DECIMAL(5,2)) + 1)"),
    # ---- strings
    ("str_basic", "string", "SELECT upper('aBc'), lower('AbC'), initcap('hello wORLD foo-bar'), length('héllo'), char_length('héllo'), octet_length('héllo')"),
    ("str_substr", "string", "SELECT substring('abcdef', 2, 3), substring('abcdef', -3), substring('abcdef', 0, 2), substr('abc', 5), left('abcdef', 2), right('abcdef', 2)"),
    ("str_pad_trim", "string", "SELECT lpad('7', 3, '0'), rpad('ab', 5, 'xy'), lpad('abcdef', 3, '0'), trim('  x  '), ltrim('xxaxx', 'x'), btrim('--a--', '-')"),
    ("str_concat_null", "string", "SELECT concat('a', NULL), concat_ws('-', 'a', NULL, 'b'), 'a' || 'b', concat_ws(',', array('x', 'y'))"),
    ("str_instr_locate", "string", "SELECT instr('hello', 'l'), locate('l', 'hello', 4), position('z' IN 'hello'), replace('aaa', 'a', 'bb'), translate('abc', 'ab', 'x')"),
    ("str_split", "string", "SELECT split('a,b,,c', ','), split('a1b22c', '[0-9]+'), split_part('a.b.c', '.', 2), size(split('', ','))"),
    ("str_format_number", "string", "SELECT format_number(1234567.891, 2), format_number(0.5, 0), format_string('%05d|%.3f|%s', 42, CAST(3.14159 AS DOUBLE), 'x')"),
    ("str_repeat_reverse", "string", "SELECT repeat('ab', 3), reverse('abc'), ascii('A'), chr(66), base64('hi'), hex('A'), md5('a'), sha2('a', 256)"),
    ("regexp_extract", "regex", "SELECT regexp_extract('order-123-x', '([a-z]+)-(\\\\d+)', 2), regexp_extract('abc', '(\\\\d+)', 1)"),
    ("regexp_replace", "regex", "SELECT regexp_replace('a1b22c333', '\\\\d+', '#'), regexp_replace('John Smith', '(\\\\w+) (\\\\w+)', '$2, $1')"),
    ("regexp_like", "regex", "SELECT 'abc' RLIKE '^a.c$', 'ABC' RLIKE '(?i)abc', regexp_like('x9', '[0-9]'), regexp_count('aaa', 'a')"),
    ("regexp_lookahead", "regex", "SELECT regexp_replace('price100usd', '\\\\d+(?=usd)', 'N')"),
    ("like_escape", "string", "SELECT 'a_c' LIKE 'a\\\\_c', 'abc' LIKE 'a%', 'ABC' ILIKE 'a%', 'a%c' LIKE 'a!%c' ESCAPE '!'"),
    # ---- dates & timestamps
    ("date_arith", "datetime", "SELECT date_add(DATE'2024-01-31', 1), date_sub(DATE'2024-03-01', 1), add_months(DATE'2024-01-31', 1), add_months(DATE'2024-02-29', 12)"),
    ("datediff_months_between", "datetime", "SELECT datediff(DATE'2024-03-01', DATE'2024-02-01'), months_between(DATE'2024-03-31', DATE'2024-02-29'), months_between(TIMESTAMP'2024-03-15 12:00:00', TIMESTAMP'2024-01-01 00:00:00', false)"),
    ("date_trunc", "datetime", "SELECT trunc(DATE'2024-05-17', 'MM'), trunc(DATE'2024-05-17', 'YEAR'), trunc(DATE'2024-05-17', 'Q'), date_trunc('week', TIMESTAMP'2024-05-17 10:00:00'), date_trunc('hour', TIMESTAMP'2024-05-17 10:59:59')"),
    ("date_parts", "datetime", "SELECT year(DATE'2024-12-31'), quarter(DATE'2024-12-31'), weekofyear(DATE'2024-12-31'), dayofweek(DATE'2024-12-29'), dayofyear(DATE'2024-12-31'), last_day(DATE'2024-02-10')"),
    ("date_format", "datetime", "SELECT date_format(TIMESTAMP'2024-03-05 07:08:09.123', 'yyyy-MM-dd HH:mm:ss.SSS'), date_format(DATE'2024-03-05', 'dd/MM/yy EEE MMM'), date_format(DATE'2024-03-05', 'D')"),
    ("to_date_formats", "datetime", "SELECT to_date('05/03/2024', 'dd/MM/yyyy'), to_date('2024-13-01'), to_timestamp('2024-03-05 07:08', 'yyyy-MM-dd HH:mm'), to_date('20240305', 'yyyyMMdd')"),
    ("unix_time", "datetime", "SELECT unix_timestamp('2024-01-01 00:00:00'), from_unixtime(0), from_unixtime(1704067200, 'yyyy-MM-dd'), to_unix_timestamp(TIMESTAMP'1970-01-02 00:00:00')"),
    ("ts_tz_convert", "datetime", "SELECT from_utc_timestamp(TIMESTAMP'2024-03-10 08:00:00', 'America/New_York'), to_utc_timestamp(TIMESTAMP'2024-03-10 02:30:00', 'America/New_York'), from_utc_timestamp(TIMESTAMP'2024-06-01 00:00:00', 'Asia/Kolkata')"),
    ("ts_arith_interval", "datetime", "SELECT TIMESTAMP'2024-01-31 10:00:00' + INTERVAL 1 MONTH, DATE'2024-01-31' + INTERVAL '1' MONTH, TIMESTAMP'2024-01-01 00:00:00' - INTERVAL '90' MINUTE"),
    ("make_date_ts", "datetime", "SELECT make_date(2024, 2, 29), make_date(2023, 2, 29), make_timestamp(2024, 1, 1, 1, 2, 3.5), date_from_unix_date(19000)"),
    ("current_date_type", "datetime", "SELECT typeof(current_date()), typeof(current_timestamp()), typeof(now())"),
    ("date_cmp_string", "datetime", "SELECT DATE'2024-01-02' > '2024-01-01', DATE'2024-01-02' = '2024-01-02', count(*) FROM range(1) WHERE DATE'2024-05-01' BETWEEN '2024-01-01' AND '2024-12-31'"),
    # ---- null semantics
    ("null_ordering_asc", "nulls", f"SELECT id, s FROM {V} ORDER BY s, id"),
    ("null_ordering_desc", "nulls", f"SELECT id, x FROM {V} ORDER BY x DESC, id"),
    ("null_agg", "nulls", f"SELECT count(*), count(s), count(DISTINCT s), sum(x), avg(x), min(s), max(d) FROM {V}"),
    ("null_empty_agg", "nulls", "SELECT sum(id), avg(id), count(id), max(id) FROM range(0) AS t(id)"),
    ("not_in_null", "nulls", "SELECT count(*) FROM range(5) WHERE id NOT IN (1, NULL)"),
    ("null_safe_eq", "nulls", "SELECT NULL <=> NULL, 1 <=> NULL, NULL = NULL, coalesce(NULL, 2, 3), nvl(NULL, 'x'), nullif(1, 1), ifnull(NULL, 0)"),
    ("case_when_null", "nulls", f"SELECT id, CASE WHEN s = 'a' THEN 1 WHEN s IS NULL THEN 2 ELSE 3 END, if(x > 0, 'pos', 'nonpos') FROM {V} ORDER BY id"),
    ("greatest_least_null", "nulls", "SELECT greatest(1, NULL, 3), least(NULL, NULL), greatest('a', 'B'), least(DATE'2024-01-01', DATE'2023-01-01')"),
    # ---- aggregates
    ("group_rollup", "aggregate", f"SELECT n_regionkey, count(*) c FROM {T}.nation GROUP BY ROLLUP(n_regionkey) ORDER BY n_regionkey NULLS LAST"),
    ("grouping_sets", "aggregate", f"SELECT o_orderstatus, o_orderpriority, count(*), grouping_id() FROM {T}.orders GROUP BY GROUPING SETS ((o_orderstatus), (o_orderpriority), ()) ORDER BY 4, 1, 2"),
    ("cube", "aggregate", f"SELECT l_returnflag, l_linestatus, count(*) FROM {T}.lineitem GROUP BY CUBE(l_returnflag, l_linestatus) ORDER BY 1 NULLS FIRST, 2 NULLS FIRST"),
    ("having_filter", "aggregate", f"SELECT c_nationkey, count(*) FILTER (WHERE c_acctbal > 0) AS pos FROM {T}.customer GROUP BY c_nationkey HAVING count(*) > 5900 ORDER BY 1"),
    ("stats_aggs", "aggregate", f"SELECT stddev(l_quantity), stddev_pop(l_quantity), variance(l_quantity), corr(l_quantity, l_extendedprice), covar_samp(l_quantity, l_discount) FROM {T}.lineitem WHERE l_orderkey < 5000"),
    ("percentile_exact", "aggregate", f"SELECT percentile(l_quantity, 0.5), percentile(l_quantity, array(0.1, 0.9)), median(l_quantity) FROM {T}.lineitem WHERE l_orderkey < 5000"),
    ("collect_sorted", "aggregate", f"SELECT n_regionkey, sort_array(collect_list(n_name)), size(collect_set(n_regionkey)) FROM {T}.nation GROUP BY n_regionkey ORDER BY 1"),
    ("bool_aggs", "aggregate", "SELECT bool_and(id > 0), bool_or(id > 3), every(id < 10), any(id = 2), count_if(id % 2 = 0) FROM range(1, 6)"),
    ("first_last_ordered", "aggregate", f"SELECT max_by(n_name, n_nationkey), min_by(n_name, n_nationkey) FROM {T}.nation"),
    ("avg_int_type", "aggregate", "SELECT avg(id), sum(id), typeof(avg(id)), typeof(sum(CAST(id AS INT))) FROM range(1, 4)"),
    ("pivot", "aggregate", f"SELECT * FROM (SELECT n_regionkey, length(n_name) AS l FROM {T}.nation) PIVOT (count(*) FOR n_regionkey IN (0, 1, 2, 3, 4)) ORDER BY l"),
    # ---- windows
    ("win_rank", "window", f"SELECT n_name, n_regionkey, row_number() OVER (PARTITION BY n_regionkey ORDER BY n_name) rn, rank() OVER (ORDER BY n_regionkey) rk, dense_rank() OVER (ORDER BY n_regionkey) drk FROM {T}.nation ORDER BY n_name"),
    ("win_running_sum", "window", f"SELECT o_orderkey, sum(o_totalprice) OVER (ORDER BY o_orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM {T}.orders WHERE o_orderkey < 200 ORDER BY 1"),
    ("win_range_default", "window", f"SELECT o_orderdate, sum(o_totalprice) OVER (ORDER BY o_orderdate) FROM {T}.orders WHERE o_orderkey < 400 ORDER BY 1, 2"),
    ("win_lag_lead", "window", f"SELECT n_nationkey, lag(n_name) OVER (ORDER BY n_nationkey), lead(n_name, 2, 'none') OVER (ORDER BY n_nationkey) FROM {T}.nation ORDER BY 1"),
    ("win_ntile_pct", "window", f"SELECT n_nationkey, ntile(4) OVER (ORDER BY n_nationkey), percent_rank() OVER (ORDER BY n_nationkey), cume_dist() OVER (ORDER BY n_regionkey) FROM {T}.nation ORDER BY 1"),
    ("win_first_last", "window", f"SELECT n_nationkey, first_value(n_name) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey), last_value(n_name) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM {T}.nation ORDER BY 1"),
    ("win_moving_avg", "window", f"SELECT o_orderkey, avg(o_totalprice) OVER (ORDER BY o_orderkey ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) FROM {T}.orders WHERE o_orderkey < 100 ORDER BY 1"),
    ("topk_per_group", "window", f"SELECT * FROM (SELECT c_nationkey, c_custkey, c_acctbal, row_number() OVER (PARTITION BY c_nationkey ORDER BY c_acctbal DESC, c_custkey) r FROM {T}.customer) WHERE r <= 2 ORDER BY 1, 4"),
    # ---- joins & subqueries
    ("join_semi_anti", "join", f"SELECT (SELECT count(*) FROM {T}.customer c LEFT SEMI JOIN {T}.orders o ON c.c_custkey = o.o_custkey), (SELECT count(*) FROM {T}.customer c LEFT ANTI JOIN {T}.orders o ON c.c_custkey = o.o_custkey)"),
    ("join_full_outer_nulls", "join", "SELECT a.k, b.k FROM VALUES (1), (2), (NULL) AS a(k) FULL OUTER JOIN VALUES (2), (3), (NULL) AS b(k) ON a.k = b.k ORDER BY a.k NULLS LAST, b.k NULLS LAST"),
    ("join_null_safe", "join", "SELECT count(*) FROM VALUES (1), (NULL) AS a(k) JOIN VALUES (1), (NULL) AS b(k) ON a.k <=> b.k"),
    ("join_using_natural", "join", f"SELECT count(*) FROM {T}.nation JOIN {T}.region ON n_regionkey = r_regionkey WHERE r_name = 'ASIA'"),
    ("corr_exists", "subquery", f"SELECT count(*) FROM {T}.orders o WHERE EXISTS (SELECT 1 FROM {T}.lineitem l WHERE l.l_orderkey = o.o_orderkey AND l_quantity > 49)"),
    ("scalar_subquery", "subquery", f"SELECT n_name, (SELECT count(*) FROM {T}.customer c WHERE c.c_nationkey = n.n_nationkey) FROM {T}.nation n ORDER BY 1"),
    ("in_subquery", "subquery", f"SELECT count(*) FROM {T}.supplier WHERE s_nationkey IN (SELECT n_nationkey FROM {T}.nation WHERE n_regionkey = 1)"),
    ("cte_recursive_like", "subquery", "WITH a AS (SELECT id FROM range(5)), b AS (SELECT id * 2 AS id FROM a) SELECT sum(id) FROM b"),
    ("lateral_view_explode", "complex", "SELECT k, v FROM VALUES (1, array('x', 'y')), (2, array()) AS t(k, arr) LATERAL VIEW OUTER explode(arr) e AS v ORDER BY k, v"),
    # ---- complex types / json
    ("array_funcs", "complex", "SELECT array_contains(array(1, 2), 2), array_distinct(array(1, 1, 2)), array_union(array(1), array(2)), element_at(array(1, 2, 3), -1), slice(array(1, 2, 3, 4), 2, 2), array_join(array('a', NULL, 'b'), ',', '?')"),
    ("map_funcs", "complex", "SELECT map_keys(map('a', 1, 'b', 2)), element_at(map('a', 1), 'z'), map_from_arrays(array('k'), array(1)), size(map())"),
    ("struct_access", "complex", "SELECT named_struct('a', 1, 'b', 'x').b, struct(1 AS x).x"),
    ("json_funcs", "complex", "SELECT get_json_object('{\"a\":{\"b\":[1,2]}}', '$.a.b[1]'), json_tuple('{\"x\":1,\"y\":\"z\"}', 'x', 'y'), to_json(named_struct('a', 1, 'b', CAST(NULL AS STRING)))"),
    ("from_json_schema", "complex", "SELECT from_json('{\"a\":1,\"b\":[1,2]}', 'a INT, b ARRAY<INT>')"),
    ("higher_order", "complex", "SELECT transform(array(1, 2), x -> x * 10), filter(array(1, 2, 3), x -> x % 2 = 1), aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x), exists(array(1, 2), x -> x > 1)"),
    # ---- misc spreadsheet-like
    ("try_funcs", "misc", "SELECT try_cast('x' AS INT), try_divide(1, 0), try_add(2147483647, 1), try_to_number('12.5', '99.9')"),
    ("hash_funcs", "misc", "SELECT hash('a'), xxhash64('a'), crc32('a')"),
    ("conditional_funcs", "misc", "SELECT nvl2(NULL, 'a', 'b'), decode(2, 1, 'one', 2, 'two', 'other'), CASE 3 WHEN 1 THEN 'x' END"),
    ("set_ops", "misc", "SELECT * FROM (SELECT id FROM range(5) UNION SELECT id FROM range(3, 8) EXCEPT SELECT 4 INTERSECT SELECT id FROM range(10)) ORDER BY 1"),
    ("distinct_on_floats", "misc", "SELECT count(DISTINCT x) FROM VALUES (0.0), (-0.0), (CAST('NaN' AS DOUBLE)), (CAST('NaN' AS DOUBLE)) AS t(x)"),
    ("order_by_alias_ordinal", "misc", f"SELECT n_regionkey AS r, count(*) AS c FROM {T}.nation GROUP BY 1 ORDER BY c DESC, r"),
    ("limit_offset", "misc", f"SELECT n_nationkey FROM {T}.nation ORDER BY 1 LIMIT 3 OFFSET 5"),
    ("sample_values_inline", "misc", "SELECT * FROM VALUES ('x', 1), ('y', 2) AS t(a, b) WHERE b > 1"),
]

# Error behaviour under ANSI mode (both engines should fail, or both succeed with the same value).
ANSI = [
    ("ansi_cast_invalid", "SELECT CAST('12a' AS INT)"),
    ("ansi_int_overflow", "SELECT CAST(2147483647 AS INT) + 1"),
    ("ansi_div_zero", "SELECT 1 / 0"),
    ("ansi_decimal_overflow", "SELECT CAST(99999 AS DECIMAL(5,0)) * 10"),
    ("ansi_bad_date", "SELECT CAST('2024-02-30' AS DATE)"),
    ("ansi_array_oob", "SELECT element_at(array(1, 2), 5)"),
]
