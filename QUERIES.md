# Every query and workload used in this POC

Pinned engine: Sail `790c622e`. Reference: Spark 3.5.10 + Iceberg 1.12.0. Comparison engine: Spark 3.5.10 + Comet 1.2.0.

| Workload | Queries | Scales | Engines | Source |
|---|---|---|---|---|
| TPC-H-derived (SQLBench-H) | 22 | SF1, SF10, SF30; 4-stream SF10 | Spark, Comet, Sail local, Sail local-cluster | `harness/queries/tpch/` |
| TPC-DS-derived | 99 | SF1 | Spark, Comet, Sail | `harness/queries/tpcds/` on branch `agent2` |
| SQL compatibility corpus | 99 + 6 ANSI | tiny inline tables | Spark vs Sail | `harness/compat_queries.py` |
| Iceberg DML matrix | 4 ops × 7 table variants | 10k rows | Spark vs Sail | `harness/suite_iceberg.py` |
| Iceberg features | CTAS, overwrite, evolution, time travel, metadata tables, procedures, 30 concurrent commits | 10k rows | Spark vs Sail | `harness/suite_iceberg.py` |
| Overhead / point lookup | `SELECT 1`, Iceberg point lookup | SF1 `nation` | Spark Connect, Sail Spark Connect, Sail Flight SQL | `harness/suite_cluster.py`, `suite_flight.py` |
| Head-of-line blocking | 1 heavy self-join + point lookups | SF1 | Spark, Sail local/local-cluster | `harness/suite_cluster.py` |
| Shuffle backends | 22 TPC-H | SF1 | Sail local, flight, storage (S3), Celeborn | `harness/suite_cluster.py`, `agent2/celeborn.py` |
| Kubernetes failure tests | heavy self-join, `SELECT 1`, point lookup | SF1 | Sail kubernetes-cluster (kind) | `harness/suite_k8s.py` |
| Memory pressure | TPC-H q9, q18, q21 | SF10, SF30 at 2/4/8 GiB | Spark, Comet, Sail | `agent2/memory.py` |
| Concurrency | 22 TPC-H, shuffled per stream | SF10, 1/4/8/16 streams | Comet, Sail, Sail local-cluster | `agent2/concurrency.py` |
| Ingest | CTAS lineitem/orders; MERGE (1.5M upd, 750k del, 750k ins) | SF10 | Spark, Comet, Sail, Sail local-cluster | `agent2/ingest.py` |
| Python UDFs | scalar, pandas, arrow, UDTF | 10M rows | Spark, Sail | `agent2/udf.py` |
| Catalog scale | SHOW / DESCRIBE / SELECT | 2,000 tables, 20 namespaces | Spark, Sail | `agent2/catalog.py` |
| Flight SQL lifecycle | 300 unfetched tickets; long cross join cancel | synthetic | Sail | `suite_flight.py`, `agent2/flight_leak.py` |
| Release vs main | 22 TPC-H | SF10 | Sail 0.7.3 wheel vs main | `agent2/release_vs_main.py` |

All TPC-H and TPC-DS numbers are derived from the TPC specifications and are not official TPC results.

## 1. TPC-H-derived queries (22)

Copied from Sail's SQLBench-H set; q15 uses a temporary view. Tables: `polaris.tpch_sf{1,10,30}`, Iceberg v2, written by Spark from `tpchgen-cli` Parquet.

<details><summary>q1</summary>

```sql
-- SPDX-FileComment: SQLBench-H query 1 derived from TPC-H query 1 under the terms of the TPC Fair Use Policy.
-- SPDX-FileCopyrightText: Copyright 1993-2022 Transaction Processing Performance Council

select
  l_returnflag,
  l_linestatus,
  sum(l_quantity) as sum_qty,
  sum(l_extendedprice) as sum_base_price,
  sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
  avg(l_quantity) as avg_qty,
  avg(l_extendedprice) as avg_price,
  avg(l_discount) as avg_disc,
  count(*) as count_order
from
  lineitem
where
  l_shipdate <= date '1998-12-01' - interval '68 days'
group by
  l_returnflag,
  l_linestatus
order by
  l_returnflag,
  l_linestatus;
```
</details>

<details><summary>q2</summary>

```sql
-- SPDX-FileComment: SQLBench-H query 2 derived from TPC-H query 2 under the terms of the TPC Fair Use Policy.
-- SPDX-FileCopyrightText: Copyright 1993-2022 Transaction Processing Performance Council

select
  s_acctbal,
  s_name,
  n_name,
  p_partkey,
  p_mfgr,
  s_address,
  s_phone,
  s_comment
from
  part,
  supplier,
  partsupp,
  nation,
  region
where
  p_partkey = ps_partkey
  and s_suppkey = ps_suppkey
  and p_size = 48
  and p_type like '%TIN'
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'ASIA'
  and ps_supplycost = (
    select
      min(ps_supplycost)
    from
      partsupp,
      supplier,
      nation,
      region
    where
      p_partkey = ps_partkey
      and s_suppkey = ps_suppkey
      and s_nationkey = n_nationkey
      and n_regionkey = r_regionkey
      and r_name = 'ASIA'
  )
order by
  s_acctbal desc,
  n_name,
  s_name,
  p_partkey
limit
  100;
```
</details>

<details><summary>q3</summary>

```sql
-- SPDX-FileComment: SQLBench-H query 3 derived from TPC-H query 3 under the terms of the TPC Fair Use Policy.
-- SPDX-FileCopyrightText: Copyright 1993-2022 Transaction Processing Performance Council

select
  l_orderkey,
  sum(l_extendedprice * (1 - l_discount)) as revenue,
  o_orderdate,
  o_shippriority
from
  customer,
  orders,
  lineitem
where
  c_mktsegment = 'BUILDING'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-03-15'
  and l_shipdate > date '1995-03-15'
group by
  l_orderkey,
  o_orderdate,
  o_shippriority
order by
  revenue desc,
  o_orderdate
limit
  10;
```
</details>

<details><summary>q4</summary>

```sql
-- SPDX-FileComment: SQLBench-H query 4 derived from TPC-H query 4 under the terms of the TPC Fair Use Policy.
-- SPDX-FileCopyrightText: Copyright 1993-2022 Transaction Processing Performance Council

select
  o_orderpriority,
  count(*) as order_count
from
  orders
where
  o_orderdate >= date '1995-04-01'
  and o_orderdate < date '1995-04-01' + interval '3' month
  and exists (
    select
      *
    from
      lineitem
    where
      l_orderkey = o_orderkey
      and l_commitdate < l_receiptdate
  )
group by
  o_orderpriority
order by
  o_orderpriority;
```
</details>

<details><summary>q5</summary>

```sql
-- SPDX-FileComment: SQLBench-H query 5 derived from TPC-H query 5 under the terms of the TPC Fair Use Policy.
-- SPDX-FileCopyrightText: Copyright 1993-2022 Transaction Processing Performance Council

select
  n_name,
  sum(l_extendedprice * (1 - l_discount)) as revenue
from
  customer,
  orders,
  lineitem,
  supplier,
  nation,
  region
where
  c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and l_suppkey = s_suppkey
  and c_nationkey = s_nationkey
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'AFRICA'
  and o_orderdate >= date '1994-01-01'
  and o_orderdate < date '1994-01-01' + interval '1' year
group by
  n_name
order by
  revenue desc;
```
</details>

<details><summary>q6</summary>

```sql
-- SPDX-FileComment: SQLBench-H query 6 derived from TPC-H query 6 under the terms of the TPC Fair Use Policy.
-- SPDX-FileCopyrightText: Copyright 1993-2022 Transaction Processing Performance Council

select
  sum(l_extendedprice * l_discount) as revenue
from
  lineitem
where
  l_shipdate >= date '1994-01-01'
  and l_shipdate < date '1994-01-01' + interval '1' year
  and l_discount between 0.04 - 0.01 and 0.04  + 0.01
  and l_quantity < 24;
```
</details>

<details><summary>q7</summary>

```sql
-- SPDX-FileComment: SQLBench-H query 7 derived from TPC-H query 7 under the terms of the TPC Fair Use Policy.
-- SPDX-FileCopyrightText: Copyright 1993-2022 Transaction Processing Performance Council

select
  supp_nation,
  cust_nation,
  l_year,
  sum(volume) as revenue
from
  (
    select
      n1.n_name as supp_nation,
      n2.n_name as cust_nation,
      extract(
        year
        from
          l_shipdate
      ) as l_year,
      l_extendedprice * (1 - l_discount) as volume
    from
      supplier,
      lineitem,
      orders,
      customer,
      nation n1,
      nation n2
    where
      s_suppkey = l_suppkey
      and o_orderkey = l_orderkey
      and c_custkey = o_custkey
      and s_nationkey = n1.n_nationkey
      and c_nationkey = n2.n_nationkey
      and (
        (
          n1.n_name = 'GERMANY'
          and n2.n_name = 'IRAQ'
        )
        or (
          n1.n_name = 'IRAQ'
          and n2.n_name = 'GERMANY'
        )
      )
      and l_shipdate between date '1995-01-01' and date  '1996-12-31'
  ) as shipping
group by
  supp_nation,
  cust_nation,
  l_year
order by
  supp_nation,
  cust_nation,
  l_year;
```
</details>

<details><summary>q8</summary>

```sql
-- SPDX-FileComment: SQLBench-H query 8 derived from TPC-H query 8 under the terms of the TPC Fair Use Policy.
-- SPDX-FileCopyrightText: Copyright 1993-2022 Transaction Processing Performance Council

select
  o_year,
  sum(
    case
      when nation = 'IRAQ' then volume
      else 0
    end
  ) / sum(volume) as mkt_share
from
  (
    select
      extract(
        year
        from
          o_orderdate
      ) as o_year,
      l_extendedprice * (1 - l_discount) as volume,
      n2.n_name as nation
    from
      part,
      supplier,
      lineitem,
      orders,
      customer,
      nation n1,
      nation n2,
      region
    where
      p_partkey = l_partkey
      and s_suppkey = l_suppkey
      and l_orderkey = o_orderkey
      and o_custkey = c_custkey
      and c_nationkey = n1.n_nationkey
      and n1.n_regionkey = r_regionkey
      and r_name = 'MIDDLE EAST'
      and s_nationkey = n2.n_nationkey
      and o_orderdate between date '1995-01-01' and date  '1996-12-31'
      and p_type = 'LARGE PLATED STEEL'
  ) as all_nations
group by
  o_year
order by
  o_year;
```
</details>

<details><summary>q9</summary>

```sql
-- SPDX-FileComment: SQLBench-H query 9 derived from TPC-H query 9 under the terms of the TPC Fair Use Policy.
-- SPDX-FileCopyrightText: Copyright 1993-2022 Transaction Processing Performance Council

select
  nation,
  o_year,
  sum(amount) as sum_profit
from
  (
    select
      n_name as nation,
      extract(
        year
        from
          o_orderdate
      ) as o_year,
      l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    from
      part,
      supplier,
      lineitem,
      partsupp,
      orders,
      nation
    where
      s_suppkey = l_suppkey
      and ps_suppkey = l_suppkey
      and ps_partkey = l_partkey
      and p_partkey = l_partkey
      and o_orderkey = l_orderkey
      and s_nationkey = n_nationkey
      and p_name like '%moccasin%'
  ) as profit
group by
  nation,
  o_year
order by
  nation,
  o_year desc;
```
</details>

<details><summary>q10</summary>

```sql
-- SPDX-FileComment: SQLBench-H query 10 derived from TPC-H query 10 under the terms of the TPC Fair Use Policy.
-- SPDX-FileCopyrightText: Copyright 1993-2022 Transaction Processing Performance Council

select
  c_custkey,
  c_name,
  sum(l_extendedprice * (1 - l_discount)) as revenue,
  c_acctbal,
  n_name,
  c_address,
  c_phone,
  c_comment
from
  customer,
  orders,
  lineitem,
  nation
where
  c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate >= date '1993-07-01'
  and o_orderdate < date '1993-07-01' + interval '3' month
  and l_returnflag = 'R'
  and c_nationkey = n_nationkey
group by
  c_custkey,
  c_name,
  c_acctbal,
  c_phone,
  n_name,
  c_address,
  c_comment
order by
  revenue desc
limit
  20;
```
</details>

<details><summary>q11</summary>

```sql
-- SPDX-FileComment: SQLBench-H query 11 derived from TPC-H query 11 under the terms of the TPC Fair Use Policy.
-- SPDX-FileCopyrightText: Copyright 1993-2022 Transaction Processing Performance Council

select
  ps_partkey,
  sum(ps_supplycost * ps_availqty) as value
from
  partsupp,
  supplier,
  nation
where
  ps_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'ALGERIA'
group by
  ps_partkey
having
  sum(ps_supplycost * ps_availqty) > (
    select
      sum(ps_supplycost * ps_availqty) * 0.0001000000
    from
      partsupp,
      supplier,
      nation
    where
      ps_suppkey = s_suppkey
      and s_nationkey = n_nationkey
      and n_name = 'ALGERIA'
  )
order by
  value desc;
```
</details>

<details><summary>q12</summary>

```sql
-- SPDX-FileComment: SQLBench-H query 12 derived from TPC-H query 12 under the terms of the TPC Fair Use Policy.
-- SPDX-FileCopyrightText: Copyright 1993-2022 Transaction Processing Performance Council

select
  l_shipmode,
  sum(
    case
      when o_orderpriority = '1-URGENT'
      or o_orderpriority = '2-HIGH' then 1
      else 0
    end
  ) as high_line_count,
  sum(
    case
      when o_orderpriority <> '1-URGENT'
      and o_orderpriority <> '2-HIGH' then 1
      else 0
    end
  ) as low_line_count
from
  orders,
  lineitem
where
  o_orderkey = l_orderkey
  and l_shipmode in ('FOB', 'SHIP')
  and l_commitdate < l_receiptdate
  and l_shipdate < l_commitdate
  and l_receiptdate >= date '1995-01-01'
  and l_receiptdate < date '1995-01-01' + interval '1' year
group by
  l_shipmode
order by
  l_shipmode;
```
</details>

<details><summary>q13</summary>

```sql
-- SPDX-FileComment: SQLBench-H query 13 derived from TPC-H query 13 under the terms of the TPC Fair Use Policy.
-- SPDX-FileCopyrightText: Copyright 1993-2022 Transaction Processing Performance Council

select
  c_count,
  count(*) as custdist
from
  (
    select
      c_custkey,
      count(o_orderkey)
    from
      customer
      left outer join orders on c_custkey = o_custkey
      and o_comment not like '%express%requests%'
    group by
      c_custkey
  ) as c_orders (c_custkey, c_count)
group by
  c_count
order by
  custdist desc,
  c_count desc;
```
</details>

<details><summary>q14</summary>

```sql
-- SPDX-FileComment: SQLBench-H query 14 derived from TPC-H query 14 under the terms of the TPC Fair Use Policy.
-- SPDX-FileCopyrightText: Copyright 1993-2022 Transaction Processing Performance Council

select
  100.00 * sum(
    case
      when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount)
      else 0
    end
  ) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
  lineitem,
  part
where
  l_partkey = p_partkey
  and l_shipdate >= date '1995-02-01'
  and l_shipdate < date '1995-02-01' + interval '1' month;
```
</details>

<details><summary>q15</summary>

```sql
-- SPDX-FileComment: SQLBench-H query 15 derived from TPC-H query 15 under the terms of the TPC Fair Use Policy.
-- SPDX-FileCopyrightText: Copyright 1993-2022 Transaction Processing Performance Council

create temporary view revenue0 (supplier_no, total_revenue) as
select
  l_suppkey,
  sum(l_extendedprice * (1 - l_discount))
from
  lineitem
where
  l_shipdate >= date '1996-08-01'
  and l_shipdate < date '1996-08-01' + interval '3' month
group by
  l_suppkey;

select
  s_suppkey,
  s_name,
  s_address,
  s_phone,
  total_revenue
from
  supplier,
  revenue0
where
  s_suppkey = supplier_no
  and total_revenue = (
    select
      max(total_revenue)
    from
      revenue0
  )
order by
  s_suppkey;

drop view revenue0;
```
</details>

<details><summary>q16</summary>

```sql
-- SPDX-FileComment: SQLBench-H query 16 derived from TPC-H query 16 under the terms of the TPC Fair Use Policy.
-- SPDX-FileCopyrightText: Copyright 1993-2022 Transaction Processing Performance Council

select
  p_brand,
  p_type,
  p_size,
  count(distinct ps_suppkey) as supplier_cnt
from
  partsupp,
  part
where
  p_partkey = ps_partkey
  and p_brand <> 'Brand#14'
  and p_type not like 'SMALL PLATED%'
  and p_size in (14, 6, 5, 31, 49, 15, 41, 47)
  and ps_suppkey not in (
    select
      s_suppkey
    from
      supplier
    where
      s_comment like '%Customer%Complaints%'
  )
group by
  p_brand,
  p_type,
  p_size
order by
  supplier_cnt desc,
  p_brand,
  p_type,
  p_size;
```
</details>

<details><summary>q17</summary>

```sql
-- SPDX-FileComment: SQLBench-H query 17 derived from TPC-H query 17 under the terms of the TPC Fair Use Policy.
-- SPDX-FileCopyrightText: Copyright 1993-2022 Transaction Processing Performance Council

select
  sum(l_extendedprice) / 7.0 as avg_yearly
from
  lineitem,
  part
where
  p_partkey = l_partkey
  and p_brand = 'Brand#42'
  and p_container = 'LG BAG'
  and l_quantity < (
    select
      0.2 * avg(l_quantity)
    from
      lineitem
    where
      l_partkey = p_partkey
  );
```
</details>

<details><summary>q18</summary>

```sql
-- SPDX-FileComment: SQLBench-H query 18 derived from TPC-H query 18 under the terms of the TPC Fair Use Policy.
-- SPDX-FileCopyrightText: Copyright 1993-2022 Transaction Processing Performance Council

select
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice,
  sum(l_quantity)
from
  customer,
  orders,
  lineitem
where
  o_orderkey in (
    select
      l_orderkey
    from
      lineitem
    group by
      l_orderkey
    having
      sum(l_quantity) > 313
  )
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice
order by
  o_totalprice desc,
  o_orderdate
limit
  100;
```
</details>

<details><summary>q19</summary>

```sql
-- SPDX-FileComment: SQLBench-H query 19 derived from TPC-H query 19 under the terms of the TPC Fair Use Policy.
-- SPDX-FileCopyrightText: Copyright 1993-2022 Transaction Processing Performance Council

select
  sum(l_extendedprice * (1 - l_discount)) as revenue
from
  lineitem,
  part
where
  (
    p_partkey = l_partkey
    and p_brand = 'Brand#21'
    and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    and l_quantity >= 8
    and l_quantity <= 8 + 10
    and p_size between 1 and 5
    and l_shipmode in ('AIR', 'AIR REG')
    and l_shipinstruct = 'DELIVER IN PERSON'
  )
  or (
    p_partkey = l_partkey
    and p_brand = 'Brand#13'
    and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
    and l_quantity >= 20
    and l_quantity <= 20 + 10
    and p_size between 1 and 10
    and l_shipmode in ('AIR', 'AIR REG')
    and l_shipinstruct = 'DELIVER IN PERSON'
  )
  or (
    p_partkey = l_partkey
    and p_brand = 'Brand#52'
    and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
    and l_quantity >= 30
    and l_quantity <= 30 + 10
    and p_size between 1 and 15
    and l_shipmode in ('AIR', 'AIR REG')
    and l_shipinstruct = 'DELIVER IN PERSON'
  );
```
</details>

<details><summary>q20</summary>

```sql
-- SPDX-FileComment: SQLBench-H query 20 derived from TPC-H query 20 under the terms of the TPC Fair Use Policy.
-- SPDX-FileCopyrightText: Copyright 1993-2022 Transaction Processing Performance Council

select
  s_name,
  s_address
from
  supplier,
  nation
where
  s_suppkey in (
    select
      ps_suppkey
    from
      partsupp
    where
      ps_partkey in (
        select
          p_partkey
        from
          part
        where
          p_name like 'blanched%'
      )
      and ps_availqty > (
        select
          0.5 * sum(l_quantity)
        from
          lineitem
        where
          l_partkey = ps_partkey
          and l_suppkey = ps_suppkey
          and l_shipdate >= date '1993-01-01'
          and l_shipdate < date '1993-01-01' + interval '1' year
      )
  )
  and s_nationkey = n_nationkey
  and n_name = 'KENYA'
order by
  s_name;
```
</details>

<details><summary>q21</summary>

```sql
-- SPDX-FileComment: SQLBench-H query 21 derived from TPC-H query 21 under the terms of the TPC Fair Use Policy.
-- SPDX-FileCopyrightText: Copyright 1993-2022 Transaction Processing Performance Council

select
  s_name,
  count(*) as numwait
from
  supplier,
  lineitem l1,
  orders,
  nation
where
  s_suppkey = l1.l_suppkey
  and o_orderkey = l1.l_orderkey
  and o_orderstatus = 'F'
  and l1.l_receiptdate > l1.l_commitdate
  and exists (
    select
      *
    from
      lineitem l2
    where
      l2.l_orderkey = l1.l_orderkey
      and l2.l_suppkey <> l1.l_suppkey
  )
  and not exists (
    select
      *
    from
      lineitem l3
    where
      l3.l_orderkey = l1.l_orderkey
      and l3.l_suppkey <> l1.l_suppkey
      and l3.l_receiptdate > l3.l_commitdate
  )
  and s_nationkey = n_nationkey
  and n_name = 'ARGENTINA'
group by
  s_name
order by
  numwait desc,
  s_name
limit
  100;
```
</details>

<details><summary>q22</summary>

```sql
-- SPDX-FileComment: SQLBench-H query 22 derived from TPC-H query 22 under the terms of the TPC Fair Use Policy.
-- SPDX-FileCopyrightText: Copyright 1993-2022 Transaction Processing Performance Council

select
  cntrycode,
  count(*) as numcust,
  sum(c_acctbal) as totacctbal
from
  (
    select
      substring(
        c_phone
        from
          1 for 2
      ) as cntrycode,
      c_acctbal
    from
      customer
    where
      substring(
        c_phone
        from
          1 for 2
      ) in ('24', '34', '16', '30', '33', '14', '13')
      and c_acctbal > (
        select
          avg(c_acctbal)
        from
          customer
        where
          c_acctbal > 0.00
          and substring(
            c_phone
            from
              1 for 2
          ) in ('24', '34', '16', '30', '33', '14', '13')
      )
      and not exists (
        select
          *
        from
          orders
        where
          o_custkey = c_custkey
      )
  ) as custsale
group by
  cntrycode
order by
  cntrycode;
```
</details>

## 2. SQL compatibility corpus (99 queries)

Run on Spark and Sail; results, column names and types compared. Inline test tables:

```sql
-- T
polaris.tpch_sf1
-- V
VALUES (1, 'a', 10.5, DATE'2024-01-31'), (2, NULL, -3.25, DATE'2024-02-29'), (3, 'c', NULL, NULL), (4, 'a', 0.0, DATE'2023-12-31') AS v(id, s, x, d)
```

| # | Name | Area | SQL |
|---|---|---|---|
| 1 | `dec_div` | decimal | `SELECT CAST(10 AS DECIMAL(10,2)) / CAST(3 AS DECIMAL(10,2)) AS r` |
| 2 | `dec_mul_scale` | decimal | `SELECT CAST(1.10 AS DECIMAL(3,2)) * CAST(123.45 AS DECIMAL(18,2)) AS r` |
| 3 | `dec_sum_avg` | decimal | `SELECT sum(l_extendedprice), avg(l_discount), sum(l_extendedprice * (1 - l_discount)) FROM polaris.tpch_sf1.lineitem WHERE l_orderkey < 1000` |
| 4 | `dec_overflow_38` | decimal | `SELECT CAST(9999999999999999999999999999.99 AS DECIMAL(30,2)) * CAST(99999999.99 AS DECIMAL(10,2)) AS r` |
| 5 | `round_half_up` | rounding | `SELECT round(2.5), round(3.5), round(-2.5), round(0.125, 2), round(1.005, 2), round(CAST(2.5 AS DOUBLE))` |
| 6 | `bround` | rounding | `SELECT bround(2.5), bround(3.5), bround(-2.5), bround(0.125, 2)` |
| 7 | `round_neg_scale` | rounding | `SELECT round(12345.678, -2), round(CAST(12345 AS INT), -2), round(CAST(155 AS BIGINT), -1)` |
| 8 | `round_double` | rounding | `SELECT round(CAST(1.005 AS DOUBLE), 2), round(CAST(2.675 AS DOUBLE), 2), round(CAST(-0.5 AS DOUBLE))` |
| 9 | `floor_ceil` | rounding | `SELECT floor(-1.5), ceil(-1.5), floor(CAST(2.7 AS DECIMAL(5,1))), ceil(CAST(2.1 AS DOUBLE))` |
| 10 | `int_div_mod` | arithmetic | `SELECT 7 DIV 2, -7 DIV 2, 7 % 3, -7 % 3, pmod(-7, 3), 7 / 2` |
| 11 | `div_by_zero` | arithmetic | `SELECT 1 / 0, 1.0 / 0, CAST(1 AS DOUBLE) / 0, 5 % 0` |
| 12 | `int_overflow_wrap` | arithmetic | `SELECT CAST(2147483647 AS INT) + 1, CAST(9223372036854775807 AS BIGINT) + 1` |
| 13 | `double_nan_inf` | arithmetic | `SELECT CAST('NaN' AS DOUBLE) = CAST('NaN' AS DOUBLE), sqrt(-1), log(0), CAST('Infinity' AS DOUBLE) > 1e308` |
| 14 | `power_exp` | arithmetic | `SELECT power(2, 10), power(2.0, -2), exp(1), ln(10), log10(1000), log(2, 8)` |
| 15 | `cast_str_int_invalid` | cast | `SELECT CAST('12a' AS INT), CAST(' 42 ' AS INT), CAST('3.9' AS INT), CAST('' AS INT)` |
| 16 | `cast_str_decimal` | cast | `SELECT CAST('1.2345' AS DECIMAL(10,2)), CAST('abc' AS DECIMAL(10,2)), CAST('1e3' AS DECIMAL(10,2))` |
| 17 | `cast_double_string` | cast | `SELECT CAST(1.0E10 AS STRING), CAST(0.1 + 0.2 AS STRING), CAST(CAST(1e-5 AS DOUBLE) AS STRING), CAST(CAST(123.0 AS DOUBLE) AS STRING)` |
| 18 | `cast_float_string` | cast | `SELECT CAST(CAST(3.14 AS FLOAT) AS STRING), CAST(CAST(1e20 AS FLOAT) AS STRING)` |
| 19 | `cast_bool` | cast | `SELECT CAST('true' AS BOOLEAN), CAST('Y' AS BOOLEAN), CAST('1' AS BOOLEAN), CAST('no' AS BOOLEAN), CAST(2 AS BOOLEAN)` |
| 20 | `cast_date_str` | cast | `SELECT CAST('2024-02-30' AS DATE), CAST('2024-1-5' AS DATE), CAST('2024-01-05 10:11:12' AS DATE), CAST('20240105' AS DATE)` |
| 21 | `cast_ts_str` | cast | `SELECT CAST('2024-01-05 10:11:12.123456' AS TIMESTAMP), CAST('2024-01-05T10:11:12Z' AS TIMESTAMP), CAST('2024-01-05' AS TIMESTAMP)` |
| 22 | `cast_decimal_to_int` | cast | `SELECT CAST(CAST(3.99 AS DECIMAL(5,2)) AS INT), CAST(CAST(-3.99 AS DECIMAL(5,2)) AS INT), CAST(CAST(3e9 AS DOUBLE) AS INT)` |
| 23 | `implicit_coercion` | cast | `SELECT 1 + '2', '3' * '4', 1 = '1', '10' > 9, concat(1, 2.5)` |
| 24 | `typeof_literals` | types | `SELECT typeof(1), typeof(1.5), typeof(1e1), typeof(2147483648), typeof(CAST(1 AS DECIMAL(5,2)) + 1)` |
| 25 | `str_basic` | string | `SELECT upper('aBc'), lower('AbC'), initcap('hello wORLD foo-bar'), length('héllo'), char_length('héllo'), octet_length('héllo')` |
| 26 | `str_substr` | string | `SELECT substring('abcdef', 2, 3), substring('abcdef', -3), substring('abcdef', 0, 2), substr('abc', 5), left('abcdef', 2), right('abcdef', 2)` |
| 27 | `str_pad_trim` | string | `SELECT lpad('7', 3, '0'), rpad('ab', 5, 'xy'), lpad('abcdef', 3, '0'), trim('  x  '), ltrim('xxaxx', 'x'), btrim('--a--', '-')` |
| 28 | `str_concat_null` | string | `SELECT concat('a', NULL), concat_ws('-', 'a', NULL, 'b'), 'a' \|\| 'b', concat_ws(',', array('x', 'y'))` |
| 29 | `str_instr_locate` | string | `SELECT instr('hello', 'l'), locate('l', 'hello', 4), position('z' IN 'hello'), replace('aaa', 'a', 'bb'), translate('abc', 'ab', 'x')` |
| 30 | `str_split` | string | `SELECT split('a,b,,c', ','), split('a1b22c', '[0-9]+'), split_part('a.b.c', '.', 2), size(split('', ','))` |
| 31 | `str_format_number` | string | `SELECT format_number(1234567.891, 2), format_number(0.5, 0), format_string('%05d\|%.3f\|%s', 42, CAST(3.14159 AS DOUBLE), 'x')` |
| 32 | `str_repeat_reverse` | string | `SELECT repeat('ab', 3), reverse('abc'), ascii('A'), chr(66), base64('hi'), hex('A'), md5('a'), sha2('a', 256)` |
| 33 | `regexp_extract` | regex | `SELECT regexp_extract('order-123-x', '([a-z]+)-(\\d+)', 2), regexp_extract('abc', '(\\d+)', 1)` |
| 34 | `regexp_replace` | regex | `SELECT regexp_replace('a1b22c333', '\\d+', '#'), regexp_replace('John Smith', '(\\w+) (\\w+)', '$2, $1')` |
| 35 | `regexp_like` | regex | `SELECT 'abc' RLIKE '^a.c$', 'ABC' RLIKE '(?i)abc', regexp_like('x9', '[0-9]'), regexp_count('aaa', 'a')` |
| 36 | `regexp_lookahead` | regex | `SELECT regexp_replace('price100usd', '\\d+(?=usd)', 'N')` |
| 37 | `like_escape` | string | `SELECT 'a_c' LIKE 'a\\_c', 'abc' LIKE 'a%', 'ABC' ILIKE 'a%', 'a%c' LIKE 'a!%c' ESCAPE '!'` |
| 38 | `date_arith` | datetime | `SELECT date_add(DATE'2024-01-31', 1), date_sub(DATE'2024-03-01', 1), add_months(DATE'2024-01-31', 1), add_months(DATE'2024-02-29', 12)` |
| 39 | `datediff_months_between` | datetime | `SELECT datediff(DATE'2024-03-01', DATE'2024-02-01'), months_between(DATE'2024-03-31', DATE'2024-02-29'), months_between(TIMESTAMP'2024-03-15 12:00:00', TIMESTAMP'2024-01-01 00:00:00', false)` |
| 40 | `date_trunc` | datetime | `SELECT trunc(DATE'2024-05-17', 'MM'), trunc(DATE'2024-05-17', 'YEAR'), trunc(DATE'2024-05-17', 'Q'), date_trunc('week', TIMESTAMP'2024-05-17 10:00:00'), date_trunc('hour', TIMESTAMP'2024-05-17 10:59:59')` |
| 41 | `date_parts` | datetime | `SELECT year(DATE'2024-12-31'), quarter(DATE'2024-12-31'), weekofyear(DATE'2024-12-31'), dayofweek(DATE'2024-12-29'), dayofyear(DATE'2024-12-31'), last_day(DATE'2024-02-10')` |
| 42 | `date_format` | datetime | `SELECT date_format(TIMESTAMP'2024-03-05 07:08:09.123', 'yyyy-MM-dd HH:mm:ss.SSS'), date_format(DATE'2024-03-05', 'dd/MM/yy EEE MMM'), date_format(DATE'2024-03-05', 'D')` |
| 43 | `to_date_formats` | datetime | `SELECT to_date('05/03/2024', 'dd/MM/yyyy'), to_date('2024-13-01'), to_timestamp('2024-03-05 07:08', 'yyyy-MM-dd HH:mm'), to_date('20240305', 'yyyyMMdd')` |
| 44 | `unix_time` | datetime | `SELECT unix_timestamp('2024-01-01 00:00:00'), from_unixtime(0), from_unixtime(1704067200, 'yyyy-MM-dd'), to_unix_timestamp(TIMESTAMP'1970-01-02 00:00:00')` |
| 45 | `ts_tz_convert` | datetime | `SELECT from_utc_timestamp(TIMESTAMP'2024-03-10 08:00:00', 'America/New_York'), to_utc_timestamp(TIMESTAMP'2024-03-10 02:30:00', 'America/New_York'), from_utc_timestamp(TIMESTAMP'2024-06-01 00:00:00', 'Asia/Kolkata')` |
| 46 | `ts_arith_interval` | datetime | `SELECT TIMESTAMP'2024-01-31 10:00:00' + INTERVAL 1 MONTH, DATE'2024-01-31' + INTERVAL '1' MONTH, TIMESTAMP'2024-01-01 00:00:00' - INTERVAL '90' MINUTE` |
| 47 | `make_date_ts` | datetime | `SELECT make_date(2024, 2, 29), make_date(2023, 2, 29), make_timestamp(2024, 1, 1, 1, 2, 3.5), date_from_unix_date(19000)` |
| 48 | `current_date_type` | datetime | `SELECT typeof(current_date()), typeof(current_timestamp()), typeof(now())` |
| 49 | `date_cmp_string` | datetime | `SELECT DATE'2024-01-02' > '2024-01-01', DATE'2024-01-02' = '2024-01-02', count(*) FROM range(1) WHERE DATE'2024-05-01' BETWEEN '2024-01-01' AND '2024-12-31'` |
| 50 | `null_ordering_asc` | nulls | `SELECT id, s FROM VALUES (1, 'a', 10.5, DATE'2024-01-31'), (2, NULL, -3.25, DATE'2024-02-29'), (3, 'c', NULL, NULL), (4, 'a', 0.0, DATE'2023-12-31') AS v(id, s, x, d) ORDER BY s, id` |
| 51 | `null_ordering_desc` | nulls | `SELECT id, x FROM VALUES (1, 'a', 10.5, DATE'2024-01-31'), (2, NULL, -3.25, DATE'2024-02-29'), (3, 'c', NULL, NULL), (4, 'a', 0.0, DATE'2023-12-31') AS v(id, s, x, d) ORDER BY x DESC, id` |
| 52 | `null_agg` | nulls | `SELECT count(*), count(s), count(DISTINCT s), sum(x), avg(x), min(s), max(d) FROM VALUES (1, 'a', 10.5, DATE'2024-01-31'), (2, NULL, -3.25, DATE'2024-02-29'), (3, 'c', NULL, NULL), (4, 'a', 0.0, DATE'2023-12-31') AS v(id, s, x, d)` |
| 53 | `null_empty_agg` | nulls | `SELECT sum(id), avg(id), count(id), max(id) FROM range(0) AS t(id)` |
| 54 | `not_in_null` | nulls | `SELECT count(*) FROM range(5) WHERE id NOT IN (1, NULL)` |
| 55 | `null_safe_eq` | nulls | `SELECT NULL <=> NULL, 1 <=> NULL, NULL = NULL, coalesce(NULL, 2, 3), nvl(NULL, 'x'), nullif(1, 1), ifnull(NULL, 0)` |
| 56 | `case_when_null` | nulls | `SELECT id, CASE WHEN s = 'a' THEN 1 WHEN s IS NULL THEN 2 ELSE 3 END, if(x > 0, 'pos', 'nonpos') FROM VALUES (1, 'a', 10.5, DATE'2024-01-31'), (2, NULL, -3.25, DATE'2024-02-29'), (3, 'c', NULL, NULL), (4, 'a', 0.0, DATE'2023-12-31') AS v(id, s, x, d) ORDER BY id` |
| 57 | `greatest_least_null` | nulls | `SELECT greatest(1, NULL, 3), least(NULL, NULL), greatest('a', 'B'), least(DATE'2024-01-01', DATE'2023-01-01')` |
| 58 | `group_rollup` | aggregate | `SELECT n_regionkey, count(*) c FROM polaris.tpch_sf1.nation GROUP BY ROLLUP(n_regionkey) ORDER BY n_regionkey NULLS LAST` |
| 59 | `grouping_sets` | aggregate | `SELECT o_orderstatus, o_orderpriority, count(*), grouping_id() FROM polaris.tpch_sf1.orders GROUP BY GROUPING SETS ((o_orderstatus), (o_orderpriority), ()) ORDER BY 4, 1, 2` |
| 60 | `cube` | aggregate | `SELECT l_returnflag, l_linestatus, count(*) FROM polaris.tpch_sf1.lineitem GROUP BY CUBE(l_returnflag, l_linestatus) ORDER BY 1 NULLS FIRST, 2 NULLS FIRST` |
| 61 | `having_filter` | aggregate | `SELECT c_nationkey, count(*) FILTER (WHERE c_acctbal > 0) AS pos FROM polaris.tpch_sf1.customer GROUP BY c_nationkey HAVING count(*) > 5900 ORDER BY 1` |
| 62 | `stats_aggs` | aggregate | `SELECT stddev(l_quantity), stddev_pop(l_quantity), variance(l_quantity), corr(l_quantity, l_extendedprice), covar_samp(l_quantity, l_discount) FROM polaris.tpch_sf1.lineitem WHERE l_orderkey < 5000` |
| 63 | `percentile_exact` | aggregate | `SELECT percentile(l_quantity, 0.5), percentile(l_quantity, array(0.1, 0.9)), median(l_quantity) FROM polaris.tpch_sf1.lineitem WHERE l_orderkey < 5000` |
| 64 | `collect_sorted` | aggregate | `SELECT n_regionkey, sort_array(collect_list(n_name)), size(collect_set(n_regionkey)) FROM polaris.tpch_sf1.nation GROUP BY n_regionkey ORDER BY 1` |
| 65 | `bool_aggs` | aggregate | `SELECT bool_and(id > 0), bool_or(id > 3), every(id < 10), any(id = 2), count_if(id % 2 = 0) FROM range(1, 6)` |
| 66 | `first_last_ordered` | aggregate | `SELECT max_by(n_name, n_nationkey), min_by(n_name, n_nationkey) FROM polaris.tpch_sf1.nation` |
| 67 | `avg_int_type` | aggregate | `SELECT avg(id), sum(id), typeof(avg(id)), typeof(sum(CAST(id AS INT))) FROM range(1, 4)` |
| 68 | `pivot` | aggregate | `SELECT * FROM (SELECT n_regionkey, length(n_name) AS l FROM polaris.tpch_sf1.nation) PIVOT (count(*) FOR n_regionkey IN (0, 1, 2, 3, 4)) ORDER BY l` |
| 69 | `win_rank` | window | `SELECT n_name, n_regionkey, row_number() OVER (PARTITION BY n_regionkey ORDER BY n_name) rn, rank() OVER (ORDER BY n_regionkey) rk, dense_rank() OVER (ORDER BY n_regionkey) drk FROM polaris.tpch_sf1.nation ORDER BY n_name` |
| 70 | `win_running_sum` | window | `SELECT o_orderkey, sum(o_totalprice) OVER (ORDER BY o_orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM polaris.tpch_sf1.orders WHERE o_orderkey < 200 ORDER BY 1` |
| 71 | `win_range_default` | window | `SELECT o_orderdate, sum(o_totalprice) OVER (ORDER BY o_orderdate) FROM polaris.tpch_sf1.orders WHERE o_orderkey < 400 ORDER BY 1, 2` |
| 72 | `win_lag_lead` | window | `SELECT n_nationkey, lag(n_name) OVER (ORDER BY n_nationkey), lead(n_name, 2, 'none') OVER (ORDER BY n_nationkey) FROM polaris.tpch_sf1.nation ORDER BY 1` |
| 73 | `win_ntile_pct` | window | `SELECT n_nationkey, ntile(4) OVER (ORDER BY n_nationkey), percent_rank() OVER (ORDER BY n_nationkey), cume_dist() OVER (ORDER BY n_regionkey) FROM polaris.tpch_sf1.nation ORDER BY 1` |
| 74 | `win_first_last` | window | `SELECT n_nationkey, first_value(n_name) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey), last_value(n_name) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM polaris.tpch_sf1.nation ORDER BY 1` |
| 75 | `win_moving_avg` | window | `SELECT o_orderkey, avg(o_totalprice) OVER (ORDER BY o_orderkey ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) FROM polaris.tpch_sf1.orders WHERE o_orderkey < 100 ORDER BY 1` |
| 76 | `topk_per_group` | window | `SELECT * FROM (SELECT c_nationkey, c_custkey, c_acctbal, row_number() OVER (PARTITION BY c_nationkey ORDER BY c_acctbal DESC, c_custkey) r FROM polaris.tpch_sf1.customer) WHERE r <= 2 ORDER BY 1, 4` |
| 77 | `join_semi_anti` | join | `SELECT (SELECT count(*) FROM polaris.tpch_sf1.customer c LEFT SEMI JOIN polaris.tpch_sf1.orders o ON c.c_custkey = o.o_custkey), (SELECT count(*) FROM polaris.tpch_sf1.customer c LEFT ANTI JOIN polaris.tpch_sf1.orders o ON c.c_custkey = o.o_custkey)` |
| 78 | `join_full_outer_nulls` | join | `SELECT a.k, b.k FROM VALUES (1), (2), (NULL) AS a(k) FULL OUTER JOIN VALUES (2), (3), (NULL) AS b(k) ON a.k = b.k ORDER BY a.k NULLS LAST, b.k NULLS LAST` |
| 79 | `join_null_safe` | join | `SELECT count(*) FROM VALUES (1), (NULL) AS a(k) JOIN VALUES (1), (NULL) AS b(k) ON a.k <=> b.k` |
| 80 | `join_using_natural` | join | `SELECT count(*) FROM polaris.tpch_sf1.nation JOIN polaris.tpch_sf1.region ON n_regionkey = r_regionkey WHERE r_name = 'ASIA'` |
| 81 | `corr_exists` | subquery | `SELECT count(*) FROM polaris.tpch_sf1.orders o WHERE EXISTS (SELECT 1 FROM polaris.tpch_sf1.lineitem l WHERE l.l_orderkey = o.o_orderkey AND l_quantity > 49)` |
| 82 | `scalar_subquery` | subquery | `SELECT n_name, (SELECT count(*) FROM polaris.tpch_sf1.customer c WHERE c.c_nationkey = n.n_nationkey) FROM polaris.tpch_sf1.nation n ORDER BY 1` |
| 83 | `in_subquery` | subquery | `SELECT count(*) FROM polaris.tpch_sf1.supplier WHERE s_nationkey IN (SELECT n_nationkey FROM polaris.tpch_sf1.nation WHERE n_regionkey = 1)` |
| 84 | `cte_recursive_like` | subquery | `WITH a AS (SELECT id FROM range(5)), b AS (SELECT id * 2 AS id FROM a) SELECT sum(id) FROM b` |
| 85 | `lateral_view_explode` | complex | `SELECT k, v FROM VALUES (1, array('x', 'y')), (2, array()) AS t(k, arr) LATERAL VIEW OUTER explode(arr) e AS v ORDER BY k, v` |
| 86 | `array_funcs` | complex | `SELECT array_contains(array(1, 2), 2), array_distinct(array(1, 1, 2)), array_union(array(1), array(2)), element_at(array(1, 2, 3), -1), slice(array(1, 2, 3, 4), 2, 2), array_join(array('a', NULL, 'b'), ',', '?')` |
| 87 | `map_funcs` | complex | `SELECT map_keys(map('a', 1, 'b', 2)), element_at(map('a', 1), 'z'), map_from_arrays(array('k'), array(1)), size(map())` |
| 88 | `struct_access` | complex | `SELECT named_struct('a', 1, 'b', 'x').b, struct(1 AS x).x` |
| 89 | `json_funcs` | complex | `SELECT get_json_object('{"a":{"b":[1,2]}}', '$.a.b[1]'), json_tuple('{"x":1,"y":"z"}', 'x', 'y'), to_json(named_struct('a', 1, 'b', CAST(NULL AS STRING)))` |
| 90 | `from_json_schema` | complex | `SELECT from_json('{"a":1,"b":[1,2]}', 'a INT, b ARRAY<INT>')` |
| 91 | `higher_order` | complex | `SELECT transform(array(1, 2), x -> x * 10), filter(array(1, 2, 3), x -> x % 2 = 1), aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x), exists(array(1, 2), x -> x > 1)` |
| 92 | `try_funcs` | misc | `SELECT try_cast('x' AS INT), try_divide(1, 0), try_add(2147483647, 1), try_to_number('12.5', '99.9')` |
| 93 | `hash_funcs` | misc | `SELECT hash('a'), xxhash64('a'), crc32('a')` |
| 94 | `conditional_funcs` | misc | `SELECT nvl2(NULL, 'a', 'b'), decode(2, 1, 'one', 2, 'two', 'other'), CASE 3 WHEN 1 THEN 'x' END` |
| 95 | `set_ops` | misc | `SELECT * FROM (SELECT id FROM range(5) UNION SELECT id FROM range(3, 8) EXCEPT SELECT 4 INTERSECT SELECT id FROM range(10)) ORDER BY 1` |
| 96 | `distinct_on_floats` | misc | `SELECT count(DISTINCT x) FROM VALUES (0.0), (-0.0), (CAST('NaN' AS DOUBLE)), (CAST('NaN' AS DOUBLE)) AS t(x)` |
| 97 | `order_by_alias_ordinal` | misc | `SELECT n_regionkey AS r, count(*) AS c FROM polaris.tpch_sf1.nation GROUP BY 1 ORDER BY c DESC, r` |
| 98 | `limit_offset` | misc | `SELECT n_nationkey FROM polaris.tpch_sf1.nation ORDER BY 1 LIMIT 3 OFFSET 5` |
| 99 | `sample_values_inline` | misc | `SELECT * FROM VALUES ('x', 1), ('y', 2) AS t(a, b) WHERE b > 1` |

**ANSI-mode cases**

| Name | SQL |
|---|---|
| `ansi_cast_invalid` | `SELECT CAST('12a' AS INT)` |
| `ansi_int_overflow` | `SELECT CAST(2147483647 AS INT) + 1` |
| `ansi_div_zero` | `SELECT 1 / 0` |
| `ansi_decimal_overflow` | `SELECT CAST(99999 AS DECIMAL(5,0)) * 10` |
| `ansi_bad_date` | `SELECT CAST('2024-02-30' AS DATE)` |
| `ansi_array_oob` | `SELECT element_at(array(1, 2), 5)` |

## 3. Iceberg write tests

Source tables:

```sql
CREATE TABLE polaris.dml.src0 USING iceberg AS
SELECT id,
       concat('R', cast(id % 5 AS string)) AS region,
       cast((id * 37 % 100000) / 100.0 AS decimal(18,2)) AS amount,
       cast(id % 50 AS int) AS qty,
       date_add(date'2024-01-01', cast(id % 365 AS int)) AS updated,
       (id % 2 = 0) AS flag,
       CASE WHEN id % 11 = 0 THEN NULL ELSE concat('n', cast(id AS string)) END AS note
FROM range(1, 10001)

CREATE TABLE polaris.dml.msrc USING iceberg AS
SELECT id,
       concat('R', cast(id % 5 AS string)) AS region,
       cast((id * 13 % 50000) / 100.0 AS decimal(18,2)) AS amount,
       cast(id % 40 AS int) AS qty,
       date_add(date'2025-01-01', cast(id % 200 AS int)) AS updated,
       (id % 3 = 0) AS flag,
       CASE WHEN id % 10 = 0 THEN 'D' ELSE 'U' END AS op
FROM range(8001, 12001)
```

Each op runs on two identical copies, one written by Spark and one by Sail; then both engines read both copies.

| Op | SQL |
|---|---|
| delete | `DELETE FROM {t} WHERE qty % 7 = 0` |
| update | `UPDATE {t} SET amount = amount * 1.10, note = 'upd' WHERE region = 'R2' AND id % 3 = 0` |
| merge | `MERGE INTO {t} AS t USING polaris.dml.msrc AS s ON t.id = s.id WHEN MATCHED AND s.op = 'D' THEN DELETE WHEN MATCHED THEN UPDATE SET amount = s.amount, qty = t.qty + 1, note = 'merged' WHEN NOT MATCHED THEN INSERT (id, region, amount, qty, updated, flag, note) VALUES (s.id, s.region, s.amount, s.qty, s.updated, s.flag, 'ins') WHEN NOT MATCHED BY SOURCE AND t.region = 'R4' AND t.id < 100 THEN DELETE` |
| insert | `INSERT INTO {t} SELECT id + 100000, region, amount, qty, updated, flag, 'app' FROM polaris.dml.src0 WHERE id <= 500` |

| Variant | Format | Write mode | Partitioned |
|---|---|---|---|
| `v2_cow_part` | v2 | copy-on-write | yes |
| `v2_cow_unpart` | v2 | copy-on-write | no |
| `v2_mor_part` | v2 | merge-on-read | yes |
| `v2_mor_unpart` | v2 | merge-on-read | no |
| `v3_cow_part` | v3 | copy-on-write | yes |
| `v3_mor_part` | v3 | merge-on-read | yes |
| `v3_mor_unpart` | v3 | merge-on-read | no |

Other Iceberg checks, all run by Sail and verified by Spark:
- `CREATE TABLE … PARTITIONED BY (days(updated)) AS SELECT …`
- `INSERT OVERWRITE` with `partitionOverwriteMode=dynamic`
- Spark-side `ADD COLUMN`, `RENAME COLUMN`, `ADD PARTITION FIELD bucket(4, id)`, then Sail reads; Sail `ADD COLUMNS`, then Spark reads
- `VERSION AS OF <id>`, `TIMESTAMP AS OF`, `VERSION AS OF '<tag>'`
- Metadata tables: `snapshots`, `history`, `files`, `manifests`, `refs`, `metadata_log_entries`, `entries`, `partitions`, `all_data_files`, `all_manifests`, `data_files`, `delete_files`
- `CALL system.rewrite_data_files`, `expire_snapshots`, `rewrite_manifests`, `remove_orphan_files`, `rewrite_position_delete_files`
- 30 concurrent `INSERT`s from 3 sessions
- Type round-trips (default TZ and UTC)

## 4. Latency and head-of-line probes

```sql
SELECT 1;
SELECT * FROM polaris.tpch_sf1.nation WHERE n_nationkey = 7;   -- Iceberg point lookup

-- HEAVY: used for head-of-line blocking and Kubernetes kill tests
SELECT count(*), sum(a.l_extendedprice * b.l_discount) FROM polaris.tpch_sf1.lineitem a JOIN polaris.tpch_sf1.lineitem b ON a.l_suppkey = b.l_suppkey WHERE a.l_orderkey < 3000000;
```

Head-of-line: HEAVY runs in one session/connection while another runs the point lookup in a loop; p50/p99/max recorded.

## 5. Flight SQL probes

- Python and Java ADBC: `SELECT 1`, `executeUpdate`, prepared statement, `getInfo`, `getObjects`/`getTables`, temp view visibility across connections, ticket reuse, abandoned tickets, cancel, full `lineitem` scan (SF1).
- Ticket leak: `SELECT id, repeat(CAST(id AS STRING), 8) AS payload FROM range(20000)`, issued 300 times without fetching.
- Cancel: `SELECT sum(pmod(a.id + b.id, 1000)) FROM range(0, 200000, 1, 8) a CROSS JOIN range(0, 200000, 1, 8) b`

## 6. TPC-DS-derived queries (99)

Branch `agent2`, `harness/queries/tpcds/q1.sql` … `q99.sql`. Data: DuckDB `tpcds` extension 1.4.4 at SF1, loaded as Iceberg v2.

## 7. Other workloads (agent2)

- **Memory:** TPC-H q9, q18, q21 at SF10 and SF30; containers at 2/4/8 GiB; Sail memory pool unbounded vs fair (half the limit); Comet heap + ¼ off-heap.
- **Concurrency:** each stream warms with q6, then runs q1–q22 in its own deterministic shuffled order; 1/4/8/16 streams; 2 trials.
- **Ingest:** CTAS of SF10 `lineitem` and `orders` (v2/v3, COW/MOR); `MERGE INTO orders USING merge_input ON o_orderkey` (`op='delete'` → DELETE, `'update'` → `o_totalprice + 1.00`, `'insert'` → INSERT). The result is verified by a full outer join against an independently built expected table.
- **UDFs:** 10M rows from `range(0, 10M, 1, P)`; scalar / pandas / Arrow UDF and a UDTF, checked against the native `pmod(id * 3 + 7, 1000003)`; plus `SELECT 1` latency during the heavy UDF.
- **Catalog:** 20 namespaces × 100 tables; `SHOW TABLES`, `DESCRIBE`, `SELECT id FROM ns.t000`, first / warm same session / warm new session; Polaris token lifetime 90 s for the expiry test.
