# Spark Spatial Query Engine

A spatial query engine on Apache Spark: custom `ST_Contains` and `ST_Within` user-defined functions (UDFs) that bring range queries, range joins, distance queries, and distance joins to plain Spark SQL over point and rectangle datasets.

## Overview

Spark SQL is excellent at tabular analytics but has no native notion of space. This project adds one: two spatial UDFs registered in Scala, each usable directly inside `spark.sql(...)` queries. Points and rectangles are read from tab-separated CSV files as DataFrames, registered as temp views, and queried with familiar SQL — `select * from point where ST_Contains('x1,y1,x2,y2', point._c0)` — while Spark distributes the computation. It's a compact demonstration of how UDFs extend a distributed engine with domain-specific logic.

## The Four Query Types

| Function | What it answers | UDF |
|---|---|---|
| `runRangeQuery` | Which points fall inside a query rectangle? | `ST_Contains(rectangle, point)` |
| `runRangeJoinQuery` | Which points fall inside which rectangles? (point × rectangle join) | `ST_Contains(rectangle, point)` |
| `runDistanceQuery` | Which points are within *d* of a query point? | `ST_Within(point1, point2, distance)` |
| `runDistanceJoinQuery` | Which point pairs are within *d* of each other? (point × point join) | `ST_Within(point1, point2, distance)` |

Each function returns the matching row count and prints the result DataFrame — handy for verifying correctness on small inputs before scaling up.

## Key Engineering Decisions

- **Boundary-inclusive containment:** `ST_Contains` returns true when a point is inside *or on the boundary* of the rectangle — the standard spatial predicate semantics.
- **Order-agnostic rectangles:** the UDF computes `min`/`max` over the rectangle's corner coordinates instead of assuming a fixed corner order, so `x1,y1,x2,y2` works regardless of which corner is listed first.
- **Fail-soft parsing:** malformed coordinate strings return `false` rather than crashing the job — one bad row shouldn't kill a distributed query over millions.
- **SQL-first interface:** spatial logic lives in UDFs, so the queries stay declarative Spark SQL. The same functions work whether the data is 100 rows on a laptop or 100M rows on a cluster.

## Technologies

- **Scala 2.12** — implementation language
- **Apache Spark 3.3** (SQL) — distributed query engine
- **sbt** — build tool

## Running Locally

**Prerequisites:** Java 8/11, sbt, and a Spark 3.3 installation.

```bash
# 1. Package
sbt package   # produces target/scala-2.12/spark-spatial-query-engine_2.12-1.0.jar

# 2. Prepare tab-separated input files, e.g. points.tsv:
#    -1.5,2.3
#    0.5,-0.2

# 3. Call the query functions from spark-shell (or wire them into a small
#    main that parses command-line arguments):
spark-shell --jars target/scala-2.12/spark-spatial-query-engine_2.12-1.0.jar
```

```scala
// inside spark-shell
import spatial.SpatialQuery
val n = SpatialQuery.runRangeQuery(spark, "points.tsv", "-2.0,-2.0,2.0,2.0")
println(s"Points in rectangle: $n")
```

## Repository Structure

```
spark-spatial-query-engine/
├── SpatialQuery.scala   # ST_Contains / ST_Within UDFs + 4 query functions
├── build.sbt           # Scala 2.12 + Spark 3.3 dependencies
├── LICENSE
└── README.md
```

## Validation

- Boundary semantics checked: points exactly on rectangle edges are included.
- Malformed inputs (non-numeric strings) return `false` instead of throwing — verified by the try/catch design, not by crashing.
- Each function prints its result DataFrame (`resultDf.show()`) so outputs are eyeball-verifiable on small test files.

## Future Improvements

- Spatial indexing (e.g. grid partitioning) to avoid the O(n·m) nested-loop cost on large joins.
- Support for polygons and `ST_Intersects` beyond rectangles.
- A proper CLI runner with argument parsing instead of hardwired calls.

## Academic Context

This project was completed as part of graduate coursework in **CSE 578** at Arizona State University. It is presented here as academic project experience: a learning exercise in extending Spark with user-defined functions, not a production spatial database.
