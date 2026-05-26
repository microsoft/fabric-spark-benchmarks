"""
Incremental Clustering Benchmark — PySpark

Runs 3 real-world workloads (streaming ingest, ETL pipeline, analytics table)
against configurable clustering modes. Results with full OPTIMIZE metrics are
written to a Delta table for analysis.

Usage (CLI):
    spark-submit incremental_clustering_benchmark.py \
        --result-table "abfss://..." \
        --engine fabric \
        --benchmark-schema-name my_benchmark_schema

Usage (programmatic):
    from incremental_clustering_benchmark import run_all
    run_all(spark, result_table_uri="abfss://...", engine="fabric",
            benchmark_schema_name="my_benchmark_schema")
"""

import argparse
import logging
import time
import uuid
from datetime import date, timedelta
from typing import Callable, Optional

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULT_NUM_ITERATIONS = 200
DEFAULT_BATCH_SIZE = 2_500_000
DEFAULT_BENCHMARK_SCHEMA = "liquid"

SELECTIVE_CATEGORY = "category_5"

# ---------------------------------------------------------------------------
# Spark configuration keys
# ---------------------------------------------------------------------------

CONF_INCREMENTAL = "spark.microsoft.delta.optimize.clustering.strategy.incremental" # Enabled by default in Fabric Spark Runtime 2.0+
CONF_OPTIMIZE_WRITE = "spark.databricks.delta.optimizeWrite.enabled"
CONF_FAST_OPTIMIZE = "spark.microsoft.delta.optimize.fast.enabled" # Enabled by default in Fabric Spark Runtime 2.0+
CONF_STATS_EXTENDED = (
    "spark.microsoft.delta.stats.collect.extended.property.setAtTableCreation"
) # Enabled by default in Fabric Spark Runtime 2.0+, disabling since other platforms don't generate sync stats as part of regular writes


# ---------------------------------------------------------------------------
# Result schema
# ---------------------------------------------------------------------------

def _build_result_schema() -> StructType:
    """Return the schema used for benchmark result rows."""
    file_size_stats = StructType([
        StructField("min", LongType()),
        StructField("max", LongType()),
        StructField("avg", DoubleType()),
        StructField("totalFiles", LongType()),
        StructField("totalSize", LongType()),
    ])
    clustering_file_stats = StructType([
        StructField("numFiles", LongType()),
        StructField("size", LongType()),
    ])
    incremental_stats = StructType([
        StructField("autoReclusterEnabled", BooleanType()),
        StructField("conformanceThreshold", DoubleType()),
        StructField("partialZCubeFound", BooleanType()),
        StructField("partialZCubeSize", LongType()),
        StructField("totalFilesInTable", LongType()),
        StructField("filesPassedToZCubeFilter", LongType()),
        StructField("filesSkippedMissingStats", IntegerType()),
        StructField("autoReclusterFilesAdded", IntegerType()),
        StructField("autoReclusterBytesAdded", LongType()),
        StructField("autoReclusterZCubesScored", IntegerType()),
    ])
    metrics_schema = StructType([
        StructField("numFilesAdded", LongType()),
        StructField("numFilesRemoved", LongType()),
        StructField("numFilesUpdatedWithoutRewrite", LongType()),
        StructField("filesAdded", file_size_stats),
        StructField("filesRemoved", file_size_stats),
        StructField("filesUpdatedWithoutRewrite", file_size_stats),
        StructField("filesRemovedBreakdown", ArrayType(StructType([
            StructField("reason", StringType()),
            StructField("metrics", file_size_stats),
        ]))),
        StructField("partitionsOptimized", LongType()),
        StructField("zOrderStats", StructType([
            StructField("strategyName", StringType()),
            StructField("inputCubeFiles", StructType([
                StructField("num", LongType()),
                StructField("size", LongType()),
            ])),
            StructField("inputOtherFiles", StructType([
                StructField("num", LongType()),
                StructField("size", LongType()),
            ])),
            StructField("inputNumCubes", LongType()),
            StructField("mergedFiles", StructType([
                StructField("num", LongType()),
                StructField("size", LongType()),
            ])),
            StructField("numOutputCubes", LongType()),
            StructField("mergedNumCubes", LongType()),
        ])),
        StructField("clusteringStats", StructType([
            StructField("inputZCubeFiles", clustering_file_stats),
            StructField("inputOtherFiles", clustering_file_stats),
            StructField("inputNumZCubes", LongType()),
            StructField("mergedFiles", clustering_file_stats),
            StructField("numOutputZCubes", LongType()),
            StructField("incrementalClusteringStats", incremental_stats),
        ])),
        StructField("numBins", LongType()),
        StructField("numBatches", LongType()),
        StructField("totalConsideredFiles", LongType()),
        StructField("totalFilesSkipped", LongType()),
        StructField("preserveInsertionOrder", BooleanType()),
        StructField("numFilesSkippedToReduceWriteAmplification", LongType()),
        StructField("numBytesSkippedToReduceWriteAmplification", LongType()),
        StructField("startTimeMs", LongType()),
        StructField("endTimeMs", LongType()),
        StructField("totalClusterParallelism", LongType()),
        StructField("totalScheduledTasks", LongType()),
        StructField("autoCompactParallelismStats", StructType([
            StructField("maxClusterActiveParallelism", LongType()),
            StructField("minClusterActiveParallelism", LongType()),
            StructField("maxSessionActiveParallelism", LongType()),
            StructField("minSessionActiveParallelism", LongType()),
        ])),
        StructField("deletionVectorStats", StructType([
            StructField("numDeletionVectorsRemoved", LongType()),
            StructField("numDeletionVectorRowsRemoved", LongType()),
        ])),
        StructField("numTableColumns", LongType()),
        StructField("numTableColumnsWithStats", LongType()),
    ])

    return StructType([
        StructField("iteration_id", StringType()),
        StructField("engine", StringType()),
        StructField("batch_size_target", LongType()),
        StructField("iterations", LongType()),
        StructField("scenario_name", StringType()),
        StructField("benchmark", StringType()),
        StructField("workload", StringType()),
        StructField("incremental_clustering_enabled", BooleanType()),
        StructField("iteration", LongType()),
        StructField("phase", StringType()),
        StructField("duration_ms", LongType()),
        StructField("active_file_count", LongType()),
        StructField("query_file_count", LongType()),
        StructField("path", StringType()),
        StructField("metrics", metrics_schema),
    ])


# ---------------------------------------------------------------------------
# Write-pattern functions
# ---------------------------------------------------------------------------

def _write_streaming_ingest(
    spark: SparkSession, table: str, iteration: int,
    batch_size: int, state: dict,
) -> str:
    """Streaming ingest: time-series appends with no overlap and growing categories."""
    num_categories = 10 + (iteration // 5)
    variance = 0.7 + (iteration % 5) * 0.15
    actual_size = int(batch_size * variance)
    date_offset = (iteration - 1) * 7

    data = (
        spark.range(0, actual_size)
        .withColumn(
            "category",
            F.concat(F.lit("category_"), (F.col("id") % num_categories).cast("string")),
        )
        .withColumn("value1", F.rand(iteration) * 1000)
        .withColumn("value2", F.rand(iteration + 1000) * 10000)
        .withColumn(
            "date1",
            F.date_add(
                F.lit("2022-01-01"),
                F.lit(date_offset) + (F.rand(iteration + 2000) * 7).cast("int"),
            ),
        )
    )
    data.write.format("delta").mode("append").saveAsTable(table)

    return str(date(2022, 1, 1) + timedelta(days=date_offset + 3))


def _write_etl_pipeline(
    spark: SparkSession, table: str, iteration: int,
    batch_size: int, state: dict,
) -> str:
    """ETL pipeline: MERGE upsert with a backfill every 30th iteration."""
    overlap = int(batch_size * 0.1)
    start = max(state["val"] - overlap, 0)
    state["val"] = start + batch_size
    is_backfill = iteration % 30 == 0

    if is_backfill:
        date_expr = F.date_add(
            F.lit("2020-01-01"), (F.rand(iteration + 2000) * 365).cast("int"),
        )
    else:
        date_expr = F.date_add(
            F.lit("2022-01-01"),
            F.lit((iteration - 1) * 7) + (F.rand(iteration + 2000) * 14).cast("int"),
        )

    data = (
        spark.range(start, state["val"])
        .withColumn(
            "category",
            F.concat(F.lit("category_"), (F.col("id") % 10).cast("string")),
        )
        .withColumn("value1", F.rand(iteration) * 1000)
        .withColumn("value2", F.rand(iteration + 1000) * 10000)
        .withColumn("date1", date_expr)
    )

    if iteration == 1:
        data.write.format("delta").mode("append").saveAsTable(table)
    else:
        data.createOrReplaceTempView("source_data")
        spark.sql(f"""
            MERGE INTO {table} t USING source_data s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    if is_backfill:
        return str(date(2020, 7, 1))
    return str(date(2022, 1, 1) + timedelta(days=(iteration - 1) * 7 + 7))


def _write_analytics_table(
    spark: SparkSession, table: str, iteration: int,
    batch_size: int, state: dict,
) -> str:
    """Analytics table: full-range append across all clustering columns."""
    variance = 0.7 + (iteration % 5) * 0.15
    actual_size = int(batch_size * variance)

    data = (
        spark.range(0, actual_size)
        .withColumn(
            "category",
            F.concat(F.lit("category_"), (F.col("id") % 10).cast("string")),
        )
        .withColumn("value1", F.rand(iteration) * 1000)
        .withColumn("value2", F.rand(iteration + 1000) * 10000)
        .withColumn(
            "date1",
            F.date_add(F.lit("2022-01-01"), (F.rand(iteration + 2000) * 1000).cast("int")),
        )
    )
    data.write.format("delta").mode("append").saveAsTable(table)

    return str(date(2022, 1, 1) + timedelta(days=(iteration - 1) + 30))


# ---------------------------------------------------------------------------
# Workload registry
# ---------------------------------------------------------------------------

WORKLOADS: dict[str, tuple[str, Callable]] = {
    "streaming": ("Streaming Ingest", _write_streaming_ingest),
    "etl":       ("ETL Pipeline",     _write_etl_pipeline),
    "analytics": ("Analytics Table",  _write_analytics_table),
}


# ---------------------------------------------------------------------------
# Benchmark runner
# ---------------------------------------------------------------------------

def _run_workload(
    spark: SparkSession,
    *,
    workload_name: str,
    scenario_name: str,
    table_name: str,
    write_fn: Callable,
    result_table_uri: str,
    iteration_id: str,
    engine: str,
    incremental: bool,
    optimize_write: bool,
    drop_table_post_run: bool = False,
    num_iterations: int = DEFAULT_NUM_ITERATIONS,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> str:
    """Run a single workload scenario and batch-write all results at the end."""

    spark.conf.set('spark.native.enabled', "true")
    spark.conf.set(CONF_INCREMENTAL, str(incremental).lower())
    spark.conf.set(CONF_OPTIMIZE_WRITE, str(optimize_write).lower())
    spark.conf.set(CONF_FAST_OPTIMIZE, "true")
    spark.conf.set(CONF_STATS_EXTENDED, "false")

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"""
        CREATE TABLE {table_name} (
            id LONG, category STRING, value1 DOUBLE, value2 DOUBLE, date1 DATE
        ) USING delta CLUSTER BY (date1, category)
    """)

    state: dict = {"val": 0}
    schema = _build_result_schema()
    all_rows: list[Row] = []

    def make_row(
        phase: str, iteration: int, duration_ms: int,
        active_files: int, query_file_count: int,
        table_path: str, metrics=None,
    ) -> Row:
        return Row(
            iteration_id=iteration_id,
            engine=engine,
            batch_size_target=batch_size,
            iterations=num_iterations,
            scenario_name=scenario_name,
            benchmark=workload_name,
            workload=workload_name,
            incremental_clustering_enabled=incremental,
            iteration=iteration,
            phase=phase,
            duration_ms=duration_ms,
            active_file_count=active_files,
            query_file_count=query_file_count,
            path=table_path,
            metrics=metrics,
        )

    try:
        for iteration in range(1, num_iterations + 1):
            # --- Write phase ---
            spark.sparkContext.setJobDescription(
                f"{workload_name} | iter {iteration}/{num_iterations} | write",
            )
            t0 = time.time()
            query_date = write_fn(spark, table_name, iteration, batch_size, state)
            write_ms = int((time.time() - t0) * 1000)

            detail = spark.sql(f"DESCRIBE DETAIL {table_name}").first()
            active_files = detail["numFiles"]
            table_path = detail["location"]

            all_rows.append(
                make_row("write", iteration, write_ms, active_files, 0, table_path),
            )

            # --- Optimize phase ---
            spark.sparkContext.setJobDescription(
                f"{workload_name} | iter {iteration}/{num_iterations} | optimize",
            )
            t0 = time.time()
            opt_result = spark.sql(f"OPTIMIZE {table_name}")
            opt_ms = int((time.time() - t0) * 1000)

            active_files = (
                spark.sql(f"DESCRIBE DETAIL {table_name}")
                .select("numFiles")
                .first()[0]
            )
            opt_row_data = opt_result.first()
            raw_metrics = opt_row_data["metrics"]
            raw_dict = raw_metrics.asDict() if raw_metrics else {}
            filtered_metrics = Row(
                **{f.name: raw_dict.get(f.name) for f in schema["metrics"].dataType},
            )

            all_rows.append(
                make_row(
                    "optimize", iteration, opt_ms, active_files, 0,
                    opt_row_data["path"], filtered_metrics,
                ),
            )

            # --- Query phase ---
            spark.sparkContext.setJobDescription(
                f"{workload_name} | iter {iteration}/{num_iterations} | query",
            )
            t0 = time.time()
            q = spark.sql(f"""
                SELECT * FROM {table_name}
                WHERE date1 = '{query_date}'
                  AND category = '{SELECTIVE_CATEGORY}'
            """)
            query_files = len(q.inputFiles())
            q.collect()
            query_ms = int((time.time() - t0) * 1000)

            all_rows.append(
                make_row("query", iteration, query_ms, active_files, query_files, table_path),
            )

            if iteration == 1 or iteration % 10 == 0:
                logger.info(
                    "[%s] iter %d/%d: write=%dms opt=%dms query=%dms files=%d queryFiles=%d",
                    scenario_name, iteration, num_iterations,
                    write_ms, opt_ms, query_ms, active_files, query_files,
                )

    finally:
        if drop_table_post_run:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    spark.createDataFrame(all_rows, schema).repartition(1) \
        .write.format("delta").mode("append").save(result_table_uri)

    logger.info("[%s] Done — %d rows written to results table.", scenario_name, len(all_rows))
    return iteration_id


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_all(
    spark: SparkSession,
    result_table_uri: str,
    engine: str,
    *,
    benchmark_schema_name: str = DEFAULT_BENCHMARK_SCHEMA,
    workload_key: Optional[str] = None,
    num_iterations: int = DEFAULT_NUM_ITERATIONS,
    batch_size: int = DEFAULT_BATCH_SIZE,
    optimize_write: bool = False,
    incremental_clustering: bool = False,
    drop_table_post_run: bool = False,
) -> dict[str, str]:
    """Run benchmark workload(s) and return a mapping of scenario name to iteration ID.

    Args:
        spark: Active SparkSession.
        result_table_uri: Delta table path where benchmark results are appended.
        engine: Engine identifier recorded in results (e.g. ``"fabric"``).
        benchmark_schema_name: Schema (database) used as the namespace for
            benchmark tables (e.g. ``"my_schema"`` → ``my_schema.bench_si_…``).
        workload_key: Run a single workload (``"streaming"``, ``"etl"``, or
            ``"analytics"``). ``None`` runs all three.
        num_iterations: Number of write/optimize/query iterations per workload.
        batch_size: Target number of rows per write batch.
        optimize_write: Enable Spark optimized writes.
        incremental_clustering: Enable incremental clustering mode.
        drop_table_post_run: Drop each benchmark table after its run completes.
    """
    if workload_key and workload_key not in WORKLOADS:
        raise ValueError(
            f"Unknown workload '{workload_key}'. "
            f"Choose from: {', '.join(WORKLOADS)}"
        )

    selected = [WORKLOADS[workload_key]] if workload_key else list(WORKLOADS.values())
    scenario_label = "Incremental" if incremental_clustering else "Baseline"
    run_id = str(uuid.uuid4())[:8]
    ids: dict[str, str] = {}

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {benchmark_schema_name}")

    for workload_name, write_fn in selected:
        scenario = f"{workload_name} - {scenario_label}"
        iteration_id = str(uuid.uuid4())
        workload_prefix = "".join(w[0].lower() for w in workload_name.split())

        table_name = f"{benchmark_schema_name}.bench_{workload_prefix}_{run_id}"

        logger.info(
            "Starting scenario: %s (engine=%s, iterations=%d, batch_size=%d, table=%s)",
            scenario, engine, num_iterations, batch_size, table_name,
        )

        _run_workload(
            spark,
            workload_name=workload_name,
            scenario_name=scenario,
            table_name=table_name,
            write_fn=write_fn,
            result_table_uri=result_table_uri,
            iteration_id=iteration_id,
            engine=engine,
            incremental=incremental_clustering,
            optimize_write=optimize_write,
            drop_table_post_run=drop_table_post_run,
            num_iterations=num_iterations,
            batch_size=batch_size,
        )
        ids[scenario] = iteration_id

    logger.info("All results written to: %s", result_table_uri)
    logger.info("Run IDs: %s", ids)
    return ids


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Incremental Clustering Benchmark for PySpark / Fabric",
    )
    parser.add_argument(
        "--result-table", required=True,
        help="Delta table URI for benchmark results",
    )
    parser.add_argument(
        "--benchmark-schema-name", default=DEFAULT_BENCHMARK_SCHEMA,
        help=f"Schema (database) for benchmark tables (default: {DEFAULT_BENCHMARK_SCHEMA})",
    )
    parser.add_argument(
        "--engine", required=True,
        help="Engine identifier recorded in results (e.g. 'fabric')",
    )
    parser.add_argument(
        "--workload", choices=list(WORKLOADS),
        default=None,
        help="Run a single workload instead of all three",
    )
    parser.add_argument(
        "--iterations", type=int, default=DEFAULT_NUM_ITERATIONS,
        help=f"Number of iterations per workload (default: {DEFAULT_NUM_ITERATIONS})",
    )
    parser.add_argument(
        "--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
        help=f"Target rows per batch (default: {DEFAULT_BATCH_SIZE:,})",
    )
    parser.add_argument(
        "--incremental-clustering", action="store_true",
        help="Enable incremental clustering mode",
    )
    parser.add_argument(
        "--optimize-write", action="store_true",
        help="Enable Spark optimized writes",
    )
    parser.add_argument(
        "--drop-table-post-run", action="store_true",
        help="Drop each benchmark table after its run completes",
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    args = _parse_args()

    spark = SparkSession.builder.appName("IncrementalClusteringBenchmark").getOrCreate()

    run_all(
        spark,
        result_table_uri=args.result_table,
        engine=args.engine,
        benchmark_schema_name=args.benchmark_schema_name,
        workload_key=args.workload,
        num_iterations=args.iterations,
        batch_size=args.batch_size,
        optimize_write=args.optimize_write,
        incremental_clustering=args.incremental_clustering,
        drop_table_post_run=args.drop_table_post_run,
    )