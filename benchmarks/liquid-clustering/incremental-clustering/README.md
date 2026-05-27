# Incremental Clustering Benchmark

## Overview

This benchmark measures the performance of running `OPTIMIZE` on Liquid Clustering enabled Delta Lake tables with Incremental Clustering versus baseline (semi-incremental clustering algorithm from OSS) across three real-world workload patterns. It quantifies write amplification reduction, optimize (clustering) duration, and query file-pruning improvements in Microsoft Fabric Spark.

## Methodology

### Workload patterns

| Workload | Write pattern | Clustering stress |
|---|---|---|
| **Streaming Ingest** | Time-series appends, no row overlap, growing number of categories | New data lands in narrow date ranges—incremental clustering can skip already-clustered regions |
| **ETL Pipeline** | MERGE upsert with 10% key overlap; full backfill every 30th iteration | Backfills scatter writes across the full date range, forcing broader re-clustering |
| **Analytics Table** | Append-only with values spanning the full range of all clustering columns | Every batch touches all clustering regions—worst case for incremental |

### Table schema

All workloads use the same schema with two cluster keys, `(date1, category)`:

```sql
CREATE TABLE ... (
    id LONG, category STRING, value1 DOUBLE, value2 DOUBLE, date1 DATE
) USING delta CLUSTER BY (date1, category)
```

### Per-iteration phases

Each of the configured iterations (default: 200) runs three timed phases:

1. **Write** — Execute the workload's write pattern (append or merge).
1. **Optimize** — Run `OPTIMIZE` and capture the full metrics struct (files added/removed, clustering stats).
1. **Query** — Run a selective point query (`WHERE date1 = '...' AND category = 'category_5'`) and record the number of files scanned.

### Timing and measurement

| Measurement | Source |
|---|---|
| Phase duration (write, optimize, query) | Wall-clock via `time.time()` |
| Active file count | `DESCRIBE DETAIL` after each write and optimize |
| OPTIMIZE metrics | Full metrics struct from the `OPTIMIZE` Spark SQL result |
| Query file count | `DataFrame.inputFiles()` |

## Prerequisites

- A Microsoft Fabric workspace with sufficient capacity (see [Published results](#published-results) for the exact SKU used).
- A Spark pool and Environment configured for your desired node count and runtime version.
- A lakehouse to run the benchmark in and write results to.
- No additional libraries required—uses only PySpark built-ins.

## How to reproduce

### Option 1: Spark Job Definition

1. In your Fabric workspace, create a new **Spark Job Definition** item.
1. Upload `incremental_clustering_benchmark.py` as the **main definition file**.
1. Set the **command line arguments** (see [Parameters](#parameters) for all options):
   ```
   --engine fabric --result-table "abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables/dbo/incremental_clustering_results" --benchmark-schema-name dbo --iterations 200 --batch-size 2500000 --optimize-write
   ```
   Add `--incremental-clustering` for the incremental run.
1. Attach the **lakehouse** that contains your result table and the **Environment** with the desired Spark pool.
1. Select **Run**. Monitor progress from the **Monitoring hub** or the job's **Run** tab.

### Option 2: Fabric notebook

1. Create a new notebook and upload `incremental_clustering_benchmark.py` to the notebook buil-in resources.
1. Attach the notebook to the **Environment** using the specified Spark Pool size and runtime.
1. Enter the following code and update parameters:

    ```python
    from builtin.incremental_clustering_benchmark import run_all
    
    run_all(
        spark,
        result_table_uri="abfss://...",
        engine="fabric",
        benchmark_schema_name="dbo",
        num_iterations=200,
        batch_size=2_500_000,
        optimize_write=True,
        incremental_clustering=True,
    )
    ```

## Parameters

| Parameter | Default | Description |
|---|---|---|
| `--result-table` | *(required)* | Delta table URI where results are appended |
| `--engine` | *(required)* | Engine identifier recorded in results (for example, `fabric`) |
| `--benchmark-schema-name` | `liquid` | Schema (database) namespace for benchmark tables |
| `--workload` | all | Run a single workload: `streaming`, `etl`, or `analytics` |
| `--iterations` | `200` | Write/optimize/query iterations per workload |
| `--batch-size` | `2,500,000` | Target rows per write batch |
| `--incremental-clustering` | off | Enable incremental clustering mode |
| `--optimize-write` | off | Enable Spark optimized writes |
| `--drop-table-post-run` | off | Drop each benchmark table after its run completes |

## Published results

The results referenced in Microsoft blog posts and documentation were produced with the configuration described in this section. To reproduce, match the compute environment and run both Spark Job Definitions.

### Compute environment

| Setting | Value |
|---|---|
| **Fabric capacity** | F16, or F2 with autoscale billing enabled (minimum 20 CUs available) |
| **Spark pool** | Fixed size, 5 Medium nodes (4 × 8-vCore workers + 1 driver) |
| **Spark runtime** | Fabric Runtime 2.0 |
| **Native Execution Engine** | Enabled |

### Spark Job Definition configuration

Two runs are required—one baseline and one with incremental clustering. Both write to the same result table for side-by-side comparison.

**Run 1 — Baseline** (full re-clustering on every OPTIMIZE):

```
--engine fabric --result-table "abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables/benchmark_results" --benchmark-schema-name benchmark --iterations 200 --batch-size 2500000 --optimize-write --drop-table-post-run
```

**Run 2 — Incremental clustering**:

```
--engine fabric --result-table "abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables/benchmark_results" --benchmark-schema-name benchmark --iterations 200 --batch-size 2500000 --optimize-write --incremental-clustering --drop-table-post-run
```

### Spark configuration applied by the script

The script sets these Spark configs programmatically at the start of each run. No manual configuration is required.

| Config key | Value |
|---|---|
| `spark.native.enabled` | `true` |
| `spark.microsoft.delta.optimize.clustering.strategy.incremental` | `true` (incremental) / `false` (baseline) |
| `spark.microsoft.delta.optimize.clustering.strategy.incremental.autoRecluster` | `true` |
| `spark.databricks.delta.optimizeWrite.enabled` | `true` (when `--optimize-write` is passed) / `false` |
| `spark.microsoft.delta.optimize.fast.enabled` | `true` |
| `spark.microsoft.delta.stats.collect.extended.property.setAtTableCreation` | `false` |

> [!NOTE]
> If you reproduce on a different capacity SKU or node size, expect proportional differences in absolute duration. The relative improvement of incremental vs. baseline clustering should remain consistent.

## Results schema

Results are appended to the Delta table specified by `--result-table`. Each row represents one phase (write, optimize, or query) of one iteration.

| Column | Type | Description |
|---|---|---|
| `iteration_id` | `STRING` | UUID for the full run of a single workload scenario |
| `engine` | `STRING` | Engine identifier |
| `batch_size_target` | `LONG` | Configured batch size |
| `iterations` | `LONG` | Total iterations configured |
| `scenario_name` | `STRING` | For example, `"Streaming Ingest - Incremental"` |
| `benchmark` | `STRING` | Workload name |
| `workload` | `STRING` | Workload name |
| `incremental_clustering_enabled` | `BOOLEAN` | Whether incremental mode was active |
| `iteration` | `LONG` | Iteration number (1-based) |
| `phase` | `STRING` | `write`, `optimize`, or `query` |
| `duration_ms` | `LONG` | Wall-clock duration of this phase |
| `active_file_count` | `LONG` | Number of active files after this phase |
| `query_file_count` | `LONG` | Files scanned by the query (query phase only) |
| `path` | `STRING` | Delta table path |
| `metrics` | `STRUCT` | Full OPTIMIZE metrics (optimize phase only) |

### Example analysis query

```sql
SELECT
    scenario_name,
    phase,
    AVG(duration_ms) AS avg_duration_ms,
    AVG(active_file_count) AS avg_files
FROM benchmark_results
WHERE iteration > 10  -- skip warmup
GROUP BY scenario_name, phase
ORDER BY scenario_name, phase
```

## Related content

- [Incremental liquid clustering in Microsoft Fabric](https://learn.microsoft.com/fabric/data-engineering/liquid-clustering?tabs=sparksql#incremental-liquid-clustering)