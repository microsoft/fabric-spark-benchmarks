# Fabric Spark Benchmarks

Reproducible benchmarks for [Microsoft Fabric](https://learn.microsoft.com/fabric/) Spark workloads. Each benchmark is a self-contained script with documentation, methodology, and instructions to reproduce published results.

## Benchmarks

| Benchmark | Description | Status |
|---|---|---|
| [Incremental Clustering](benchmarks/incremental-clustering/README.md) | Measures OPTIMIZE performance across streaming, ETL, and analytics workloads with incremental vs. baseline clustering | ✅ Ready |

## Quick start

1. Clone this repo.
2. Navigate to a benchmark directory (for example, `benchmarks/incremental-clustering/`).
3. Follow the benchmark's `README.md` for prerequisites and run instructions.

Each benchmark can be run via `spark-submit` or imported into a Fabric notebook.

## Repository structure

```
benchmarks/
├── incremental-clustering/    # One directory per benchmark
│   ├── README.md              # Methodology, parameters, how to reproduce
│   ├── *.py                   # Runnable benchmark script
│   └── notebooks/             # Optional Fabric notebook exports
├── _template/                 # Skeleton for new benchmarks
shared/                        # Common utilities (as needed)
docs/                          # Cross-cutting methodology notes
```

## Adding a new benchmark

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on adding benchmarks to this repo.

## LakeBench

Some benchmarks use [LakeBench](https://github.com/microsoft/LakeBench) as their execution framework for multi-engine comparisons. When a benchmark depends on LakeBench, it declares `pip install lakebench[...]` in its own README. Benchmarks that are Fabric-Spark-specific (like incremental clustering) use raw PySpark with no external framework.

## License

[MIT](LICENSE) — Copyright (c) Microsoft Corporation.