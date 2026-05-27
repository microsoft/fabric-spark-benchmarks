# Fabric Spark Benchmarks

Reproducible benchmarks for [Microsoft Fabric](https://learn.microsoft.com/fabric/) Spark workloads. Each benchmark is a self-contained script with documentation, methodology, and instructions to reproduce published results.

## Benchmarks

| Benchmark | Description | Blog Link |
|---|---|---|
| [Incremental Clustering](benchmarks/liquid-clustering/incremental-clustering/README.md) | Measures clustering performance across streaming, ETL, and analytics workloads with Incremental Liquid Clustering in Microsoft Fabric compared to the baseline Liquid Clustering algorithm from OSS `delta-spark`. | [Incremental Liquid Clustering in Microsoft Fabric: Faster, smarter, and truly incremental](https://community.fabric.microsoft.com/t5/Fabric-Updates-Blog/Incremental-Liquid-Clustering-in-Microsoft-Fabric-Faster-smarter/ba-p/5189122) |

## Quick start

1. Clone this repo.
2. Navigate to a benchmark directory (for example, `benchmarks/liquid-clustering/incremental-clustering/`).
3. Follow the benchmark's `README.md` for prerequisites and run instructions.

Each benchmark can be run via a Spark Job or imported into a Fabric notebook.

## Repository structure

```
benchmarks/
├── liquid-clustering/             # Benchmark category
│   ├── incremental-clustering/    # One directory per benchmark
│   │   ├── README.md              # Methodology, parameters, how to reproduce
│   │   ├── *.py                   # Runnable benchmark script
│   │   └── notebooks/             # Optional Fabric notebook exports
```

## License

[MIT](LICENSE) — Copyright (c) Microsoft Corporation.