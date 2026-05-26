# Copilot Instructions — fabric-spark-benchmarks

## What is this repo?

A collection of **self-contained, reproducible benchmarks** for Microsoft Fabric Spark workloads. Each benchmark lives in its own directory under `benchmarks/` with a script, README, and optional notebook.

## Project layout

```
benchmarks/
├── incremental-clustering/    # Incremental vs. baseline clustering OPTIMIZE
│   ├── README.md
│   ├── incremental_clustering_benchmark.py
│   └── notebooks/
├── _template/                 # Scaffold for new benchmarks
shared/                        # Common utilities (when needed)
docs/                          # Cross-cutting methodology
```

## Conventions

- Each benchmark is standalone — no shared package install required.
- Use `argparse` for CLI, `logging` for output, type hints on public APIs.
- Use keyword-only args (`*`) for functions with many optional parameters.
- Benchmark tables use a configurable schema name (`--benchmark-schema-name`).
- Results are written to a Delta table specified by `--result-table`.
- Follow the README template in `benchmarks/_template/README.md` for documentation.

## Key files

- `CONTRIBUTING.md` — How to add new benchmarks, naming conventions, PR checklist.
- Each benchmark's `README.md` — Methodology, parameters, reproduction steps.
