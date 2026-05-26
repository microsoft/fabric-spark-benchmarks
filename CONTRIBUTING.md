# Contributing to Fabric Spark Benchmarks

Thank you for your interest in contributing benchmarks! This guide covers how to add a new benchmark and the standards each benchmark should follow.

## Adding a new benchmark

1. **Copy the template**: Copy `benchmarks/_template/` to `benchmarks/<your-benchmark-name>/`.
2. **Implement**: Write your benchmark script following the patterns in the template.
3. **Document**: Fill in all sections of the `README.md` template — methodology is the most important part for reproducibility.
4. **Register**: Add a row to the benchmarks table in the root `README.md`.

## Naming conventions

| Item | Convention | Example |
|---|---|---|
| Benchmark directory | `kebab-case` | `incremental-clustering` |
| Python files | `snake_case` | `incremental_clustering_benchmark.py` |
| CLI parameters | `--kebab-case` | `--benchmark-schema-name` |
| Result table columns | `snake_case` | `active_file_count` |

## Benchmark README requirements

Every benchmark must have a `README.md` with these sections:

- **Overview** — What is being measured and why.
- **Methodology** — Data generation, workload patterns, what is timed vs. excluded.
- **Prerequisites** — Fabric workspace setup, Spark pool config, library versions.
- **How to reproduce** — Both `spark-submit` and Fabric notebook options.
- **Parameters** — Table of all CLI arguments with defaults and descriptions.
- **Results schema** — Description of the output Delta table and how to query it.
- **Published results** — Exact compute environment (capacity SKU, pool config, runtime, node size), CLI commands, and Spark config used to produce any results referenced in blog posts or docs.
- **Related content** — Links to the Fabric docs or blog posts this benchmark supports.

## Design principles

- **Self-contained**: Each benchmark directory should be independently runnable. A user should be able to clone the repo, `cd` into the benchmark, and run it.
- **No unnecessary dependencies**: Use raw PySpark for Fabric-specific benchmarks. Only depend on LakeBench when multi-engine comparison is the goal.
- **Deterministic**: Use seeded random generation so results are reproducible across runs.
- **Document assumptions**: Every shortcut or simplification should be called out in the methodology.

## Code style

- Type hints on public function signatures.
- Docstrings on all public functions.
- Use keyword-only arguments (`*`) for functions with more than 3 optional parameters.
- Use `logging` (not `print`) for status output.
- Use `argparse` for CLI entry points.

## Pull request checklist

- [ ] Benchmark runs end-to-end on a Fabric workspace.
- [ ] `README.md` has all required sections filled in.
- [ ] Root `README.md` updated with the new benchmark.
- [ ] No secrets, workspace IDs, or personal paths in committed code.
