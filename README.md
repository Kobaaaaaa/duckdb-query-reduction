# duckdb-query-reduction

A repo exploring **data reduction in DuckDB** to minimize how much data needs **LLM evaluation**.

Includes a Goodbooks-10k demo (`flock-llm-reduction/goodbooks/`) with:
- DuckDB + CSV loading setup
- SQL queries in two forms:
  - **LLM-enabled** queries using the **Flock DuckDB extension** (`llm_filter`, `llm_complete`, `llm_reduce`, …)
  - **Baseline** equivalents without LLM calls
- A Python **reduction analyzer** (`scripts/reduction_analyzer.py`) with unit tests to estimate potential tuple/data reduction before LLM execution.
