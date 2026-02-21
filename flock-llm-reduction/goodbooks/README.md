# goodbooks (DuckDB + Flock + Query Reduction)

This folder provides:
- DuckDB setup to load the Goodbooks CSV dataset
- SQL queries in two versions:
  - **LLM queries** using the **Flock** DuckDB extension (`llm_filter`, `llm_complete`, `llm_reduce`, …)
  - **Baseline queries** with the same logic but **without** LLM calls
- A Python tool to estimate how much data can be reduced before LLM evaluation:
  `scripts/reduction_analyzer.py` + unit tests

## Repository layout

- `data/`
  - `original_data/` — full CSV dataset (recommended to run the reduction script on this data)
  - `samples/` — optional smaller extracts
- `scripts/`
  - `reduction_analyzer.py`
  - `test_reduction_analyzer.py`
- `sql/`
  - `setup/load.sql` — installs/loads Flock + creates tables from CSVs
  - `llm_queries/` — LLM-enabled SQL queries
  - `baseline_queries/` — baseline equivalents (LLM calls removed, just for reference)

## Requirements

- DuckDB CLI
- Internet access to run `load.sql`
- Python 3.9+ (for analyzer + tests)

```bash
pip install duckdb pytest
```

## Flock secrets

Create a local (non-committed) file:
- `local/secrets.sql`

Example (adapt to your provider/config):

```sql
-- local/secrets.sql (DO NOT COMMIT)
CREATE SECRET (TYPE OPENAI, API_KEY 'sk-....');
```

## Load the dataset in DuckDB

Start DuckDB:
```bash
duckdb
```

In the DuckDB terminal, change to the goodbooks directory and load:
```sql
.cd 'path\to\goodbooks'
.read 'sql/setup/load.sql'
```

## Run queries (LLM vs baseline)

Run an LLM query:
```sql
.read 'sql/llm_queries/q01_popular_scifi_books.sql'
```

This might take a while to run depending on whether you run the query on the original data or on the sample data. You can change this in `load.sql`.

## Tuple reduction analyzer

Run on a single query (reads CSVs from `--data-dir`):
```bash
python scripts/reduction_analyzer.py sql/llm_queries/q01_popular_scifi_books.sql --data-dir data/original_data
```

### Running multiple queries at once

Each query's analysis is fully isolated, so the tables are reset to their original state between queries.

**Linux / macOS:**
```bash
python scripts/reduction_analyzer.py sql/llm_queries/*.sql --data-dir data/original_data
```

**Windows — PowerShell:**
```powershell
python scripts/reduction_analyzer.py (Get-ChildItem sql/llm_queries/*.sql) --data-dir data/original_data
```

## Tests

Use `python -m pytest` rather than `pytest` to avoid PATH issues on Windows.

**Quiet mode:**
```bash
python -m pytest -q scripts/test_reduction_analyzer.py
```

**Verbose mode:**
```bash
python -m pytest -v scripts/test_reduction_analyzer.py
```