# duckdb-query-reduction

A repo for experimenting with **query-time reduction** in DuckDB + Flock (so that fewer LLM calls are made).

## How it’s organized

- `flock-llm-reduction/<dataset>/`
  - `data/original_data/` full data
  - `data/samples/` tiny data for visualization and testing
  - `sql/setup/load.sql` loads tables (+ Flock + secrets)
  - `sql/llm_queries/` and/or `sql/baseline_queries/`
- `flock-llm-reduction/tools/`
  - `reduction_analyzer.py` estimates how much you can reduce before LLM evaluation
  - `test_reduction_analyzer.py`

## Quick start (any dataset)

1) Create secrets file (don’t commit):
- `flock-llm-reduction/<dataset>/local/secrets.sql`
```sql
CREATE SECRET (TYPE OPENAI, API_KEY 'sk-...');
```
The type doesn't have to be OPENAI, you can choose from the available ones in the Flock documentation.

1) Load in DuckDB:
```sql
duckdb
.cd '.../duckdb-query-reduction/flock-llm-reduction/<dataset>'
.read 'sql/setup/load.sql'
```

1) Run a query:
```sql
.read 'sql/llm_queries/<query>.sql'
-- or
.read 'sql/baseline_queries/<query>.sql'
```

## Reduction analyzer

Each query's analysis is fully isolated, so the tables are reset to their original state between queries.

Linux / macOS:
```powershell
python scripts/reduction_analyzer.py sql/llm_queries/*.sql --data-dir data/original_data
```

Windows — PowerShell:
```powershell
python scripts/reduction_analyzer.py (Get-ChildItem sql/llm_queries/*.sql) --data-dir data/original_data
```

## Tests

```powershell
cd flock-llm-reduction\<dataset>
python -m pytest -q ..\tools\test_reduction_analyzer.py
```
