## DuckDB table reduction baseline (toy database)

Run in DuckDB:
- `.read q1_reduce.sql`
- `.read q2_reduce.sql`
- `.read q3_having.sql`

Each script:
1) loads toy_db.sql
2) runs baseline query
3) creates reduced tables
4) reruns query on the reduced tables
5) checks equality (final check outputs 0 rows)
