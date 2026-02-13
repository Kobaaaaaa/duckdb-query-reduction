.read sql/00_setup/01_load.sql

.output results/sql/q01_top_cited.txt
.read sql/10_benchmarks_sql/q01_top_cited.sql
.output

.output results/sql/q02_top_citing.txt
.read sql/10_benchmarks_sql/q02_top_citing.sql
.output

.output results/sql/q03_citing_to_titles.txt
.read sql/10_benchmarks_sql/q03_citing_to_titles.sql
.output

.output results/sql/q04_citations_by_cited_publisher.txt
.read sql/10_benchmarks_sql/q04_citations_by_cited_publisher.sql
.output

.output results/sql/q05_top_authors_by_in_citations.txt
.read sql/10_benchmarks_sql/q05_top_authors_by_in_citations.sql
.output

.output results/sql/q06_coauthor_pairs.txt
.read sql/10_benchmarks_sql/q06_coauthor_pairs.sql
.output

.output results/sql/q07_special_citations.txt
.read sql/10_benchmarks_sql/q07_special_citations.sql
.output