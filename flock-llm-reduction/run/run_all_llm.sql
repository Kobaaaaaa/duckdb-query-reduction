INSTALL flock FROM community;
LOAD flock;

.read '.\local\secrets.sql'

.read sql/00_setup/01_load.sql
.read sql/00_setup/02_views.sql
.read sql/00_setup/03_checks.sql

.output results/llm/q08_llm_topic_tag.txt
.read sql/20_benchmarks_llm/q08_llm_topic_tag.sql
.output

.output results/llm/q09_llm_filter_bibliometrics.txt
.read sql/20_benchmarks_llm/q09_llm_filter_bibliometrics.sql
.output

.output results/llm/q10_llm_reduce_cited_summary.txt
.read sql/20_benchmarks_llm/q10_llm_reduce_cited_summary.sql
.output