.read './toy_db.sql'

-- Benchmark query 2:
-- Find all edges where BOTH the source and destination nodes are in Canada, and return the labels.
DROP TABLE IF EXISTS res2_base;
CREATE TEMP TABLE res2_base AS
SELECT e.src, e.dst, s.label AS src_label, d.label AS dst_label
FROM edges e
JOIN nodes s ON s.id = e.src
JOIN nodes d ON d.id = e.dst
WHERE s.country = 'CA' AND d.country = 'CA';

SELECT * FROM res2_base ORDER BY src, dst;

-------------------
---- Reduction ----
-------------------

DROP TABLE IF EXISTS nodes2_reduced;
DROP TABLE IF EXISTS edges2_reduced;

-- Nodes in Canada (used for both src and dst)
DROP TABLE IF EXISTS ca_nodes2;
CREATE TEMP TABLE ca_nodes2 AS
SELECT id FROM nodes WHERE country = 'CA';

-- We only need edges where BOTH src and dst are in Canada
CREATE TABLE edges2_reduced AS
SELECT * FROM edges e
WHERE e.src IN (SELECT id FROM ca_nodes2)
  AND e.dst IN (SELECT id FROM ca_nodes2);

-- We only need nodes referenced by those edges
CREATE TABLE nodes2_reduced AS
SELECT * FROM nodes n
WHERE n.id IN (SELECT src FROM edges2_reduced UNION SELECT dst FROM edges2_reduced);

-- Show the reduction
SELECT 
  (SELECT COUNT(*) FROM nodes) AS original_node_count,
  (SELECT COUNT(*) FROM nodes2_reduced) AS reduced_node_count,
  (SELECT COUNT(*) FROM edges) AS original_edge_count,
  (SELECT COUNT(*) FROM edges2_reduced) AS reduced_edge_count;

-- We run the benchmark query again on the reduced dataset
DROP TABLE IF EXISTS res2_reduced;
CREATE TEMP TABLE res2_reduced AS
SELECT e.src, e.dst, s.label AS src_label, d.label AS dst_label
FROM edges2_reduced e
JOIN nodes2_reduced s ON s.id = e.src
JOIN nodes2_reduced d ON d.id = e.dst
WHERE s.country = 'CA' AND d.country = 'CA';

SELECT * FROM res2_reduced ORDER BY src, dst;

-- Final check to see if both results are the same (should return 0 rows if they are the same)
SELECT * FROM (SELECT * FROM res2_base EXCEPT SELECT * FROM res2_reduced)
UNION ALL
SELECT * FROM (SELECT * FROM res2_reduced EXCEPT SELECT * FROM res2_base);
