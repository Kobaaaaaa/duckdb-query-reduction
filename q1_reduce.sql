.read './toy_db.sql'

-- Benchmark query 1: 
-- Find all edges where the destination node is in Canada, and return the source and destination labels.
DROP TABLE IF EXISTS res_base;
CREATE TEMP TABLE res_base AS
SELECT e.src, e.dst, s.label AS src_label, d.label AS dst_label
FROM edges e
JOIN nodes s ON s.id = e.src
JOIN nodes d ON d.id = e.dst
WHERE d.country = 'CA';

SELECT * FROM res_base ORDER BY src, dst;

-------------------
---- Reduction ----
-------------------

DROP TABLE IF EXISTS nodes_reduced;
DROP TABLE IF EXISTS edges_reduced;

-- Destination nodes in Canada
DROP TABLE IF EXISTS ca_nodes;
CREATE TEMP TABLE ca_nodes AS
SELECT id FROM nodes WHERE country = 'CA';

-- We only need edges where the destination is in Canada
CREATE TABLE edges_reduced AS
SELECT * FROM edges e
WHERE e.dst IN (SELECT id FROM ca_nodes);

-- We only need source nodes that have edges to Canada
CREATE TABLE nodes_reduced AS
SELECT * FROM nodes n
WHERE n.id IN (SELECT src FROM edges_reduced UNION SELECT dst FROM edges_reduced);

-- Show the reduction
SELECT 
  (SELECT COUNT(*) FROM nodes) AS original_node_count,
  (SELECT COUNT(*) FROM nodes_reduced) AS reduced_node_count,
  (SELECT COUNT(*) FROM edges) AS original_edge_count,
  (SELECT COUNT(*) FROM edges_reduced) AS reduced_edge_count;

-- We run the benchmark query again on the reduced dataset
DROP TABLE IF EXISTS res_reduced;
CREATE TEMP TABLE res_reduced AS
SELECT e.src, e.dst, s.label AS src_label, d.label AS dst_label
FROM edges_reduced e
JOIN nodes_reduced s ON s.id = e.src
JOIN nodes_reduced d ON d.id = e.dst
WHERE d.country = 'CA';

SELECT * FROM res_reduced ORDER BY src, dst;

-- Final check to see if both results are the same (should return 0 rows if they are the same)
SELECT * FROM (SELECT * FROM res_base EXCEPT SELECT * FROM res_reduced)
UNION ALL
SELECT * FROM (SELECT * FROM res_reduced EXCEPT SELECT * FROM res_base);
