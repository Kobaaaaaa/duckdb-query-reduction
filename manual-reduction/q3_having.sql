.read './toy_db.sql'

-- Benchmark query 3 (GROUP BY + HAVING):
-- Count edges going to Canada per source country, keep only countries with >= 2 such edges.
DROP TABLE IF EXISTS res3_base;
CREATE TEMP TABLE res3_base AS
SELECT s.country AS src_country, COUNT(*) AS n_edges_to_ca
FROM edges e
JOIN nodes s ON s.id = e.src
JOIN nodes d ON d.id = e.dst
WHERE d.country = 'CA'
GROUP BY s.country
HAVING COUNT(*) >= 2
ORDER BY src_country;

SELECT * FROM res3_base;

-------------------
---- Reduction ----
-------------------

-- Nodes in Canada (dst-side)
DROP TABLE IF EXISTS ca_nodes3;
CREATE TEMP TABLE ca_nodes3 AS
SELECT id FROM nodes WHERE country = 'CA';

-- Having part, we need to know which source countries have >= 2 edges to Canada
DROP TABLE IF EXISTS good_src_countries;
CREATE TEMP TABLE good_src_countries AS
SELECT s.country AS src_country
FROM edges e
JOIN nodes s ON s.id = e.src
WHERE e.dst IN (SELECT id FROM ca_nodes3)
GROUP BY s.country
HAVING COUNT(*) >= 2;

-- Reduce edge to only those that go to Canada and have a source in a 'good country'
DROP TABLE IF EXISTS edges3_reduced;
CREATE TABLE edges3_reduced AS
SELECT e.*
FROM edges e
JOIN nodes s ON s.id = e.src
WHERE e.dst IN (SELECT id FROM ca_nodes3)
  AND s.country IN (SELECT src_country FROM good_src_countries);

-- Reduce nodes to only those that are referenced by the reduced edges
DROP TABLE IF EXISTS nodes3_reduced;
CREATE TABLE nodes3_reduced AS
SELECT *
FROM nodes
WHERE id IN (SELECT src FROM edges3_reduced UNION SELECT dst FROM edges3_reduced);

-- Show the reduction
SELECT 
  (SELECT COUNT(*) FROM nodes) AS original_node_count,
  (SELECT COUNT(*) FROM nodes3_reduced) AS reduced_node_count,
  (SELECT COUNT(*) FROM edges) AS original_edge_count,
  (SELECT COUNT(*) FROM edges3_reduced) AS reduced_edge_count;

-- Rerun the SAME query with minimal changes (swap table names)
DROP TABLE IF EXISTS res3_reduced;
CREATE TEMP TABLE res3_reduced AS
SELECT s.country AS src_country, COUNT(*) AS n_edges_to_ca
FROM edges3_reduced e
JOIN nodes3_reduced s ON s.id = e.src
JOIN nodes3_reduced d ON d.id = e.dst
WHERE d.country = 'CA'
GROUP BY s.country
HAVING COUNT(*) >= 2
ORDER BY src_country;

SELECT * FROM res3_reduced;

-- Final check to see if both results are the same (should return 0 rows if they are the same)
SELECT * FROM (SELECT * FROM res3_base EXCEPT SELECT * FROM res3_reduced)
UNION ALL
SELECT * FROM (SELECT * FROM res3_reduced EXCEPT SELECT * FROM res3_base);
