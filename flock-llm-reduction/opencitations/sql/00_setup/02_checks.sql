-- Checks if works have multiple authors
SELECT doi, COUNT(*) AS n_authors
FROM work_authors
GROUP BY doi
HAVING COUNT(*) > 1
ORDER BY n_authors DESC, doi;

-- Checks if there are citations where the citing or cited DOI is missing from the works table
SELECT
  SUM(CASE WHEN w1.doi IS NULL THEN 1 ELSE 0 END) AS missing_citing,
  SUM(CASE WHEN w2.doi IS NULL THEN 1 ELSE 0 END) AS missing_cited
FROM citations c
LEFT JOIN works w1 ON w1.doi = c.citing_doi
LEFT JOIN works w2 ON w2.doi = c.cited_doi;
