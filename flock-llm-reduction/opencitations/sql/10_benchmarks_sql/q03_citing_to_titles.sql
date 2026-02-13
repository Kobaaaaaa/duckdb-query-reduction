-- DOIs and titles of citing and cited works
SELECT 
  c.citing_doi, w1.title AS citing_title,
  c.cited_doi, w2.title AS cited_title
FROM citations c
JOIN works w1 ON w1.doi = c.citing_doi
JOIN works w2 ON w2.doi = c.cited_doi
ORDER BY c.citing_doi, c.cited_doi;