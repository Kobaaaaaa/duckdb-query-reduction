-- Top 20 most cited works (by number of in-citations)
SELECT w.doi, w.title, COUNT(*) AS in_citations
FROM citations c
JOIN works w ON w.doi = c.cited_doi
GROUP BY w.doi, w.title
ORDER BY in_citations DESC, w.doi
LIMIT 20;