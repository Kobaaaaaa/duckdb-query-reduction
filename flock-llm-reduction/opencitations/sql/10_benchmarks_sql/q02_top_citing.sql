-- Top 20 most citing works (by number of out-citations)
SELECT w.doi, w.title, COUNT(*) AS out_citations
FROM citations c
JOIN works w ON w.doi = c.citing_doi
GROUP BY w.doi, w.title
ORDER BY out_citations DESC, w.doi
LIMIT 20;