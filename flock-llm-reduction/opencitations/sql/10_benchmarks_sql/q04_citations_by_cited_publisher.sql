-- Top cited publishers (by number of in-citations)
SELECT w.publisher, COUNT(*) AS citations_to_publisher
FROM citations c
JOIN works w ON w.doi = c.cited_doi
GROUP BY w.publisher
ORDER BY citations_to_publisher DESC, w.publisher;