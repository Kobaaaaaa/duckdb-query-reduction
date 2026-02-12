-- Top 20 authors with the most in-citations (citations received)
SELECT a.author_id, a.author_name, COUNT(*) AS citations_received
FROM citations c
JOIN work_authors wa ON wa.doi = c.cited_doi
JOIN authors a ON a.author_id = wa.author_id
GROUP BY a.author_id, a.author_name
ORDER BY citations_received DESC, a.author_id
LIMIT 20;