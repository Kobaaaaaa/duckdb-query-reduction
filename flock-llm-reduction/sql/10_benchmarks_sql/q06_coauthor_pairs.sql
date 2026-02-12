-- Top 20 co-author pairs (by number of shared works)
SELECT a1.author_name AS author_1, a2.author_name AS author_2, COUNT(*) AS n_shared_works
FROM work_authors wa1
JOIN work_authors wa2
  ON wa1.doi = wa2.doi
 AND wa1.author_id < wa2.author_id
JOIN authors a1 ON a1.author_id = wa1.author_id
JOIN authors a2 ON a2.author_id = wa2.author_id
GROUP BY a1.author_name, a2.author_name
ORDER BY n_shared_works DESC, author_1, author_2
LIMIT 20;