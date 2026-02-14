-- Find books that readers engaged with, enhanced with emotional analysis
SELECT b.book_id, b.title, b.authors, COUNT(*) AS num_ratings, AVG(CAST(r.rating AS INTEGER)) AS avg_rating
FROM books b
JOIN ratings r ON b.book_id = r.book_id
GROUP BY b.book_id, b.title, b.authors
HAVING COUNT(*) >= 2
ORDER BY avg_rating DESC, num_ratings DESC