-- Generate engaging reading recommendations for highly-rated books
SELECT book_id, title, authors, average_rating
FROM books
WHERE CAST(average_rating AS DECIMAL(3,2)) >= 4.3 AND CAST(ratings_count AS INTEGER) >= 50000
ORDER BY ratings_count DESC