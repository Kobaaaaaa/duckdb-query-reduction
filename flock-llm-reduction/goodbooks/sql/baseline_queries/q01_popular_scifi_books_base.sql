-- Select books with an average rating of 4.0 or higher, keeping only the sci-fi themed ones  
SELECT book_id, title, authors, average_rating
FROM books
WHERE CAST(average_rating AS DECIMAL(2,1)) >= 4.0