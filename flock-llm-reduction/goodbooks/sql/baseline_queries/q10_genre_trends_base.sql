SELECT tags.tag_name
FROM books
JOIN book_tags ON books.goodreads_book_id = book_tags.goodreads_book_id
JOIN tags ON book_tags.tag_id = tags.tag_id
WHERE CAST(book_tags."count" AS INT) >= 1000
GROUP BY tags.tag_name