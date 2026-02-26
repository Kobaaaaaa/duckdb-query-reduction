-- Rerank top-rated wishlisted books for book club picks
SELECT llm_rerank(
  {'model_name': 'gpt-4o'},
  {
    'prompt': 'Best books for a book club',
    'context_columns': [
      {'data': b.title,   'name': 'title'},
      {'data': b.authors, 'name': 'authors'}
    ]
  }
) AS reranked
FROM to_read tr
JOIN books b      ON b.book_id = tr.book_id
JOIN ratings r    ON r.book_id = b.book_id
JOIN book_tags bt ON bt.goodreads_book_id = b.goodreads_book_id
JOIN tags t       ON t.tag_id = bt.tag_id
WHERE CAST(r.rating AS INTEGER) = 5
  AND CAST(b.average_rating AS DECIMAL(3,2)) >= 4.5
  AND CAST(b.ratings_count AS INTEGER) >= 100000
  AND CAST(bt.count AS INTEGER) >= 10000
  AND lower(t.tag_name) LIKE '%fiction%';