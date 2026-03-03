-- One-sentence reading hook for each highly-rated popular book
SELECT b.book_id, b.title, b.authors, b.average_rating,
  llm_complete(
    {'model_name': 'gpt-4o'},
    {
      'prompt': 'Write a one-sentence hook that would make someone want to read this book.',
      'context_columns': [
        {'data': b.title,   'name': 'title'},
        {'data': b.authors, 'name': 'authors'}
      ]
    }
  ) AS reading_hook
FROM books b
JOIN book_tags bt ON bt.goodreads_book_id = b.goodreads_book_id
JOIN tags t       ON t.tag_id = bt.tag_id
WHERE b.average_rating::DECIMAL(3,2) >= 4.3
  AND b.ratings_count::INTEGER >= 50000
  AND bt.count::INTEGER >= 1000
  AND lower(t.tag_name) LIKE '%fiction%'
ORDER BY b.ratings_count::INTEGER DESC