-- Summarise what books under each popular tag have in common
SELECT t.tag_name, COUNT(DISTINCT b.book_id) AS num_books,
  llm_reduce(
    {'model_name': 'gpt-4o'},
    {
      'prompt': 'In one sentence, describe what these books have in common.',
      'context_columns': [
        {'data': regexp_replace(b.title,   '[\x00-\x1F\x7F"]', ' ', 'g'), 'name': 'title'},
        {'data': regexp_replace(b.authors, '[\x00-\x1F\x7F"]', ' ', 'g'), 'name': 'authors'}
      ]
    }
  ) AS common_theme
FROM tags t
JOIN book_tags bt ON bt.tag_id           = t.tag_id
JOIN books b      ON b.goodreads_book_id = bt.goodreads_book_id
WHERE bt.count::INTEGER          >= 1000
  AND b.ratings_count::INTEGER   >= 10000
  AND b.average_rating::DECIMAL(3,2) >= 4.0
GROUP BY t.tag_name
HAVING COUNT(DISTINCT b.book_id) >= 5
ORDER BY num_books DESC