-- Profile the reader community around each popular book via co-ratings
SELECT b1.title AS anchor_title, b1.authors,
  llm_reduce(
    {'model_name': 'gpt-4o'},
    {
      'prompt': 'Describe in one sentence the kind of reader who tends to love these books together.',
      'context_columns': [
        {'data': regexp_replace(b2.title,   '[\x00-\x1F\x7F"]', ' ', 'g'), 'name': 'companion_title'},
        {'data': regexp_replace(b2.authors, '[\x00-\x1F\x7F"]', ' ', 'g'), 'name': 'companion_authors'}
      ]
    }
  ) AS reader_profile
FROM ratings r1
JOIN ratings r2 ON r1.user_id = r2.user_id AND r1.book_id < r2.book_id
JOIN books b1   ON b1.book_id = r1.book_id
JOIN books b2   ON b2.book_id = r2.book_id
WHERE r1.rating::INTEGER          = 5
  AND r2.rating::INTEGER          = 5
  AND b1.ratings_count::INTEGER  >= 100000
  AND b2.ratings_count::INTEGER  >= 10000
GROUP BY b1.book_id, b1.title, b1.authors
HAVING COUNT(DISTINCT r1.user_id) >= 50
ORDER BY COUNT(DISTINCT r1.user_id) DESC