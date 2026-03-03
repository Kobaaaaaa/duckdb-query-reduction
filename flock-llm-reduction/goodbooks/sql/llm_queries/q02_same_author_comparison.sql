-- Compare two books by the same author based on their ratings profile
SELECT b1.authors, b1.title AS title_a, b2.title AS title_b,
  llm_complete(
    {'model_name': 'gpt-4o'},
    {
      'prompt': 'In one sentence, describe how these two books by the same author differ in tone or audience.',
      'context_columns': [
        {'data': b1.title,                            'name': 'title_a'},
        {'data': b1.average_rating::VARCHAR,          'name': 'avg_rating_a'},
        {'data': b1.ratings_count::VARCHAR,           'name': 'ratings_a'},
        {'data': b2.title,                            'name': 'title_b'},
        {'data': b2.average_rating::VARCHAR,          'name': 'avg_rating_b'},
        {'data': b2.ratings_count::VARCHAR,           'name': 'ratings_b'},
        {'data': b1.authors,                          'name': 'authors'}
      ]
    }
  ) AS comparison
FROM books b1
JOIN books b2
  ON  b1.authors = b2.authors
  AND b1.book_id < b2.book_id
WHERE b1.ratings_count::INTEGER >= 50000
  AND b2.ratings_count::INTEGER >= 50000
ORDER BY b1.authors, b1.title