-- Generate engaging reading recommendations for highly-rated books
SELECT book_id, title, authors, average_rating,
  llm_complete(
    {'model_name': 'gpt-4o'},
    {
      'prompt': 'Write a one-sentence hook that would make someone want to read this book.',
      'context_columns': [
        {'data': title, 'name': 'title'},
        {'data': authors, 'name': 'authors'}
      ]
    }
  ) AS reading_hook
FROM books
WHERE CAST(average_rating AS DECIMAL(3,2)) >= 4.3 AND CAST(ratings_count AS INTEGER) >= 50000
ORDER BY ratings_count DESC
LIMIT 30;