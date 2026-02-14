-- Analyze books by their associated tags and identify patterns
SELECT t.tag_name, COUNT(DISTINCT b.book_id) AS num_books, AVG(CAST(b.average_rating AS DECIMAL(3,2))) AS avg_rating,
  llm_reduce(
    {'model_name': 'gpt-4o'},
    {
      'prompt': 'Based on these book titles, what type of content does this tag represent?',
      'context_columns': [{'data': b.title}]
    }
  ) AS tag_theme
FROM books b
JOIN book_tags bt ON b.goodreads_book_id = bt.goodreads_book_id
JOIN tags t ON bt.tag_id = t.tag_id
WHERE CAST(bt.count AS INTEGER) >= 1000
GROUP BY t.tag_name
HAVING COUNT(DISTINCT b.book_id) >= 2
ORDER BY num_books DESC