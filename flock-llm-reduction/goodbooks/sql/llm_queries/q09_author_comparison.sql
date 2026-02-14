-- Compare popular authors in terms of their writing styles
SELECT comp.author1, comp.author2, comp.avg_rating1, comp.avg_rating2,
  llm_complete(
    {'model_name': 'gpt-4o'},
    {
      'prompt': 'In 15-20 words, compare the writing styles of these two authors.',
      'context_columns': [
        {'data': comp.author1, 'name': 'author1'},
        {'data': comp.author2, 'name': 'author2'},
        {'data': comp.titles1, 'name': 'sample_books1'},
        {'data': comp.titles2, 'name': 'sample_books2'}
      ]
    }
  ) AS style_comparison
FROM (
  SELECT a1.authors AS author1, a2.authors AS author2,
    AVG(CAST(a1.average_rating AS DECIMAL(3,2))) AS avg_rating1,
    AVG(CAST(a2.average_rating AS DECIMAL(3,2))) AS avg_rating2,
    STRING_AGG(a1.title, '; ') AS titles1,
    STRING_AGG(a2.title, '; ') AS titles2
  FROM (SELECT * FROM books WHERE CAST(ratings_count AS INTEGER) >= 10000) a1
  CROSS JOIN (SELECT * FROM books WHERE CAST(ratings_count AS INTEGER) >= 10000) a2
  WHERE a1.authors < a2.authors
  GROUP BY a1.authors, a2.authors
  HAVING COUNT(DISTINCT a1.book_id) >= 2 AND COUNT(DISTINCT a2.book_id) >= 2
) comp
ORDER BY (comp.avg_rating1 + comp.avg_rating2) DESC