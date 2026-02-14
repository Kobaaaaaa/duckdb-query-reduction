SELECT llm_rerank(
  {'model_name': 'gpt-4o'},
  {
    'prompt': 'Mystery books that are fun, easy to read, and not very violent',
    'context_columns': [
      {'data': title, 'name': 'title'},
      {'data': authors, 'name': 'authors'}
    ]
  }
) AS reranked
FROM (
  SELECT DISTINCT
    b.book_id, b.title, b.authors, b.average_rating, b.ratings_count
  FROM books b
  JOIN book_tags bt ON bt.goodreads_book_id = b.goodreads_book_id
  JOIN tags t ON t.tag_id = bt.tag_id
  WHERE lower(t.tag_name) LIKE '%mystery%'
     OR lower(t.tag_name) LIKE '%thriller%'
     OR lower(t.tag_name) LIKE '%crime%'
     OR lower(t.tag_name) LIKE '%detective%'
  ORDER BY CAST(b.ratings_count AS BIGINT) DESC
) candidates;