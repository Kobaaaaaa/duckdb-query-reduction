-- Rank candidate books from mystery-related tags with llm_rerank
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
  SELECT b.book_id, b.title, b.authors, b.average_rating, b.ratings_count
  FROM books b
  JOIN book_tags bt ON bt.goodreads_book_id = b.goodreads_book_id
  JOIN tags t ON t.tag_id = bt.tag_id
  WHERE llm_filter(
    {'model_name': 'gpt-4o'},
    {
      'prompt': 'From this tag alone: does it sound like this book is a mystery/crime/thriller?',
      'context_columns': [{'name': 'tag_name', 'data': t.tag_name}]
    }
  )
  ORDER BY b.ratings_count DESC
  LIMIT 60
) candidates;
