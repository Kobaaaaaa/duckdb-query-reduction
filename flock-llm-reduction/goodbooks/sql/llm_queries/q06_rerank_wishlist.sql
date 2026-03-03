-- Rank wishlisted fiction books as best next read using engagement metadata
SELECT llm_rerank(
  {'model_name': 'gpt-4o'},
  {
    'prompt': 'Rank these books as the most rewarding next read using the quality and engagement signals.',
    'context_columns': [
      {'data': title,                            'name': 'title'},
      {'data': authors,                          'name': 'authors'},
      {'data': average_rating::VARCHAR,          'name': 'avg_rating'},
      {'data': ratings_count::VARCHAR,           'name': 'total_ratings'},
      {'data': ratings_5::VARCHAR,               'name': 'five_star_votes'},
      {'data': ratings_1::VARCHAR,               'name': 'one_star_votes'},
      {'data': work_text_reviews_count::VARCHAR, 'name': 'review_count'},
      {'data': books_count::VARCHAR,             'name': 'editions_count'}
    ]
  }
) AS reranked
FROM (
  SELECT DISTINCT b.book_id, b.title, b.authors, b.average_rating,
    b.ratings_count, b.ratings_5, b.ratings_1,
    b.work_text_reviews_count, b.books_count
  FROM to_read tr
  JOIN books b      ON b.book_id            = tr.book_id
  JOIN book_tags bt ON bt.goodreads_book_id = b.goodreads_book_id
  JOIN tags t       ON t.tag_id             = bt.tag_id
  WHERE b.average_rating::DECIMAL(3,2) >= 4.2
    AND b.ratings_count::INTEGER        >= 50000
    AND bt.count::INTEGER               >= 5000
    AND lower(t.tag_name) LIKE '%fiction%'
  ORDER BY b.ratings_count::BIGINT DESC
) candidates