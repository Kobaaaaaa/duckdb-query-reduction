-- Rank mystery/thriller books by reader satisfaction using quality signals
SELECT llm_rerank(
  {'model_name': 'gpt-4o'},
  {
    'prompt': 'Rank these books by overall reader satisfaction using the provided metrics.',
    'context_columns': [
      {'data': title,                            'name': 'title'},
      {'data': authors,                          'name': 'authors'},
      {'data': average_rating::VARCHAR,          'name': 'avg_rating'},
      {'data': ratings_count::VARCHAR,           'name': 'total_ratings'},
      {'data': ratings_5::VARCHAR,               'name': 'five_star_votes'},
      {'data': ratings_1::VARCHAR,               'name': 'one_star_votes'},
      {'data': work_text_reviews_count::VARCHAR, 'name': 'review_count'}
    ]
  }
) AS reranked
FROM (
  SELECT DISTINCT b.book_id, b.title, b.authors, b.average_rating,
    b.ratings_count, b.ratings_5, b.ratings_1, b.work_text_reviews_count
  FROM books b
  JOIN book_tags bt ON bt.goodreads_book_id = b.goodreads_book_id
  JOIN tags t       ON t.tag_id = bt.tag_id
  WHERE (lower(t.tag_name) LIKE '%mystery%' OR lower(t.tag_name) LIKE '%thriller%')
    AND b.ratings_count::INTEGER >= 10000
    AND bt.count::INTEGER        >= 500
  ORDER BY b.ratings_count::BIGINT DESC
) candidates