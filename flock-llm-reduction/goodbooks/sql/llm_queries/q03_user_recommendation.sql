-- Top 20 books with at least 100 ratings, with a note summarizing the rating information
SELECT x.book_id, x.title, x.authors, x.n_ratings, x.avg_user_rating,
  llm_complete(
    {'model_name': 'gpt-4o'},
    {
      'prompt': 'In one short sentence, explain what this rating summary suggests.',
      'context_columns': [
        {'data': x.title, 'name': 'title'},
        {'data': x.n_ratings::VARCHAR, 'name': 'n_ratings'},
        {'data': x.avg_user_rating::VARCHAR, 'name': 'avg_rating'}
      ]
    }
  ) AS note  
FROM (
  SELECT
    b.book_id,
    b.title,
    b.authors,
    count(*) AS n_ratings,
    avg(CAST(r.rating AS INTEGER)) AS avg_user_rating
  FROM ratings r
  JOIN books b ON b.book_id = r.book_id
  GROUP BY b.book_id, b.title, b.authors
  HAVING count(*) >= 100
) AS x -- x is the subquery that calculates the number of ratings and average rating for each book
ORDER BY x.n_ratings DESC
LIMIT 20;