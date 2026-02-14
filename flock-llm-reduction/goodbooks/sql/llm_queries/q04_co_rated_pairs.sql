-- Pairs of books co-rated highly by the same user, with shared theme
SELECT pd.user_id, pd.book_a_id, pd.title_a, pd.book_b_id, pd.title_b,
  llm_complete(
    {'model_name': 'gpt-4o'},
    {
      'prompt': 'What is the likely shared theme between these two books? Answer in 10-15 words.',
      'context_columns': [
        {'data': pd.title_a, 'name': 'title_a'},
        {'data': pd.authors_a, 'name': 'authors_a'},
        {'data': pd.title_b, 'name': 'title_b'},
        {'data': pd.authors_b, 'name': 'authors_b'}
      ]
    }
  ) AS shared_theme
FROM (
  SELECT p.user_id, a.book_id AS book_a_id, a.title AS title_a, a.authors AS authors_a,
         b.book_id AS book_b_id, b.title AS title_b, b.authors AS authors_b
  FROM (
    SELECT r1.user_id, r1.book_id AS book_a, r2.book_id AS book_b
    FROM (SELECT user_id, book_id FROM ratings WHERE CAST(rating AS INTEGER) >= 4) AS r1
    JOIN (SELECT user_id, book_id FROM ratings WHERE CAST(rating AS INTEGER) >= 4) AS r2
      ON r1.user_id = r2.user_id AND r1.book_id < r2.book_id
  ) AS p
  JOIN books AS a ON a.book_id = p.book_a
  JOIN books AS b ON b.book_id = p.book_b
) AS pd
ORDER BY pd.user_id
LIMIT 40;
