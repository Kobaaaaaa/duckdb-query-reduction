-- Pairs of books co-rated highly by the same user, with shared theme
SELECT pd.user_id, pd.book_a_id, pd.title_a, pd.book_b_id, pd.title_b
FROM (
  SELECT
    p.user_id,

    a.book_id AS book_a_id,
    regexp_replace(a.title,   '[\x00-\x1F\x7F"]', ' ', 'g') AS title_a,
    regexp_replace(a.authors, '[\x00-\x1F\x7F"]', ' ', 'g') AS authors_a,

    b.book_id AS book_b_id,
    regexp_replace(b.title,   '[\x00-\x1F\x7F"]', ' ', 'g') AS title_b,
    regexp_replace(b.authors, '[\x00-\x1F\x7F"]', ' ', 'g') AS authors_b
  FROM (
    SELECT
      r1.user_id,
      r1.book_id AS book_a,
      r2.book_id AS book_b
    FROM ratings r1
    JOIN ratings r2
      ON r1.user_id = r2.user_id
     AND r1.book_id < r2.book_id
    WHERE CAST(r1.rating AS INT) >= 4
      AND CAST(r2.rating AS INT) >= 4
    ORDER BY r1.user_id, r1.book_id, r2.book_id
    LIMIT 500
  ) p
  JOIN books a ON a.book_id = p.book_a
  JOIN books b ON b.book_id = p.book_b
) pd
ORDER BY pd.user_id;