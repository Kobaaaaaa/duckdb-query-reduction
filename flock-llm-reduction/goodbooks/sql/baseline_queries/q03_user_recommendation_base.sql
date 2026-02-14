SELECT x.book_id, x.title, x.authors, x.n_ratings, x.avg_user_rating
FROM (
  SELECT b.book_id, b.title, b.authors,
    count(*) AS n_ratings,
    avg(CAST(r.rating AS INTEGER)) AS avg_user_rating
  FROM ratings r
  JOIN books b ON b.book_id = r.book_id
  GROUP BY b.book_id, b.title, b.authors
  HAVING count(*) >= 100
) AS x -- x is the subquery that calculates the number of ratings and average rating for each book
ORDER BY x.n_ratings DESC