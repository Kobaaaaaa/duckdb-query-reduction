SELECT authors
FROM books
WHERE authors IN (
  SELECT authors
  FROM books
  GROUP BY authors
  HAVING COUNT(*) <= 10
)
GROUP BY authors;