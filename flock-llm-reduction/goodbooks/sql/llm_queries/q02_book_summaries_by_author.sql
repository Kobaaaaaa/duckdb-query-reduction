SELECT authors,
  llm_reduce(
    {'model_name': 'gpt-4o'},
    {
      'prompt': 'Summarize the writing style and common themes of this author based on their book titles.',
      'context_columns': [
        {'data': regexp_replace(title, '[\x00-\x1F\x7F"]', ' ', 'g')}
      ]
    }
  ) AS author_summary
FROM books
WHERE authors IN (
  SELECT authors
  FROM books
  GROUP BY authors
  HAVING COUNT(*) <= 10
)
GROUP BY authors;