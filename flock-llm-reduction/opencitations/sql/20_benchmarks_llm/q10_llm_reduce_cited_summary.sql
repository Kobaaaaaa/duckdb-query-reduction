-- Using 'llm_reduce' to summarize the themes of cited papers for each citing paper
SELECT
  c.citing_doi,
  w1.title AS citing_title,
  llm_reduce(
    {'model_name':'gpt-4o'},
    {
      'prompt': 'Write ONE plain sentence (max 18 words) describing what the cited titles are mostly about.
                No markdown, no quotes, no colon, no bullets.',
      'context_columns': [
        {'data': w2.title, 'name':'cited_title'}
      ]
    }
  ) AS cited_theme_summary
FROM citations c
LEFT JOIN works w1 ON w1.doi = c.citing_doi
LEFT JOIN works w2 ON w2.doi = c.cited_doi
GROUP BY c.citing_doi, w1.title;