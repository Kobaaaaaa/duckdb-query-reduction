-- Using 'llm_reduce' to summarize the themes of cited papers for each citing paper
SELECT
  citing_doi,
  citing_title,
  llm_reduce(
    {'model_name':'gpt-4o'},
    {
      'prompt': 'Write ONE plain sentence (max 18 words) describing what the cited titles are mostly about.
                No markdown, no quotes, no colon, no bullets.',
      'context_columns': [
        {'data': cited_title, 'name':'cited_title'}
      ]
    }
  ) AS cited_theme_summary
FROM citations_enriched
GROUP BY citing_doi, citing_title;