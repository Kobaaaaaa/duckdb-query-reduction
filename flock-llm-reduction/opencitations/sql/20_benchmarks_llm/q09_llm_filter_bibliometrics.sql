-- Using 'llm_filter' to identify works that are about metrics on papers
SELECT *
FROM works
WHERE llm_filter(
  {'model_name':'gpt-4o'},
  {
    'prompt': 'Is the citing paper about bibliometrics / citations / impact factor? Answer true/false.',
    'context_columns': [
      {'data': title, 'name':'title'},
      {'data': venue, 'name':'venue'}
    ]
  }
);