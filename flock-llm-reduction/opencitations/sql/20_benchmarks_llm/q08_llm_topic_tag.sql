-- Generate topic labels for all works using 'llm_complete'
SELECT
  doi,
  title,
  venue,
  llm_complete(
    {'model_name':'gpt-4o'},
    {
      'prompt': 'Return a 3-6 word topic label for this paper.',
      'context_columns': [
        {'data': title, 'name':'title'},
        {'data': venue, 'name':'venue'}
      ]
    }
  ) AS topic_label
FROM works;