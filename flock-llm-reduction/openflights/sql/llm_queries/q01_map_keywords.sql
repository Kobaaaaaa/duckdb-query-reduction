-- Generate search keywords for codeshare routes
SELECT
  r.airline_code,
  r.src_airport_code,
  r.dst_airport_code,
  r.equipment,
  al.name        AS airline_name,
  al.country     AS airline_country,
  ap.city        AS dest_city,
  ap.country     AS dest_country,
  llm_complete(
    {'model_name': 'gpt-4o'},
    {
      'prompt': 'Generate a short list of searchable keywords for this flight route. Plain text, comma-separated.',
      'context_columns': [
        {'data': al.name,    'name': 'airline'},
        {'data': al.country, 'name': 'airline_country'},
        {'data': ap.city,    'name': 'dest_city'},
        {'data': ap.country, 'name': 'dest_country'},
        {'data': r.equipment,'name': 'aircraft'}
      ]
    }
  ) AS search_keywords
FROM routes r
JOIN airlines al ON TRY_CAST(r.airline_id AS INTEGER)     = TRY_CAST(al.airline_id AS INTEGER)
JOIN airports ap ON TRY_CAST(r.dst_airport_id AS INTEGER) = TRY_CAST(ap.airport_id AS INTEGER)
WHERE r.codeshare = 'Y'
  AND al.country IN ('United States', 'United Kingdom', 'Germany')
  AND ap.country IN ('United States', 'United Kingdom', 'Germany');