-- Rerank UTC+0 timezone airports as international connecting hubs
SELECT llm_rerank(
  {'model_name': 'gpt-4o'},
  {
    'prompt': 'Best airport for international connections',
    'context_columns': [
      {'data': ap.name,    'name': 'airport'},
      {'data': ap.city,    'name': 'city'},
      {'data': ap.country, 'name': 'country'}
    ]
  }
) AS reranked
FROM airports ap
JOIN routes r    ON TRY_CAST(r.src_airport_id AS INTEGER) = TRY_CAST(ap.airport_id AS INTEGER)
JOIN airlines al ON TRY_CAST(r.airline_id AS INTEGER)     = TRY_CAST(al.airline_id AS INTEGER)
JOIN countries c ON c.name = ap.country
WHERE ap.dst = 'E'
  AND ap.country IN ('United Kingdom', 'France', 'Germany', 'Spain', 'Italy')
  AND al.active = 'Y'
  AND al.country NOT IN ('United Kingdom', 'France', 'Germany', 'Spain', 'Italy')
  AND (r.equipment LIKE '%777%' OR r.equipment LIKE '%787%' OR r.equipment LIKE '%A380%' OR r.equipment LIKE '%A330%')