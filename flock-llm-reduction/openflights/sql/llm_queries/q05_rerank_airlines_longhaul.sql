-- Rerank airlines from countries with 2-letter ISO code starting with 'A'
SELECT llm_rerank(
  {'model_name': 'gpt-4o'},
  {
    'prompt': 'Best airline for long-haul international travel',
    'context_columns': [
      {'data': al.name,     'name': 'airline'},
      {'data': al.country,  'name': 'country'},
      {'data': al.callsign, 'name': 'callsign'}
    ]
  }
) AS reranked
FROM airlines al
JOIN routes r    ON TRY_CAST(r.airline_id AS INTEGER)     = TRY_CAST(al.airline_id AS INTEGER)
JOIN airports ap ON TRY_CAST(r.dst_airport_id AS INTEGER) = TRY_CAST(ap.airport_id AS INTEGER)
JOIN countries c ON c.name = al.country
WHERE c.iso_code IN ('AU', 'AE', 'AR', 'AT')
  AND al.active = 'Y'
  AND ap.country IN ('United Kingdom', 'United States', 'China', 'Japan', 'Germany')
  AND (r.equipment LIKE '%777%' OR r.equipment LIKE '%787%' OR r.equipment LIKE '%A380%')