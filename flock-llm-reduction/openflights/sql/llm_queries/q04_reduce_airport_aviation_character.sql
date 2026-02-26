-- Summarize the character of airlines serving each US airport
SELECT
  ap.airport_id,
  ap.name    AS airport_name,
  ap.city,
  ap.country,
  llm_reduce(
    {'model_name': 'gpt-4o'},
    {
      'prompt': 'In one sentence, describe the type of airlines and routes that serve this airport.',
      'context_columns': [
        {'data': al.name,     'name': 'airline'},
        {'data': al.country,  'name': 'airline_country'},
        {'data': al.callsign, 'name': 'callsign'}
      ]
    }
  ) AS airport_character
FROM airports ap
JOIN routes r    ON TRY_CAST(r.dst_airport_id AS INTEGER) = TRY_CAST(ap.airport_id AS INTEGER)
JOIN airlines al ON TRY_CAST(r.airline_id AS INTEGER)     = TRY_CAST(al.airline_id AS INTEGER)
WHERE ap.country = 'United States'
  AND ap.city IN ('New York', 'Los Angeles', 'Chicago', 'Miami', 'San Francisco')
  AND al.active = 'Y'
  AND (r.equipment LIKE '%737%' OR r.equipment LIKE '%320%' OR r.equipment LIKE '%757%')
GROUP BY ap.airport_id, ap.name, ap.city, ap.country
HAVING COUNT(DISTINCT al.airline_id) >= 3;