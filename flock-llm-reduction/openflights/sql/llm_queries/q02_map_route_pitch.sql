-- Write a 1-sentence pitch for routes operated by US airlines
SELECT
  r.airline_code,
  r.src_airport_code,
  r.dst_airport_code,
  al.name    AS airline_name,
  ap.city    AS dest_city,
  ap.country AS dest_country,
  c.iso_code AS dest_iso,
  llm_complete(
    {'model_name': 'gpt-4o'},
    {
      'prompt': 'Write one enticing sentence pitching this flight route to a traveler.',
      'context_columns': [
        {'data': al.name,    'name': 'airline'},
        {'data': ap.city,    'name': 'dest_city'},
        {'data': ap.country, 'name': 'dest_country'},
        {'data': c.iso_code, 'name': 'dest_iso'}
      ]
    }
  ) AS travel_pitch
FROM routes r
JOIN airlines al ON TRY_CAST(r.airline_id AS INTEGER)     = TRY_CAST(al.airline_id AS INTEGER)
JOIN airports ap ON TRY_CAST(r.dst_airport_id AS INTEGER) = TRY_CAST(ap.airport_id AS INTEGER)
JOIN countries c ON c.name = ap.country
WHERE al.country = 'United States'
  AND ap.country IN ('France', 'Italy', 'Spain', 'Greece')
  AND r.stops = '0'
  AND r.equipment LIKE '%777%'
LIMIT 500;