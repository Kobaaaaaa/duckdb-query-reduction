-- One-sentence travel pitch for each non-stop US-to-Europe route
SELECT
  r.airline_code, r.src_airport_code, r.dst_airport_code,
  al.name AS airline_name, ap_dst.city AS dest_city, ap_dst.country AS dest_country,
  llm_complete(
    {'model_name': 'gpt-4o'},
    {
      'prompt': 'Write one enticing sentence pitching this flight route to a traveler.',
      'context_columns': [
        {'data': al.name,        'name': 'airline'},
        {'data': ap_src.city,    'name': 'origin_city'},
        {'data': ap_dst.city,    'name': 'dest_city'},
        {'data': ap_dst.country, 'name': 'dest_country'},
        {'data': r.equipment,    'name': 'aircraft'}
      ]
    }
  ) AS travel_pitch
FROM routes r
JOIN airlines al  ON TRY_CAST(r.airline_id AS INTEGER)      = TRY_CAST(al.airline_id AS INTEGER)
JOIN airports ap_src ON TRY_CAST(r.src_airport_id AS INTEGER) = TRY_CAST(ap_src.airport_id AS INTEGER)
JOIN airports ap_dst ON TRY_CAST(r.dst_airport_id AS INTEGER) = TRY_CAST(ap_dst.airport_id AS INTEGER)
WHERE al.country = 'United States'
  AND ap_dst.country IN ('France', 'Italy', 'Spain', 'Germany', 'United Kingdom')
  AND r.stops = '0'
  AND al.active = 'Y'