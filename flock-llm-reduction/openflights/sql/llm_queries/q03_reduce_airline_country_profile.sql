-- Summarize each airline country's destination footprint for European airports only
SELECT
  al.country AS airline_country,
  llm_reduce(
    {'model_name': 'gpt-4o'},
    {
      'prompt': 'In one sentence, describe the route-network profile of airlines from this country based on the destinations they serve.',
      'context_columns': [
        {'data': ap.city,    'name': 'dest_city'},
        {'data': ap.country, 'name': 'dest_country'}
      ]
    }
  ) AS network_profile
FROM airlines al
JOIN routes r    ON TRY_CAST(r.airline_id AS INTEGER)     = TRY_CAST(al.airline_id AS INTEGER)
JOIN airports ap ON TRY_CAST(r.dst_airport_id AS INTEGER) = TRY_CAST(ap.airport_id AS INTEGER)
WHERE ap.dst = 'E'
  AND al.country IN ('United Kingdom', 'Germany', 'France')
  AND TRY_CAST(ap.altitude_ft AS INTEGER) > 1000
  AND (r.equipment LIKE '%737%' OR r.equipment LIKE '%320%' OR r.equipment LIKE '%319%')
GROUP BY al.country
HAVING COUNT(DISTINCT ap.airport_id) >= 5;