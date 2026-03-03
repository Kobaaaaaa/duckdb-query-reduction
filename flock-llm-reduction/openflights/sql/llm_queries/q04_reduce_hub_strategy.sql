-- Characterise each airline's hub-and-spoke strategy from its connecting pairs
SELECT
  al.name AS airline_name,
  al.country,
  COUNT(*) AS num_connections,
  llm_reduce(
    {'model_name': 'gpt-4o'},
    {
      'prompt': 'Based on these layover cities, describe in one sentence this airline''s hub strategy.',
      'context_columns': [
        {'data': ap_mid.city,    'name': 'hub_city'},
        {'data': ap_mid.country, 'name': 'hub_country'}
      ]
    }
  ) AS hub_strategy
FROM routes r1
JOIN routes r2
  ON  TRY_CAST(r1.dst_airport_id AS INTEGER) = TRY_CAST(r2.src_airport_id AS INTEGER)
  AND TRY_CAST(r1.airline_id AS INTEGER)     = TRY_CAST(r2.airline_id AS INTEGER)
JOIN airlines al    ON TRY_CAST(r1.airline_id AS INTEGER)      = TRY_CAST(al.airline_id AS INTEGER)
JOIN airports ap_mid ON TRY_CAST(r1.dst_airport_id AS INTEGER) = TRY_CAST(ap_mid.airport_id AS INTEGER)
WHERE al.active = 'Y'
  AND al.country IN ('United States', 'United Kingdom', 'Germany', 'United Arab Emirates', 'Singapore')
GROUP BY al.airline_id, al.name, al.country
HAVING COUNT(*) >= 20
ORDER BY num_connections DESC