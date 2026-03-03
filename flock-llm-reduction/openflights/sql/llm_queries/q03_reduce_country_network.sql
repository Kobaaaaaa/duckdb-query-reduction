-- Summarise the destination footprint of airlines grouped by their home country
SELECT
  al.country AS airline_country,
  COUNT(DISTINCT ap.airport_id) AS num_destinations,
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
WHERE al.active = 'Y'
  AND al.country IN ('United States', 'United Kingdom', 'Germany', 'France', 'Japan')
GROUP BY al.country
HAVING COUNT(DISTINCT ap.airport_id) >= 10