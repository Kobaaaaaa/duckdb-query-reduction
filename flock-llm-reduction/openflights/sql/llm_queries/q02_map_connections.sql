-- Describe each connecting flight pair (same airline, leg1.dst = leg2.src)
SELECT
  al.name AS airline_name,
  ap_src.city AS origin_city, ap_mid.city AS layover_city, ap_dst.city AS dest_city,
  r1.equipment AS leg1_aircraft, r2.equipment AS leg2_aircraft,
  llm_complete(
    {'model_name': 'gpt-4o'},
    {
      'prompt': 'In one sentence, describe this two-leg connecting itinerary and who it might appeal to.',
      'context_columns': [
        {'data': al.name,        'name': 'airline'},
        {'data': ap_src.city,    'name': 'origin'},
        {'data': ap_mid.city,    'name': 'layover'},
        {'data': ap_dst.city,    'name': 'destination'},
        {'data': r1.equipment,   'name': 'leg1_aircraft'},
        {'data': r2.equipment,   'name': 'leg2_aircraft'}
      ]
    }
  ) AS connection_note
FROM routes r1
JOIN routes r2
  ON  TRY_CAST(r1.dst_airport_id AS INTEGER) = TRY_CAST(r2.src_airport_id AS INTEGER)
  AND TRY_CAST(r1.airline_id AS INTEGER)     = TRY_CAST(r2.airline_id AS INTEGER)
JOIN airlines al    ON TRY_CAST(r1.airline_id AS INTEGER)       = TRY_CAST(al.airline_id AS INTEGER)
JOIN airports ap_src ON TRY_CAST(r1.src_airport_id AS INTEGER) = TRY_CAST(ap_src.airport_id AS INTEGER)
JOIN airports ap_mid ON TRY_CAST(r1.dst_airport_id AS INTEGER) = TRY_CAST(ap_mid.airport_id AS INTEGER)
JOIN airports ap_dst ON TRY_CAST(r2.dst_airport_id AS INTEGER) = TRY_CAST(ap_dst.airport_id AS INTEGER)
WHERE al.country = 'United States'
  AND ap_src.country = 'United States'
  AND ap_dst.country NOT IN ('United States', 'Canada', 'Mexico')
  AND al.active = 'Y'
LIMIT 200