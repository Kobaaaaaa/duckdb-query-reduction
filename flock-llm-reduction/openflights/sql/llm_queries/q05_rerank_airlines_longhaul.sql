-- Rank widebody routes by likely flight range based on aircraft type and destination country
SELECT llm_rerank(
  {'model_name': 'gpt-4o'},
  {
    'prompt': 'Rank these flight routes by expected flight duration, using only aircraft type and destination country as signals.',
    'context_columns': [
      {'data': al.name,        'name': 'airline'},
      {'data': ap_src.country, 'name': 'origin_country'},
      {'data': ap_dst.country, 'name': 'dest_country'},
      {'data': r.equipment,    'name': 'aircraft_type'},
      {'data': r.stops,        'name': 'stops'}
    ]
  }
) AS reranked
FROM routes r
JOIN airlines al     ON TRY_CAST(r.airline_id AS INTEGER)       = TRY_CAST(al.airline_id AS INTEGER)
JOIN airports ap_src ON TRY_CAST(r.src_airport_id AS INTEGER)   = TRY_CAST(ap_src.airport_id AS INTEGER)
JOIN airports ap_dst ON TRY_CAST(r.dst_airport_id AS INTEGER)   = TRY_CAST(ap_dst.airport_id AS INTEGER)
WHERE al.active = 'Y'
  AND r.stops = '0'
  AND (r.equipment LIKE '%777%' OR r.equipment LIKE '%787%' OR r.equipment LIKE '%380%')