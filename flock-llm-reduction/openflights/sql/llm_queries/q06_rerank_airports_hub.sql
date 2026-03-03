-- Rank airport-route pairs by degree of international connectivity using airline origin vs airport country
SELECT llm_rerank(
  {'model_name': 'gpt-4o'},
  {
    'prompt': 'Rank these airport routes by degree of international connectivity,
     using only whether the airline country differs from the airport country, and how far the destination country is from the airport country.',
    'context_columns': [
      {'data': ap.name,        'name': 'airport'},
      {'data': ap.country,     'name': 'airport_country'},
      {'data': al.country,     'name': 'airline_country'},
      {'data': ap_dst.country, 'name': 'dest_country'},
      {'data': r.equipment,    'name': 'aircraft_type'}
    ]
  }
) AS reranked
FROM airports ap
JOIN routes r        ON TRY_CAST(r.src_airport_id AS INTEGER)   = TRY_CAST(ap.airport_id AS INTEGER)
JOIN airlines al     ON TRY_CAST(r.airline_id AS INTEGER)       = TRY_CAST(al.airline_id AS INTEGER)
JOIN airports ap_dst ON TRY_CAST(r.dst_airport_id AS INTEGER)   = TRY_CAST(ap_dst.airport_id AS INTEGER)
WHERE ap.country IN ('United Kingdom', 'France', 'Germany', 'Netherlands', 'Turkey')
  AND al.active = 'Y'
  AND al.country != ap.country