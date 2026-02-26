# openflights

## Dataset

OpenFlights provides airport, airline, route, plane, and country reference data. The airports database includes over 10,000 entries worldwide. The airlines database lists 5,888 airlines. The routes database contains 67,663 routes between 3,321 airports on 548 airlines (historical snapshot, last updated in 2014). The planes database includes 173 common aircraft types. Data is UTF-8 encoded and uses `\N` for nulls.

## Data

- Full: `data/original_data/`
- Sample: `data/samples/`

## Key files:

- `airports.csv` - airport ID, name, city, country, IATA/ICAO codes, lat/lon, altitude, timezone, type, source
- `airlines.csv` - airline ID, name, alias, IATA/ICAO codes, callsign, country, active flag
- `routes.csv` - airline code/ID, source and destination airport code/ID, codeshare, stops, equipment
- `planes.csv` - aircraft name, IATA, ICAO
- `countries.csv` - country name, ISO code, DAFIF code

## Common joins

- `routes.src_airport_id = airports.airport_id`
- `routes.dst_airport_id = airports.airport_id`
- `routes.airline_id = airlines.airline_id`
- `routes.src_airport_code = airports.iata OR airports.icao`
- `routes.dst_airport_code = airports.iata OR airports.icao`
- `routes.airline_code = airlines.iata OR airlines.icao`
- `airports.country = countries.name`
- `airlines.country = countries.name`
- `routes.equipment` can contain multiple codes separated by spaces; match each to `planes.iata` or `planes.icao`

Load tables with: `sql/setup/load.sql`.
