INSTALL flock FROM community;
LOAD flock;

.read '.\local\secrets.sql'

CREATE OR REPLACE TABLE airports AS
SELECT * FROM read_csv_auto('data/original_data/airports.csv', all_varchar=true);

CREATE OR REPLACE TABLE airlines AS
SELECT * FROM read_csv_auto('data/original_data/airlines.csv', all_varchar=true);

CREATE OR REPLACE TABLE routes AS
SELECT * FROM read_csv_auto('data/original_data/routes.csv', all_varchar=true);

CREATE OR REPLACE TABLE planes AS
SELECT * FROM read_csv_auto('data/original_data/planes.csv', all_varchar=true);

CREATE OR REPLACE TABLE countries AS
SELECT * FROM read_csv_auto('data/original_data/countries.csv', all_varchar=true);