INSTALL flock FROM community;
LOAD flock;

.read '.\local\secrets.sql'

-- Choose dataset folder (edit this ONE LINE)
-- Use: 'data/original_data'  OR  'data/samples'
CREATE OR REPLACE MACRO data_dir() AS 'data/original_data';

CREATE OR REPLACE TABLE airports AS
SELECT * FROM read_csv_auto(data_dir() || '/airports.csv', all_varchar=true);

CREATE OR REPLACE TABLE airlines AS
SELECT * FROM read_csv_auto(data_dir() || '/airlines.csv', all_varchar=true);

CREATE OR REPLACE TABLE routes AS
SELECT * FROM read_csv_auto(data_dir() || '/routes.csv', all_varchar=true);

CREATE OR REPLACE TABLE planes AS
SELECT * FROM read_csv_auto(data_dir() || '/planes.csv', all_varchar=true);

CREATE OR REPLACE TABLE countries AS
SELECT * FROM read_csv_auto(data_dir() || '/countries.csv', all_varchar=true);