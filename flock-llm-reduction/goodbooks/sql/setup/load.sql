INSTALL flock FROM community;
LOAD flock;

.read '.\local\secrets.sql'

CREATE OR REPLACE TABLE book_tags AS
SELECT * FROM read_csv_auto('data/original_data/book_tags.csv', all_varchar=true);

CREATE OR REPLACE TABLE books AS
SELECT * FROM read_csv_auto('data/original_data/books.csv', all_varchar=true);

CREATE OR REPLACE TABLE ratings AS
SELECT * FROM read_csv_auto('data/original_data/ratings.csv', all_varchar=true);

CREATE OR REPLACE TABLE tags AS
SELECT * FROM read_csv_auto('data/original_data/tags.csv', all_varchar=true);

CREATE OR REPLACE TABLE to_read AS
SELECT * FROM read_csv_auto('data/original_data/to_read.csv', all_varchar=true);