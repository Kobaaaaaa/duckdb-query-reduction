CREATE OR REPLACE TABLE works AS
SELECT * FROM read_csv_auto('data/opencitations/works.csv', all_varchar=true);

CREATE OR REPLACE TABLE citations AS
SELECT * FROM read_csv_auto('data/opencitations/citations.csv', all_varchar=true);

CREATE OR REPLACE TABLE authors AS
SELECT * FROM read_csv_auto('data/opencitations/authors.csv', all_varchar=true);

CREATE OR REPLACE TABLE work_authors AS
SELECT * FROM read_csv_auto('data/opencitations/work_authors.csv', all_varchar=true);

-- Change the 'yes'/'no' values in the journal_sc and author_sc in the citations table to boolean values
ALTER TABLE citations ADD COLUMN IF NOT EXISTS journal_sc_bool BOOLEAN;
ALTER TABLE citations ADD COLUMN IF NOT EXISTS author_sc_bool  BOOLEAN;

UPDATE citations
SET journal_sc_bool = (lower(journal_sc)='yes'),
    author_sc_bool  = (lower(author_sc)='yes');
