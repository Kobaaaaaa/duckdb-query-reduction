-- Useful views we will use in the reduction process

-- Citations enriched with metadata about the citing and cited works (title, venue, publisher)
CREATE OR REPLACE VIEW citations_enriched AS
SELECT
  c.*, w1.title AS citing_title, w2.title AS cited_title,
  w1.venue AS citing_venue, w2.venue AS cited_venue,
  w1.publisher AS citing_publisher, w2.publisher AS cited_publisher
FROM citations c
LEFT JOIN works w1 ON w1.doi = c.citing_doi
LEFT JOIN works w2 ON w2.doi = c.cited_doi;

-- Enriched work_authors view with author names
CREATE OR REPLACE VIEW work_authors_enriched AS
SELECT wa.doi, wa.author_pos, a.author_id, a.author_name
FROM work_authors wa
JOIN authors a ON a.author_id = wa.author_id;
