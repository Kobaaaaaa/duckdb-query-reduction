-- Works for which an author or journal are both cited and citing (self-citations)
SELECT c.citing_doi, w1.title AS citing_title,
  c.cited_doi, w2.title AS cited_title,
  c.journal_sc, c.author_sc
FROM citations c
LEFT JOIN works w1 ON w1.doi = c.citing_doi
LEFT JOIN works w2 ON w2.doi = c.cited_doi
WHERE c.journal_sc_bool OR c.author_sc_bool
ORDER BY c.citing_doi, c.cited_doi;