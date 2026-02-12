-- Works for which an author or journal are both cited and citing, (i.e., self-citations)
SELECT citing_doi, citing_title, cited_doi,  cited_title, journal_sc, author_sc
FROM citations_enriched
WHERE journal_sc_bool OR author_sc_bool
ORDER BY citing_doi, cited_doi;