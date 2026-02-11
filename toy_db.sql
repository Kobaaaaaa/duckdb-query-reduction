DROP TABLE IF EXISTS edges;
DROP TABLE IF EXISTS nodes;

CREATE TABLE nodes(id INTEGER, country VARCHAR, label VARCHAR);
CREATE TABLE edges(src INTEGER, dst INTEGER);

INSERT INTO nodes VALUES
  (1,'CA','Montreal'),
  (2,'CA','Toronto'),
  (3,'CA','Vancouver'),
  (4,'US','NYC'),
  (5,'US','Boston'),
  (6,'US','SF'),
  (7,'FR','Paris'),
  (8,'FR','Lyon'),
  (9,'DE','Berlin'),
  (10,'JP','Tokyo'),
  (11,'BR','SaoPaulo'),
  (12,'CA','QuebecCity');

INSERT INTO edges VALUES 
(4,1),(4,2),(4,3),(5,1),(5,2),(6,3),(6,2),(7,1),(7,4),(7,9),(8,7),(8,2),(9,7),
(9,2),(10,1),(10,12),(10,6),(11,2),(11,5),(11,8),(1,4),(1,7),(2,4),(2,5),(3,6),
(3,10),(12,1),(12,2),(12,9),(5,9);
