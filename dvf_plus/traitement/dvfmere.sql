## _CREER_TABLE        
-- table {0}.{1}
CREATE TABLE {0}.{1}
(
{2}   
);

## _CREER_TABLE_HERITEE        
-- table fille {0}.{1}
CREATE TABLE {0}.{1}
(   
)INHERITS ({2}.{3});        

## _AJOUT_INSERT_TRIGGER
-- création du trigger de la table {1}
CREATE OR REPLACE FUNCTION {0}.{1}_insert_trigger()
RETURNS TRIGGER AS $$
BEGIN
     {2}
    
END;
$$
LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS insert_{1}_trigger ON {0}.{1};      
CREATE TRIGGER insert_{1}_trigger
    BEFORE INSERT ON {0}.{1}
    FOR EACH ROW EXECUTE PROCEDURE {0}.{1}_insert_trigger();

## _SUPPRIMER_TRIGGER
DROP TRIGGER insert_{2}_trigger ON {0}.{1};
--CREATE TRIGGER insert_{1}_trigger
--BEFORE INSERT ON {0}.{1}
--FOR EACH ROW EXECUTE PROCEDURE {0}.{1}_insert_trigger();

## _RENOMMER_TRIGGER
ALTER TRIGGER insert_{1}_trigger ON {0}.{2} RENAME TO insert_{2}_trigger

## _RECUPERER_CURVAL_SEQUENCE
SELECT nextval(pg_get_serial_sequence('{0}.{1}','{2}')::TEXT);

## _AFFECTER_CURVAL_SEQUENCE
SELECT nextval(pg_get_serial_sequence('{0}.{1}','{2}')::TEXT);
SELECT setval(pg_get_serial_sequence('{0}.{1}','{2}')::TEXT, {3});

## _RENOMMER_CONTRAINTE
ALTER TABLE {0}.{1} RENAME CONSTRAINT {1}{3} TO {2}{3}