## INSERER_DONNEES_TABLES_ANNEXES
-- ajout des données annexes
INSERT INTO {0}.ann_type_local(codtyploc, libtyploc) VALUES (1, 'Maison');
INSERT INTO {0}.ann_type_local(codtyploc, libtyploc) VALUES (2, 'Appartement');
INSERT INTO {0}.ann_type_local(codtyploc, libtyploc) VALUES (3, 'Dépendance');
INSERT INTO {0}.ann_type_local(codtyploc, libtyploc) VALUES (4, 'Local industriel, commercial ou assimilé');

INSERT INTO {0}.ann_nature_mutation(libnatmut) VALUES ('Vente');
INSERT INTO {0}.ann_nature_mutation(libnatmut) VALUES ('Vente en l''état futur d''achèvement');
INSERT INTO {0}.ann_nature_mutation(libnatmut) VALUES ('Expropriation');
INSERT INTO {0}.ann_nature_mutation(libnatmut) VALUES ('Vente terrain à bâtir');
INSERT INTO {0}.ann_nature_mutation(libnatmut) VALUES ('Adjudication');
INSERT INTO {0}.ann_nature_mutation(libnatmut) VALUES ('Echange');

## CREER_TABLES_ANNEXES_TEMPORAIRES
SET client_encoding = 'UTF8';
-- CREATION DE TABLES ANNEXES TEMPORAIRES

 --> table ann_cgi
DROP TABLE IF EXISTS {0}.tmp_ann_cgi;
CREATE TABLE {0}.tmp_ann_cgi
(
    article_cgi character varying(20),
    lib_article_cgi character varying(254),
    CONSTRAINT tmp_ann_cgi_unique PRIMARY KEY (article_cgi)
);

 --> table tmp_ann_nature_culture
DROP TABLE IF EXISTS {0}.tmp_ann_nature_culture;
CREATE TABLE {0}.tmp_ann_nature_culture
(
    nature_culture character varying(2),
    lib_nature_culture character varying(254),
    CONSTRAINT tmp_ann_nature_culture_pkey PRIMARY KEY (nature_culture)    
);

 --> table tmp_ann_nature_culture_speciale
DROP TABLE IF EXISTS {0}.tmp_ann_nature_culture_speciale;
CREATE TABLE {0}.tmp_ann_nature_culture_speciale
(
    nature_culture_speciale character varying(5),
    lib_nature_culture_speciale character varying(254),
    CONSTRAINT tmp_nature_culture_speciale_pkey PRIMARY KEY (nature_culture_speciale)
);

## MAJ_TABLES_ANNEXES
-- MAJ DES TABLES ANNEXES
INSERT INTO {0}.ann_cgi 
(
    artcgi,libartcgi
)
(
    SELECT t1.article_cgi, t1.lib_article_cgi 
    FROM {0}.tmp_ann_cgi t1
    LEFT JOIN {0}.ann_cgi t2 
    ON t1.article_cgi=t2.artcgi
    WHERE t2.artcgi IS NULL
);

INSERT INTO {0}.ann_nature_culture 
(
    natcult,libnatcult
)
(
    SELECT t1.nature_culture, t1.lib_nature_culture
    FROM {0}.tmp_ann_nature_culture t1
    LEFT JOIN {0}.ann_nature_culture t2 
    ON t1.nature_culture=t2.natcult
    WHERE t2.natcult IS NULL
);

INSERT INTO {0}.ann_nature_culture_speciale 
(
    natcultspe,libnatcusp
)
(
    SELECT t1.nature_culture_speciale, t1.lib_nature_culture_speciale
    FROM {0}.tmp_ann_nature_culture_speciale t1
    LEFT JOIN {0}.ann_nature_culture_speciale t2 
    ON t1.nature_culture_speciale=t2.natcultspe
    WHERE t2.natcultspe IS NULL
);

## CREER_TABLE_IMPORT_TEMPORAIRE
SET client_encoding = 'UTF8';
SET datestyle = 'ISO, DMY';
-- ******************************
-- CREATION DE LA TABLE TEMPORAIRE VIDE POUR IMPORT DES DONNEES
-- ******************************

DROP TABLE IF EXISTS source.tmp;
CREATE TABLE source.tmp
(
    code_service_ch character varying(7),
    reference_document character varying(10),
    "1_articles_cgi" character varying(20),
    "2_articles_cgi" character varying(20),
    "3_articles_cgi" character varying(20),
    "4_articles_cgi" character varying(20),
    "5_articles_cgi" character varying(20),
    no_disposition integer,
    date_mutation date,
    nature_mutation character varying(34),
    valeur_fonciere character varying,     -- PASSE EN NUMERIC ENSUITE APRES AVOIR REMPLACE LES VIRGULES PAR DES POINTS
    no_voie integer,    
    b_t_q character varying(1),        
    type_de_voie character varying(4),    
    code_voie character varying(4),        
    voie character varying(254),        
    code_postal character varying(5),    
    commune character varying(254),    
    code_departement character varying(3),    -- COMPLETE ENSUITE PAR DES "0" SI NECESSAIRE
    code_commune character varying(3),        -- COMPLETE ENSUITE PAR DES "0" SI NECESSAIRE
    prefixe_de_section character varying(3),    -- COMPLETE ENSUITE PAR DES "0" SI NECESSAIRE
    section character varying(2),            -- COMPLETE ENSUITE PAR DES "0" SI NECESSAIRE
    no_plan character varying(4),            -- COMPLETE ENSUITE PAR DES "0" SI NECESSAIRE
    no_volume character varying(7),        
    "1er_lot" character varying(7),
    surface_carrez_du_1er_lot character varying,     -- PASSE EN NUMERIC ENSUITE APRES AVOIR REMPLACE LES VIRGULES PAR DES POINTS
    "2eme_lot" character varying(7),
    surface_carrez_du_2eme_lot character varying,     -- PASSE EN NUMERIC ENSUITE APRES AVOIR REMPLACE LES VIRGULES PAR DES POINTS
    "3eme_lot" character varying(7),
    surface_carrez_du_3eme_lot character varying,     -- PASSE EN NUMERIC ENSUITE APRES AVOIR REMPLACE LES VIRGULES PAR DES POINTS
    "4eme_lot" character varying(7),
    surface_carrez_du_4eme_lot character varying,     -- PASSE EN NUMERIC ENSUITE APRES AVOIR REMPLACE LES VIRGULES PAR DES POINTS
    "5eme_lot" character varying(7),
    surface_carrez_du_5eme_lot character varying,     -- PASSE EN NUMERIC ENSUITE APRES AVOIR REMPLACE LES VIRGULES PAR DES POINTS
    nombre_de_lots integer,
    code_type_local integer,            
    type_local character varying(254),
    identifiant_local character varying(10),
    surface_reelle_bati integer,
    nombre_pieces_principales integer,
    nature_culture character varying(2),            
    nature_culture_speciale character varying(5),    
    surface_terrain integer
);

## CREER_TABLE_SOURCE_SANS_RECHERCHE_DIFFERENTIELLE
DROP TABLE IF EXISTS source.{0};
CREATE TABLE source.{0} AS 
(
    SELECT 
        code_service_ch, reference_document,
        concat(code_service_ch, '_', reference_document) AS idmutinvar, 
        "1_articles_cgi", "2_articles_cgi", "3_articles_cgi", "4_articles_cgi", "5_articles_cgi", no_disposition, 
        date_mutation, 
        EXTRACT(YEAR FROM date_mutation)::integer as anneemut,
        EXTRACT(MONTH FROM date_mutation)::integer as moismut,
        nature_mutation, 

        (replace(valeur_fonciere, ',', '.'))::numeric AS valeur_fonciere, 

                no_voie, b_t_q, 
        type_de_voie, code_voie, voie, lpad(code_postal,5,'0') AS code_postal, commune, 
        CASE WHEN code_departement NOT LIKE '97_' THEN lpad(COALESCE(code_departement,''), 2, '0') ELSE code_departement END AS code_departement,
        CASE WHEN code_departement NOT LIKE '97_' THEN lpad(COALESCE(code_commune,''), 3, '0') ELSE lpad(COALESCE(code_commune,''), 2, '0') END AS code_commune,
        lpad(COALESCE(prefixe_de_section,''), 3, '0') AS prefixe_de_section,
        lpad(COALESCE(section,''), 2, '0') AS section,
        lpad(COALESCE(no_plan,''), 4, '0') AS no_plan,
        no_volume, 
        "1er_lot", (replace(surface_carrez_du_1er_lot, ',', '.'))::numeric AS surface_carrez_du_1er_lot, 
        "2eme_lot", (replace(surface_carrez_du_2eme_lot, ',', '.'))::numeric AS surface_carrez_du_2eme_lot, 
        "3eme_lot", (replace(surface_carrez_du_3eme_lot, ',', '.'))::numeric AS surface_carrez_du_3eme_lot, 
        "4eme_lot", (replace(surface_carrez_du_4eme_lot, ',', '.'))::numeric AS surface_carrez_du_4eme_lot, 
        "5eme_lot", (replace(surface_carrez_du_5eme_lot, ',', '.'))::numeric AS surface_carrez_du_5eme_lot, 
        nombre_de_lots, code_type_local, 
        type_local, identifiant_local, surface_reelle_bati, nombre_pieces_principales, 
        nature_culture, nature_culture_speciale, surface_terrain,
        
       CASE 
                WHEN nature_culture LIKE 'T%' THEN 1
                WHEN nature_culture LIKE 'P%' THEN 2
                WHEN nature_culture = 'VE' THEN 3
                WHEN nature_culture = 'VI' THEN 4
                WHEN nature_culture LIKE 'B%' THEN 5
                WHEN nature_culture LIKE 'L%' THEN 6
                WHEN nature_culture = 'CA' THEN 7
                WHEN nature_culture = 'E' THEN 8
                WHEN nature_culture = 'J' THEN 9
                WHEN nature_culture = 'AB' THEN 10
                WHEN nature_culture = 'AG' THEN 11
                WHEN nature_culture = 'CH' THEN 12
                WHEN nature_culture = 'S' THEN 13
            ELSE NULL 
            END AS nodcnt,

        CASE WHEN code_departement NOT LIKE '97_' THEN lpad(COALESCE(code_departement,''), 2, '0') ELSE code_departement END||
            CASE WHEN code_departement NOT LIKE '97_' THEN lpad(COALESCE(code_commune,''), 3, '0') ELSE lpad(COALESCE(code_commune,''), 2, '0') END||
            lpad(COALESCE(prefixe_de_section,''), 3, '0')||lpad(COALESCE(section,''), 2, '0')||lpad(COALESCE(no_plan,''), 4, '0') 
        AS idpar,
        
        CASE WHEN code_departement NOT LIKE '97_' THEN lpad(COALESCE(code_departement,''), 2, '0') ELSE code_departement END||
            CASE 
                WHEN lpad(COALESCE(prefixe_de_section,''), 3, '0') = '000' THEN 
                    CASE 
                        WHEN code_departement NOT LIKE '97_' THEN lpad(COALESCE(code_commune,''), 3, '0') 
                        ELSE lpad(COALESCE(code_commune,''), 2, '0') 
                    END 
                ELSE 
                    CASE 
                        WHEN code_departement NOT LIKE '97_' THEN lpad(COALESCE(prefixe_de_section,''), 3, '0') 
                        ELSE lpad(COALESCE(substring(prefixe_de_section from '..$'),''), 2, '0') 
                    END 
            END||
            lpad(identifiant_local, 7, '0') AS idloc,

        CASE WHEN no_voie IS NULL THEN '' ELSE no_voie::varchar END || '$' ||
            CASE WHEN b_t_q IS NULL THEN '' ELSE b_t_q END || '$' ||
            CASE WHEN code_voie IS NULL THEN '' ELSE code_voie END || '$' ||
            CASE WHEN type_de_voie IS NULL THEN '' ELSE type_de_voie END || '$' ||
            CASE WHEN voie IS NULL THEN '' ELSE voie END || '$' ||
            CASE WHEN code_postal IS NULL THEN '' ELSE code_postal END || '$' ||
            CASE WHEN commune IS NULL THEN '' ELSE commune END 
        AS idadr_tmp,
        
        CASE WHEN nature_culture IS NULL THEN '' ELSE nature_culture::varchar END || '$' ||
            CASE WHEN nature_culture_speciale IS NULL THEN '' ELSE nature_culture_speciale::varchar END || '$' ||
            CASE WHEN surface_terrain IS NULL THEN '' ELSE surface_terrain::varchar END
        AS idsuf_tmp
        
    FROM 
        source.tmp
);


## CREER_TABLE_SOURCE_AVEC_RECHERCHE_DIFFERENTIELLE
DROP TABLE IF EXISTS source.{0};
CREATE TABLE source.{0} AS 
(
    SELECT
        t.*
    FROM(
    
        SELECT 
            code_service_ch, reference_document,
            concat(code_service_ch, '_', reference_document) AS idmutinvar, 
            "1_articles_cgi", "2_articles_cgi", "3_articles_cgi", "4_articles_cgi", "5_articles_cgi", no_disposition, 
            date_mutation, 
            EXTRACT(YEAR FROM date_mutation)::integer as anneemut,
            EXTRACT(MONTH FROM date_mutation)::integer as moismut,            
            nature_mutation, 
    
            (replace(valeur_fonciere, ',', '.'))::numeric AS valeur_fonciere, 
    
                    no_voie, b_t_q, 
            type_de_voie, code_voie, voie, lpad(code_postal,5,'0') AS code_postal, commune, 
            CASE WHEN code_departement NOT LIKE '97_' THEN lpad(COALESCE(code_departement,''), 2, '0') ELSE code_departement END AS code_departement,
            CASE WHEN code_departement NOT LIKE '97_' THEN lpad(COALESCE(code_commune,''), 3, '0') ELSE lpad(COALESCE(code_commune,''), 2, '0') END AS code_commune,
            lpad(COALESCE(prefixe_de_section,''), 3, '0') AS prefixe_de_section,
            lpad(COALESCE(section,''), 2, '0') AS section,
            lpad(COALESCE(no_plan,''), 4, '0') AS no_plan,
            no_volume, 
            "1er_lot", (replace(surface_carrez_du_1er_lot, ',', '.'))::numeric AS surface_carrez_du_1er_lot, 
            "2eme_lot", (replace(surface_carrez_du_2eme_lot, ',', '.'))::numeric AS surface_carrez_du_2eme_lot, 
            "3eme_lot", (replace(surface_carrez_du_3eme_lot, ',', '.'))::numeric AS surface_carrez_du_3eme_lot, 
            "4eme_lot", (replace(surface_carrez_du_4eme_lot, ',', '.'))::numeric AS surface_carrez_du_4eme_lot, 
            "5eme_lot", (replace(surface_carrez_du_5eme_lot, ',', '.'))::numeric AS surface_carrez_du_5eme_lot, 
            nombre_de_lots, code_type_local, 
            type_local, identifiant_local, surface_reelle_bati, nombre_pieces_principales, 
            nature_culture, nature_culture_speciale, surface_terrain,
            
            CASE 
                WHEN nature_culture LIKE 'T%' THEN 1
                WHEN nature_culture LIKE 'P%' THEN 2
                WHEN nature_culture = 'VE' THEN 3
                WHEN nature_culture = 'VI' THEN 4
                WHEN nature_culture LIKE 'B%' THEN 5
                WHEN nature_culture LIKE 'L%' THEN 6
                WHEN nature_culture = 'CA' THEN 7
                WHEN nature_culture = 'E' THEN 8
                WHEN nature_culture = 'J' THEN 9
                WHEN nature_culture = 'AB' THEN 10
                WHEN nature_culture = 'AG' THEN 11
                WHEN nature_culture = 'CH' THEN 12
                WHEN nature_culture = 'S' THEN 13
            ELSE NULL 
            END AS nodcnt,
    
            CASE WHEN code_departement NOT LIKE '97_' THEN lpad(COALESCE(code_departement,''), 2, '0') ELSE code_departement END||
                CASE WHEN code_departement NOT LIKE '97_' THEN lpad(COALESCE(code_commune,''), 3, '0') ELSE lpad(COALESCE(code_commune,''), 2, '0') END||
                lpad(COALESCE(prefixe_de_section,''), 3, '0')||lpad(COALESCE(section,''), 2, '0')||lpad(COALESCE(no_plan,''), 4, '0') 
            AS idpar,
            
            CASE WHEN code_departement NOT LIKE '97_' THEN lpad(COALESCE(code_departement,''), 2, '0') ELSE code_departement END||
            CASE 
                WHEN lpad(COALESCE(prefixe_de_section,''), 3, '0') = '000' THEN 
                    CASE 
                        WHEN code_departement NOT LIKE '97_' THEN lpad(COALESCE(code_commune,''), 3, '0') 
                        ELSE lpad(COALESCE(code_commune,''), 2, '0') 
                    END 
                ELSE 
                    CASE 
                        WHEN code_departement NOT LIKE '97_' THEN lpad(COALESCE(prefixe_de_section,''), 3, '0') 
                        ELSE lpad(COALESCE(substring(prefixe_de_section from '..$'),''), 2, '0') 
                    END 
            END||
            lpad(identifiant_local, 7, '0') AS idloc,
    
            CASE WHEN no_voie IS NULL THEN '' ELSE no_voie::varchar END || '$' ||
                CASE WHEN b_t_q IS NULL THEN '' ELSE b_t_q END || '$' ||
                CASE WHEN code_voie IS NULL THEN '' ELSE code_voie END || '$' ||
                CASE WHEN type_de_voie IS NULL THEN '' ELSE type_de_voie END || '$' ||
                CASE WHEN voie IS NULL THEN '' ELSE voie END || '$' ||
                CASE WHEN code_postal IS NULL THEN '' ELSE code_postal END || '$' ||
                CASE WHEN commune IS NULL THEN '' ELSE commune END 
            AS idadr_tmp,
            
            CASE WHEN nature_culture IS NULL THEN '' ELSE nature_culture::varchar END || '$' ||
                CASE WHEN nature_culture_speciale IS NULL THEN '' ELSE nature_culture_speciale::varchar END || '$' ||
                CASE WHEN surface_terrain IS NULL THEN '' ELSE surface_terrain::varchar END
            AS idsuf_tmp
            
        FROM 
            source.tmp
    ) t
    LEFT JOIN dvf.mutation tt
    ON t.idmutinvar = tt.idmutinvar
    WHERE tt.idmutinvar IS NULL
);

## CREER_TABLE_SOURCE_DEPARTEMENTALE
DROP TABLE IF EXISTS source.{0}_d{1};
CREATE TABLE source.{0}_d{1} AS
(
	SELECT * FROM source.{0} WHERE code_departement = '{2}'
);

## CREER_TABLE_CALCUL_LOT
CREATE TABLE source.tmp_calcul_lot AS(

    SELECT 
        t1.idmutinvar, t1.no_disposition, t1.idpar,
        COALESCE(t1.nb_de_lots_distinct,0) + COALESCE(t2.nombre_de_lots_sup5,0) - COALESCE(nb_de_lots_distinct_sup5,0) AS nblot
    FROM
    (
    SELECT idmutinvar, no_disposition, idpar,
        array_length(ARRAY(SELECT DISTINCT unnest(array_supprimer_null(array_cat(array_agg("1er_lot"),array_cat(array_agg("2eme_lot"),array_cat(array_agg("3eme_lot"),array_cat(array_agg("4eme_lot"), array_agg("5eme_lot")))))::VARCHAR[]))),1) as nb_de_lots_distinct
        --
        -- à partir de la version 9.3
        --array_length(ARRAY(SELECT DISTINCT unnest(array_remove(array_cat(array_agg("1er_lot"),array_cat(array_agg("2eme_lot"),array_cat(array_agg("3eme_lot"),array_cat(array_agg("4eme_lot"), array_agg("5eme_lot"))))), NULL)::VARCHAR[])),1) as nb_de_lots_distinct
      FROM source.{0}
      WHERE nombre_de_lots != 0
      GROUP BY idmutinvar, no_disposition, idpar
     ) t1
    LEFT JOIN
    (
    SELECT idmutinvar, no_disposition, idpar,
        array_length(ARRAY(SELECT DISTINCT unnest(array_supprimer_null(array_cat(array_agg("1er_lot"),array_cat(array_agg("2eme_lot"),array_cat(array_agg("3eme_lot"),array_cat(array_agg("4eme_lot"), array_agg("5eme_lot")))))::VARCHAR[]))),1) as nb_de_lots_distinct_sup5,
        --
        -- à partir de la version 9.3
        --array_length(ARRAY(SELECT DISTINCT unnest(array_remove(array_cat(array_agg("1er_lot"),array_cat(array_agg("2eme_lot"),array_cat(array_agg("3eme_lot"),array_cat(array_agg("4eme_lot"), array_agg("5eme_lot"))))), NULL)::VARCHAR[])),1) as nb_de_lots_distinct_sup5,
        SUM(nombre_de_lots) as nombre_de_lots_sup5
      FROM source.{0}
      WHERE nombre_de_lots > 5
      GROUP BY idmutinvar, no_disposition, idpar
    ) t2
    ON t1.idmutinvar = t2.idmutinvar and t1.no_disposition = t2.no_disposition and t1.idpar = t2.idpar
);

## MAJ_TABLE_ANN_NATURE_MUTATION
-- insertion table ann_nature_mutation
-- NE SERT PLUS : dorénavant, les valeurs sont figés : cf INSERER_DONNEES_TABLES_ANNEXES
INSERT INTO {0}.ann_nature_mutation(libnatmut)
(
    SELECT t.nature_mutation 
    FROM source.{1} t
    LEFT JOIN {0}.ann_nature_mutation t1 ON t.nature_mutation = t1.libnatmut
    WHERE t1.idnatmut IS NULL
    GROUP BY t.nature_mutation
);

## MAJ_TABLE_MUTATION
-- insertion table mutation
INSERT INTO {0}.mutation 
(
    idmutinvar, idnatmut, codservch, refdoc, datemut, anneemut, moismut, coddep
)
(
    SELECT t.idmutinvar, t1.idnatmut, t.code_service_ch, t.reference_document, t.date_mutation, t.anneemut, t.moismut, t.code_departement
    FROM source.{1} t
    LEFT JOIN {2}.ann_nature_mutation t1 ON t.nature_mutation=t1.libnatmut
    LEFT JOIN {0}.mutation t2 ON t.code_service_ch=t2.codservch AND t.reference_document=t2.refdoc 
    WHERE t2.idmutation IS NULL
    GROUP BY t.idmutinvar, t.code_service_ch, t.reference_document, t.date_mutation, t1.idnatmut, t.anneemut, t.moismut, t.code_departement    
);

## MAJ_TABLE_MUTATION_ART_CGI
-- insertion table mutation_article_cgi
INSERT INTO {0}.mutation_article_cgi
(
    idmutation, idartcgi, ordarticgi, coddep
)
(
    SELECT t2.idmutation, t3.idartcgi, 1 AS ordarticgi, t2.coddep
    FROM source.{1} t
    LEFT JOIN {0}.mutation t2 ON t.code_service_ch=t2.codservch AND t.reference_document=t2.refdoc  
    LEFT JOIN {2}.ann_cgi t3 ON t."1_articles_cgi"=t3.artcgi
    LEFT JOIN {0}.mutation_article_cgi t4 ON t2.idmutation=t4.idmutation AND 1=t4.ordarticgi
    WHERE 
        t."1_articles_cgi" IS NOT NULL 
        AND t4.idmutation IS NULL
    GROUP BY t2.idmutation, t3.idartcgi, t2.coddep
    
    UNION

    SELECT t2.idmutation, t3.idartcgi, 2 AS ordarticgi, t2.coddep
    FROM source.{1} t
    LEFT JOIN {0}.mutation t2 ON t.code_service_ch=t2.codservch AND t.reference_document=t2.refdoc  
    LEFT JOIN {2}.ann_cgi t3 ON t."2_articles_cgi"=t3.artcgi
    LEFT JOIN {0}.mutation_article_cgi t4 ON t2.idmutation=t4.idmutation AND 2=t4.ordarticgi
    WHERE 
        t."2_articles_cgi" IS NOT NULL 
        AND t4.idmutation IS NULL
    GROUP BY t2.idmutation, t3.idartcgi, t2.coddep

    UNION

    SELECT t2.idmutation, t3.idartcgi, 3 AS ordarticgi, t2.coddep
    FROM source.{1} t
    LEFT JOIN {0}.mutation t2 ON t.code_service_ch=t2.codservch AND t.reference_document=t2.refdoc  
    LEFT JOIN {2}.ann_cgi t3 ON t."3_articles_cgi"=t3.artcgi
    LEFT JOIN {0}.mutation_article_cgi t4 ON t2.idmutation=t4.idmutation AND 3=t4.ordarticgi
    WHERE 
        t."3_articles_cgi" IS NOT NULL 
        AND t4.idmutation IS NULL
    GROUP BY t2.idmutation, t3.idartcgi, t2.coddep

    UNION

    SELECT t2.idmutation, t3.idartcgi, 4 AS ordarticgi, t2.coddep
    FROM source.{1} t
    LEFT JOIN {0}.mutation t2 ON t.code_service_ch=t2.codservch AND t.reference_document=t2.refdoc  
    LEFT JOIN {2}.ann_cgi t3 ON t."4_articles_cgi"=t3.artcgi
    LEFT JOIN {0}.mutation_article_cgi t4 ON t2.idmutation=t4.idmutation AND 4=t4.ordarticgi
    WHERE 
        t."4_articles_cgi" IS NOT NULL 
        AND t4.idmutation IS NULL
    GROUP BY t2.idmutation, t3.idartcgi, t2.coddep
    
    UNION

    SELECT t2.idmutation, t3.idartcgi, 5 AS ordarticgi, t2.coddep
    FROM source.{1} t
    LEFT JOIN {0}.mutation t2 ON t.code_service_ch=t2.codservch AND t.reference_document=t2.refdoc  
    LEFT JOIN {2}.ann_cgi t3 ON t."5_articles_cgi"=t3.artcgi
    LEFT JOIN {0}.mutation_article_cgi t4 ON t2.idmutation=t4.idmutation AND 5=t4.ordarticgi
    WHERE 
        t."5_articles_cgi" IS NOT NULL 
        AND t4.idmutation IS NULL
    GROUP BY t2.idmutation, t3.idartcgi, t2.coddep
    
);

## MAJ_TABLE_DISPOSITION
-- insertion table disposition
INSERT INTO {0}.disposition 
(
    idmutation, nodispo, valeurfonc, nblot, coddep
)
(
    SELECT tt.idmutation, tt.no_disposition, tt.valeur_fonciere, COALESCE(tt.nblot, 0), tt.code_departement
    FROM
    (
        SELECT t2.idmutation, t.no_disposition, t.valeur_fonciere, t4.nblot, t.code_departement
        FROM source.{1} t
        LEFT JOIN {0}.mutation t2 ON t.code_service_ch=t2.codservch AND t.reference_document=t2.refdoc
        LEFT JOIN 
            (
                SELECT 
                    idmutinvar, no_disposition, sum(nblot) as nblot
                FROM
                source.tmp_calcul_lot
                GROUP BY idmutinvar, no_disposition
            ) t4 
            ON t.idmutinvar = t4.idmutinvar AND t.no_disposition = t4.no_disposition
        LEFT JOIN {0}.disposition t3 ON t2.idmutation=t3.idmutation AND t.no_disposition=t3.nodispo         
        WHERE iddispo IS NULL
        GROUP BY t2.idmutation, t.no_disposition,t.valeur_fonciere, t4.nblot, t.code_departement        
    ) tt
    GROUP BY tt.idmutation, tt.no_disposition, tt.valeur_fonciere, tt.nblot, tt.code_departement    
);

## MAJ_TABLE_PARCELLE
-- insertion table parcelle
INSERT INTO {0}.parcelle 
(
    idpar, coddep, codcomm, prefsect, nosect, noplan
)
(
    SELECT 
        t.idpar,
        t.code_departement,
        t.code_commune,
        t.prefixe_de_section,
        t.section,
        t.no_plan
    FROM source.{1} t
    LEFT JOIN {0}.parcelle t4 ON t.idpar=t4.idpar
    WHERE t4.idparcelle IS NULL
    GROUP BY t.idpar, t.code_departement, t.code_commune, t.prefixe_de_section, t.section, t.no_plan
);

## MAJ_TABLE_DISPOSITION_PARCELLE
-- insertion table disposition_parcelle
INSERT INTO {0}.disposition_parcelle 
(
    iddispo, idparcelle, idmutation, coddep, datemut, anneemut, moismut 
)
(
    SELECT t3.iddispo, t4.idparcelle, t2.idmutation, t2.coddep, t2.datemut, t.anneemut, t.moismut
    FROM source.{1} t
    LEFT JOIN {0}.mutation t2 ON t.code_service_ch=t2.codservch AND t.reference_document=t2.refdoc 
    LEFT JOIN {0}.disposition t3 ON t2.idmutation=t3.idmutation AND t.no_disposition=t3.nodispo
    LEFT JOIN {0}.parcelle t4 ON t.idpar=t4.idpar
    LEFT JOIN {0}.disposition_parcelle t5 ON t3.iddispo=t5.iddispo AND t4.idparcelle=t5.idparcelle
    WHERE t5.iddispopar IS NULL
    GROUP BY t3.iddispo, t4.idparcelle, t2.idmutation, t2.coddep, t2.datemut, t.anneemut, t.moismut
);

## MAJ_TABLE_ADRESSE
-- insertion table adresse
INSERT INTO {0}.adresse 
(
    novoie, btq, codvoie, typvoie, voie, codepostal, commune, idadrinvar, coddep
)
(
    SELECT t.no_voie, t.b_t_q, t.code_voie, t.type_de_voie, t.voie, t.code_postal, t.commune, t.idadr_tmp, t.code_departement
    FROM source.{1} t
    LEFT JOIN {0}.adresse t6 ON t.idadr_tmp=t6.idadrinvar -- AND t.idadr_tmp not like '%$$$$%' -- eviter les pbs d adresses non renseignees pour contraintes d unicites (ID Bertrand à etudier - m'a fait planter le script / j'aurai tendance a le mettre sur la ligne du dessous)
    WHERE t6.idadresse IS NULL 
    GROUP BY t.no_voie, t.b_t_q, t.code_voie, t.type_de_voie, t.voie, t.code_postal, t.commune, t.idadr_tmp, t.code_departement
    ORDER BY t.no_voie, t.b_t_q, t.code_voie, t.type_de_voie, t.voie, t.code_postal, t.commune, t.idadr_tmp, t.code_departement
);

## CREER_TABLE_TEMPORAIRE_INTERMEDIAIRE
CREATE TABLE source.{1}_tmp AS
(
    SELECT 
        t2.idmutation,
        t2.coddep,
        t2.datemut,
        t2.anneemut,
        t2.moismut,
        t5.iddispopar,
        t3.iddispo, 
        t4.idparcelle,
        t4.idpar,
        -- pour table "volume"
        no_volume, 
        -- pour table "lot"
        "1er_lot", surface_carrez_du_1er_lot, "2eme_lot", surface_carrez_du_2eme_lot, 
        "3eme_lot", surface_carrez_du_3eme_lot, "4eme_lot", surface_carrez_du_4eme_lot, 
        "5eme_lot", surface_carrez_du_5eme_lot, 
        -- pour table "local"
        code_type_local, type_local, identifiant_local, surface_reelle_bati, nombre_pieces_principales, idloc,
        -- pour table "suf" 
        nature_culture, nature_culture_speciale, surface_terrain, nodcnt,
        -- pour table "adresse"
        no_voie, b_t_q, type_de_voie, code_voie, voie, code_postal, commune,
        -- pour tous
        idadr_tmp, idsuf_tmp
    FROM source.{1} t
    LEFT JOIN {0}.mutation t2 ON t.code_service_ch=t2.codservch AND t.reference_document=t2.refdoc 
    LEFT JOIN {0}.disposition t3 ON t2.idmutation=t3.idmutation AND t.no_disposition=t3.nodispo
    LEFT JOIN {0}.parcelle t4 ON t.idpar=t4.idpar
    LEFT JOIN {0}.disposition_parcelle t5 ON t3.iddispo=t5.iddispo AND t4.idparcelle=t5.idparcelle
);

## MAJ_TABLE_LOCAL
-- insertion table local

INSERT INTO {0}.local 
(
    idmutation, iddispopar, idpar, idloc, identloc, codtyploc, libtyploc, nbpprinc, sbati, coddep, datemut, anneemut, moismut
)
(
    SELECT t.idmutation, t.iddispopar, t.idpar, t.idloc, t.identifiant_local, first(t.code_type_local) AS codtyploc, first(t.type_local), first(t.nombre_pieces_principales), first(t.surface_reelle_bati), t.coddep, t.datemut, t.anneemut, t.moismut
    FROM source.{1}_tmp t
    LEFT JOIN {0}.local t7 ON t.iddispopar=t7.iddispopar AND t.identifiant_local=t7.identloc
   WHERE 
        t7.iddispoloc IS NULL
        AND t.identifiant_local IS NOT NULL
    GROUP BY t.idmutation, t.iddispopar, t.idpar, t.idloc, t.identifiant_local, t.coddep, t.datemut, t.anneemut, t.moismut
    ORDER BY t.idmutation, t.iddispopar, t.identifiant_local, codtyploc
);

## MAJ_TABLE_VOLUME
-- insertion table volume
INSERT INTO {0}.volume 
(
    iddispopar, idmutation, novolume, coddep
)
(
    SELECT t.iddispopar, t.idmutation, t.no_volume, t.coddep
    FROM source.{1}_tmp t
    LEFT JOIN {0}.volume t8 ON t.iddispopar=t8.iddispopar AND t.no_volume=t8.novolume
    WHERE 
        t8.iddispovol IS NULL
        AND t.no_volume IS NOT NULL
    GROUP BY t.iddispopar, t.idmutation, t.no_volume, t.coddep
    ORDER BY t.iddispopar, t.idmutation, t.no_volume
);

## MAJ_TABLE_SUF
DROP TABLE IF EXISTS source.tmp_suf;
CREATE TABLE source.tmp_suf AS
(
    SELECT
        iddispopar, idmutation, count(*) as nb_suf_idt, surface_terrain, nature_culture, nature_culture_speciale, idsuf_tmp, coddep, nodcnt
    FROM 
        source.{1}_tmp t1
    WHERE
        nature_culture IS NOT NULL
    GROUP BY 
        iddispopar, idmutation, surface_terrain, nature_culture, nature_culture_speciale, idsuf_tmp, coddep, nodcnt
);
-- insertion table suf
INSERT INTO {0}.suf 
(
    iddispopar, idmutation, nbsufidt, sterr, natcult, natcultspe, idsufinvar, coddep, nodcnt
)
(
    SELECT
        t1.iddispopar, t1.idmutation, t1.nb_suf_idt/t2.pgcd_nb_suf_idt AS nb_suf_idt, t1.surface_terrain*t1.nb_suf_idt/t2.pgcd_nb_suf_idt AS surface_terrain, t1.nature_culture, t1.nature_culture_speciale, t1.idsuf_tmp, t1.coddep, t1.nodcnt
    FROM 
        source.tmp_suf t1
    LEFT JOIN
        (SELECT iddispopar, pgcd(array_agg(nb_suf_idt)::integer[]) AS pgcd_nb_suf_idt FROM source.tmp_suf GROUP BY iddispopar) t2
    ON
        t1.iddispopar=t2.iddispopar
    LEFT JOIN
        {0}.suf t3
    ON 
        t1.iddispopar=t3.iddispopar
    WHERE
        t3.iddispopar IS NULL
);
DROP TABLE IF EXISTS source.tmp_suf;

## MAJ_TABLE_LOT
-- insertion table lot
INSERT INTO {0}.lot
(
    iddispopar, idmutation, iddispoloc, nolot, scarrez, coddep
)
(
    SELECT t.iddispopar, t.idmutation, t.iddispoloc, t.nolot, t.surf, t.coddep
    FROM
    (
        SELECT t1.iddispopar, t1.idmutation, t3.iddispoloc, t1."1er_lot" as nolot, surface_carrez_du_1er_lot as surf, t1.coddep
        FROM source.{1}_tmp t1
        LEFT JOIN {0}.lot t2 ON t1.iddispopar=t2.iddispopar AND t2.nolot=t1."1er_lot"
        LEFT JOIN {0}.local t3 ON t1.iddispopar=t3.iddispopar AND t1.identifiant_local=t3.identloc
        WHERE 
            t1."1er_lot" IS NOT NULL 
            AND t2.iddispolot IS NULL
        
        UNION
    
        SELECT t1.iddispopar, t1.idmutation, t3.iddispoloc, t1."2eme_lot" as nolot, surface_carrez_du_2eme_lot as surf, t1.coddep
        FROM source.{1}_tmp t1
        LEFT JOIN {0}.lot t2 ON t1.iddispopar=t2.iddispopar AND t2.nolot=t1."2eme_lot"
        LEFT JOIN {0}.local t3 ON t1.iddispopar=t3.iddispopar AND t1.identifiant_local=t3.identloc
        WHERE 
            t1."2eme_lot" IS NOT NULL 
            AND t2.iddispolot IS NULL
        
        UNION
    
        SELECT t1.iddispopar, t1.idmutation, t3.iddispoloc, t1."3eme_lot" as nolot, surface_carrez_du_3eme_lot as surf, t1.coddep
        FROM source.{1}_tmp t1
        LEFT JOIN {0}.lot t2 ON t1.iddispopar=t2.iddispopar AND t2.nolot=t1."3eme_lot"
        LEFT JOIN {0}.local t3 ON t1.iddispopar=t3.iddispopar AND t1.identifiant_local=t3.identloc
        WHERE 
            t1."3eme_lot" IS NOT NULL 
            AND t2.iddispolot IS NULL
        
        UNION
    
        SELECT t1.iddispopar, t1.idmutation, t3.iddispoloc, t1."4eme_lot" as nolot, surface_carrez_du_4eme_lot as surf, t1.coddep
        FROM source.{1}_tmp t1
        LEFT JOIN {0}.lot t2 ON t1.iddispopar=t2.iddispopar AND t2.nolot=t1."4eme_lot"
        LEFT JOIN {0}.local t3 ON t1.iddispopar=t3.iddispopar AND t1.identifiant_local=t3.identloc
        WHERE 
            t1."4eme_lot" IS NOT NULL 
            AND t2.iddispolot IS NULL
        
        UNION
    
        SELECT t1.iddispopar, t1.idmutation, t3.iddispoloc, t1."5eme_lot" as nolot, surface_carrez_du_5eme_lot as surf, t1.coddep
        FROM source.{1}_tmp t1
        LEFT JOIN {0}.lot t2 ON t1.iddispopar=t2.iddispopar AND t2.nolot=t1."5eme_lot"
        LEFT JOIN {0}.local t3 ON t1.iddispopar=t3.iddispopar AND t1.identifiant_local=t3.identloc
        WHERE 
            t1."5eme_lot" IS NOT NULL 
            AND t2.iddispolot IS NULL
    ) t
    GROUP BY t.iddispopar, t.idmutation, t.iddispoloc, t.nolot, t.surf, t.coddep
);

## MAJ_TABLES_PASSAGES_ADRESSES
DROP TABLE IF EXISTS source.tmp_adresse_dispoparc_local;
CREATE TABLE source.tmp_adresse_dispoparc_local AS
(
    SELECT t3.idadresse, t2.iddispoloc, t.iddispopar, t.coddep, t.idmutation
    FROM source.{1}_tmp t
    LEFT JOIN {0}.local t2 ON t.iddispopar=t2.iddispopar AND t.identifiant_local=t2.identloc
    LEFT JOIN {0}.adresse t3 ON t.idadr_tmp=t3.idadrinvar
);
    -- insertion adresse_dispoparc
    INSERT INTO {0}.adresse_dispoparc 
    (
        idadresse, iddispopar, coddep, idmutation
    )
    (
        SELECT t1.idadresse, t1.iddispopar, t1.coddep, t1.idmutation
        FROM source.tmp_adresse_dispoparc_local t1
        LEFT JOIN {0}.adresse_dispoparc t2
        ON t1.idadresse=t2.idadresse AND t1.iddispopar=t2.iddispopar
        WHERE 
            t1.idadresse IS NOT NULL
            AND t1.iddispopar IS NOT NULL
            AND t2.idadresse IS NULL
            AND t2.iddispopar IS NULL
        GROUP BY t1.idadresse, t1.iddispopar, t1.coddep, t1.idmutation
        
    );

    -- insertion adresse_local
    INSERT INTO {0}.adresse_local 
    (
        idadresse, iddispoloc, coddep, idmutation
    )
    (
        SELECT t1.idadresse, t1.iddispoloc, t1.coddep, t1.idmutation
        FROM source.tmp_adresse_dispoparc_local t1
        LEFT JOIN {0}.adresse_local t2
        ON t1.idadresse=t2.idadresse AND t1.iddispoloc=t2.iddispoloc
        WHERE 
            t1.idadresse IS NOT NULL
            AND t1.iddispoloc IS NOT NULL
            AND t2.idadresse IS NULL
            AND t2.iddispoloc IS NULL
        GROUP BY t1.idadresse, t1.iddispoloc, t1.coddep, t1.idmutation
        
    );
DROP TABLE IF EXISTS source.tmp_adresse_dispoparc_local;

