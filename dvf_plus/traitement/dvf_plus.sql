## CREER_TABLE_CALCUL_PARCELLE_IDDISPOPAR
CREATE TABLE {0}.tmp_calcul_parcelle_iddispopar AS(         
    SELECT
        t.iddispopar, 
        tt.*
    FROM
        {0}.disposition_parcelle t
    LEFT JOIN
        {0}.parcelle tt
        ON t.idparcelle = tt.idparcelle

);

## CREER_TABLE_CALCUL_SUF_IDDISPOPAR
CREATE TABLE {0}.tmp_parcelle_mutee_rangee AS(
    SELECT 
        idparcelle, datemut, anneemut, l_idmut, l_iddispopar, nbmutjour, rank() OVER (PARTITION BY idparcelle ORDER BY datemut) as rang
    FROM(
        SELECT 
            t.idparcelle, tt.datemut, tt.anneemut, array_agg(DISTINCT tt.idmutation) as l_idmut, array_agg(DISTINCT t.iddispopar) As l_iddispopar, count(DISTINCT tt.idmutation) as nbmutjour
        FROM(
            SELECT t1.idmutation, t1.iddispopar, t1.idparcelle
            FROM {0}.disposition_parcelle t1 
            JOIN {0}.suf t2 
            ON t1.iddispopar = t2.iddispopar 
            GROUP BY t1.idmutation, t1.iddispopar, t1.idparcelle
            ) t
        JOIN {0}.mutation tt
        ON t.idmutation = tt.idmutation
        GROUP BY t.idparcelle, tt.datemut, tt.anneemut
    ) ta
    --ORDER BY idparcelle, datemut
);
ALTER TABLE {0}.tmp_parcelle_mutee_rangee ADD PRIMARY KEY (idparcelle,datemut);

CREATE TABLE {0}.tmp_calcul_suf_iddispopar01 AS(        
    SELECT
        t1.iddispopar,
        t1.idparcelle,
        t1.datemut,
        CASE WHEN t3.idparcelle IS NULL THEN false ELSE true END::BOOLEAN AS parcvendue,
        COALESCE(t3.nbmutjour, 0)::INTEGER AS nbmutjour,
        COALESCE(t4.nbmutannee, 0)::INTEGER AS nbmutannee,
        CASE WHEN t5.datemut IS NULL THEN (SELECT max(datemut) FROM {0}.tmp_parcelle_mutee_rangee WHERE idparcelle = t1.idparcelle AND datemut < t1.datemut) ELSE t5.datemut END AS datemutpre,
        CASE WHEN t6.datemut IS NULL THEN (SELECT min(datemut) FROM {0}.tmp_parcelle_mutee_rangee WHERE idparcelle = t1.idparcelle AND datemut > t1.datemut) ELSE t6.datemut END AS datemutsui,
        t7.dcnt01,
        t7.dcnt02,
        t7.dcnt03,
        t7.dcnt04,
        t7.dcnt05,
        t7.dcnt06,
        t7.dcnt07,
        t7.dcnt08,
        t7.dcnt09,
        t7.dcnt10,
        t7.dcnt11,
        t7.dcnt12,
        t7.dcnt13
    FROM {0}.disposition_parcelle t1
    LEFT JOIN {0}.tmp_parcelle_mutee_rangee t3
    ON t1.idparcelle = t3.idparcelle AND t1.datemut = t3.datemut AND ARRAY[t1.iddispopar]::INTEGER[] && t3.l_iddispopar
    LEFT JOIN (SELECT idparcelle, anneemut, sum(nbmutjour) AS nbmutannee FROM {0}.tmp_parcelle_mutee_rangee GROUP BY idparcelle, anneemut) t4
    ON t1.idparcelle = t4.idparcelle AND t1.anneemut = t4.anneemut
    LEFT JOIN {0}.tmp_parcelle_mutee_rangee    t5
    ON t1.idparcelle = t5.idparcelle AND t3.rang-1 = t5.rang
    LEFT JOIN {0}.tmp_parcelle_mutee_rangee    t6
    ON t1.idparcelle = t6.idparcelle AND t3.rang+1 = t6.rang
    LEFT JOIN 
    (    SELECT 
            iddispopar,
            COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 1 THEN 1 ELSE 0 END),0) AS dcnt01,
            COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 2 THEN 1 ELSE 0 END),0) AS dcnt02,
            COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 3 THEN 1 ELSE 0 END),0) AS dcnt03,
            COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 4 THEN 1 ELSE 0 END),0) AS dcnt04,
            COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 5 THEN 1 ELSE 0 END),0) AS dcnt05,
            COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 6 THEN 1 ELSE 0 END),0) AS dcnt06,
            COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 7 THEN 1 ELSE 0 END),0) AS dcnt07,
            COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 8 THEN 1 ELSE 0 END),0) AS dcnt08,
            COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 9 THEN 1 ELSE 0 END),0) AS dcnt09,
            COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 10 THEN 1 ELSE 0 END),0) AS dcnt10,
            COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 11 THEN 1 ELSE 0 END),0) AS dcnt11,
            COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 12 THEN 1 ELSE 0 END),0) AS dcnt12,
            COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 13 THEN 1 ELSE 0 END),0) AS dcnt13
        FROM  (SELECT m.iddispopar, t.iddisposuf, t.nbsufidt, t.sterr, t.nodcnt FROM {0}.disposition_parcelle m LEFT JOIN {0}.suf t ON m.iddispopar = t.iddispopar) tt
        GROUP BY iddispopar
    ) t7
    ON  t1.iddispopar = t7.iddispopar
);

CREATE TABLE {0}.tmp_calcul_suf_iddispopar AS(
SELECT
    iddispopar,
    datemut,
    parcvendue,
    nbmutjour,
    nbmutannee,
    datemutpre,
    (SELECT l_idmut FROM {0}.tmp_parcelle_mutee_rangee tpre WHERE tpre.idparcelle = tf.idparcelle and tpre.datemut = tf.datemutpre) AS l_idmutpre,
    datemutsui,
    (SELECT l_idmut FROM {0}.tmp_parcelle_mutee_rangee tsui WHERE tsui.idparcelle = tf.idparcelle and tsui.datemut = tf.datemutsui) AS l_idmutsui,
    dcnt01,
    dcnt02,
    dcnt03,
    dcnt04,
    dcnt05,
    dcnt06,
    dcnt07,
    dcnt08,
    dcnt09,
    dcnt10,
    dcnt11,
    dcnt12,
    dcnt13,
    dcnt09 + dcnt10 + dcnt11 + dcnt12+ dcnt13 AS dcntsol,
    dcnt01 + dcnt02 + dcnt03 + dcnt04 AS dcntagri,
    dcnt06 + dcnt07 AS dcntnat
FROM {0}.tmp_calcul_suf_iddispopar01 tf
);

## CREER_TABLE_CALCUL_LOCAL_IDDISPOLOC
CREATE TABLE {0}.tmp_local_mutee_rangee AS(
    SELECT 
        idloc, datemut, anneemut, l_idmut, nbmutjour, rank() OVER (PARTITION BY idloc ORDER BY datemut) as rang
    FROM(
        SELECT 
            t.idloc, tt.datemut, tt.anneemut, array_agg(DISTINCT tt.idmutation) as l_idmut, count(DISTINCT tt.idmutation) as nbmutjour
        FROM(
            SELECT t1.idmutation, t1.idloc
            FROM {0}.local t1  
            GROUP BY t1.idmutation, t1.idloc
            ) t
        JOIN {0}.mutation tt
        ON t.idmutation = tt.idmutation
        GROUP BY t.idloc, tt.datemut, tt.anneemut
    ) ta
    --ORDER BY idloc, datemut
);
ALTER TABLE {0}.tmp_local_mutee_rangee ADD PRIMARY KEY (idloc,datemut);

CREATE TABLE {0}.tmp_calcul_local_iddispoloc AS(
SELECT
    t1.iddispoloc, 
    CASE WHEN t3.nbmutjour IS NULL THEN 0 ELSE t3.nbmutjour END::INTEGER AS nbmutjour,
    CASE WHEN t4.nbmutannee IS NULL THEN 0 ELSE t4.nbmutannee END::INTEGER AS nbmutannee,
    t5.datemut AS datemutpre,
    t5.l_idmut AS l_idmutpre,
    t6.datemut AS datemutsui, 
    t6.l_idmut AS l_idmutsui
FROM {0}.local t1
LEFT JOIN {0}.tmp_local_mutee_rangee    t3
ON t1.idloc = t3.idloc AND t1.datemut = t3.datemut
LEFT JOIN (SELECT idloc, anneemut, sum(nbmutjour) AS nbmutannee FROM {0}.tmp_local_mutee_rangee GROUP BY idloc, anneemut) t4
ON t1.idloc = t4.idloc AND t1.anneemut = t4.anneemut
LEFT JOIN {0}.tmp_local_mutee_rangee t5
ON t1.idloc = t5.idloc AND t3.rang-1 = t5.rang
LEFT JOIN {0}.tmp_local_mutee_rangee    t6
ON t1.idloc = t6.idloc AND t3.rang+1 = t6.rang
);

## CREER_TABLE_CALCUL_ANN_NATURE_MUTATION_IDMUTATION
CREATE TABLE {0}.tmp_calcul_ann_nature_mutation_idmutation AS(         
    SELECT
        t1.idmutation, 
        t2.libnatmut
    FROM
        {0}.mutation t1
    LEFT JOIN
        {1}.ann_nature_mutation t2
    ON 
        t1.idnatmut = t2.idnatmut
);

## CREER_TABLE_CALCUL_MUTATION_ARTICLE_CGI_IDMUTATION
CREATE TABLE {0}.tmp_calcul_mutation_article_cgi_idmutation AS(         
    SELECT
        idmutation, 
        COALESCE(count(DISTINCT idartcgi), 0) AS nbartcgi,
        array_supprimer_null(array_agg(artcgi)) AS l_artcgi
        --
        -- A partir de la version 9.3 de PostgreSQL:
        -- array_remove(array_agg(artcgi), NULL) AS l_artcgi
    FROM
        (SELECT m.idmutation, t.idartcgi, tt.artcgi FROM {0}.mutation m LEFT JOIN {0}.mutation_article_cgi t ON m.idmutation = t.idmutation LEFT JOIN dvf_annexe.ann_cgi tt ON t.idartcgi = tt.idartcgi ORDER BY t.ordarticgi ASC) t1
    GROUP BY idmutation
);

## CREER_TABLE_CALCUL_VEFA_IDMUTATION
CREATE TABLE {0}.tmp_calcul_annexes_idmutation AS(         
    SELECT
        t1.idmutation, 
        CASE 
            WHEN t1.libnatmut = 'Vente en l''état futur d''achèvement' THEN TRUE
            WHEN '1594FQA*2'= ANY(t2.l_artcgi) THEN TRUE
            WHEN '257-7-1*2'= ANY(t2.l_artcgi) THEN TRUE
            WHEN '296-1-a-*2'= ANY(t2.l_artcgi) THEN TRUE
            WHEN '296-1-b-*2'= ANY(t2.l_artcgi) THEN TRUE
        ELSE FALSE
        END AS vefa
    FROM
    {0}.tmp_calcul_ann_nature_mutation_idmutation t1
    LEFT JOIN
    {0}.tmp_calcul_mutation_article_cgi_idmutation t2
    ON 
    t1.idmutation = t2.idmutation
);

## CREER_TABLE_CALCUL_DISPOSITION_IDMUTATION
CREATE TABLE {0}.tmp_calcul_disposition_idmutation AS(         
    SELECT
        idmutation, 
        sum(valeurfonc) AS valeurfonc, 
        count(DISTINCT iddispo) AS nbdispo, 
        sum(nblot) AS nblot 
    FROM
        {0}.disposition
    GROUP BY idmutation

);

## CREER_TABLE_CALCUL_DISPOSITION_PARCELLE_IDMUTATION
-- RAPPEL IMPORTANT : Une même parcelle peut apparaître deux fois dans la table disposition_parcelle pour un même idmutation (elle peut apparaître dans 2 dispositions différentes)
CREATE TABLE  {0}.tmp_calcul_disposition_parcelle_idmutation AS(

    SELECT 
        t.idmutation,
        count(DISTINCT t.codcomm) as nbcomm,
        array_agg(DISTINCT (t.coddep || t.codcomm)) as l_codinsee,
        count(DISTINCT t.nosect) as nbsection,
        array_agg(DISTINCT t.nosect) as l_section,
        count(DISTINCT t.idpar) as nbpar,
        array_agg(DISTINCT t.idpar) AS l_idpar,
        count(DISTINCT tt.idpar) AS nbparmut,
        array_supprimer_null(array_agg(DISTINCT tt.idpar)) AS l_idparmut
        --
        -- A partir de la version 9.3 de PostgreSQL:
        -- array_remove(array_agg(DISTINCT tt.idpar), NULL) AS l_idparmut
    FROM  {0}.disposition_parcelle{1} t
    LEFT JOIN (SELECT iddispopar, idpar FROM {0}.disposition_parcelle{1} WHERE parcvendue = TRUE) tt
    ON t.iddispopar = tt.iddispopar
    GROUP BY t.idmutation                    

);

## CREER_TABLE_CALCUL_SUF_IDMUTATION
CREATE TABLE {0}.tmp_calcul_suf_idmutation AS(

    SELECT 
        idmutation,
        COALESCE(count(DISTINCT iddisposuf), 0) as nbsuf,
        COALESCE(sum(nbsufidt * sterr), 0) AS sterr,
        ARRAY[COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 1 THEN 1 ELSE 0 END),0),
        COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 2 THEN 1 ELSE 0 END),0),
        COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 3 THEN 1 ELSE 0 END),0),
        COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 4 THEN 1 ELSE 0 END),0),
        COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 5 THEN 1 ELSE 0 END),0),
        COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 6 THEN 1 ELSE 0 END),0),
        COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 7 THEN 1 ELSE 0 END),0),
        COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 8 THEN 1 ELSE 0 END),0),
        COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 9 THEN 1 ELSE 0 END),0),
        COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 10 THEN 1 ELSE 0 END),0),
        COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 11 THEN 1 ELSE 0 END),0),
        COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 12 THEN 1 ELSE 0 END),0),
        COALESCE(sum(nbsufidt * sterr * CASE WHEN nodcnt = 13 THEN 1 ELSE 0 END),0)]::NUMERIC[] as l_dcnt
    FROM  (
			SELECT m.idmutation, t.iddisposuf, t.nbsufidt, t.sterr, t.nodcnt 
			FROM (SELECT DISTINCT ON (idmutation, idpar) *  FROM {0}.disposition_parcelle) m
			LEFT JOIN {0}.suf t 
			ON m.idmutation = t.idmutation AND m.iddispopar = t.iddispopar
			) tt 
    GROUP BY idmutation                    

);

## CREER_TABLE_CALCUL_VOLUME_IDMUTATION
CREATE TABLE {0}.tmp_calcul_volume_idmutation AS(

    SELECT 
        idmutation,
        count(iddispovol) AS nbvolmut
    FROM  (SELECT m.idmutation, v.iddispovol FROM {0}.mutation m LEFT JOIN {0}.volume v ON m.idmutation = v.idmutation) t    
    GROUP BY idmutation
);

## CREER_TABLE_CALCUL_LOCAL_IDMUTATION
-- RAPPEL IMPORTANT : Un même local peut apparaître deux fois dans la table local pour un même idmutation (il peut apparaître dans 2 dispositions différentes)
CREATE TABLE {0}.tmp_calcul_local_idmutation AS(

    SELECT 
        idmutation,
        COALESCE(count(DISTINCT idloc),0) as nblocmut,
        array_supprimer_null(array_agg(DISTINCT idloc)) as l_idlocmut,
        --
        -- à partir de la version 9.3
        -- array_remove(array_agg(idloc), NULL) as l_idlocmut,
        COALESCE(sum(CASE WHEN codtyploc = 1 THEN 1 ELSE 0 END),0) AS nblocmai,
        COALESCE(sum(CASE WHEN codtyploc = 2 THEN 1 ELSE 0 END),0) AS nblocapt,
        COALESCE(sum(CASE WHEN codtyploc = 3 THEN 1 ELSE 0 END),0) AS nblocdep,
        COALESCE(sum(CASE WHEN codtyploc = 4 THEN 1 ELSE 0 END),0) AS nblocact,
        COALESCE(sum(CASE WHEN codtyploc = 2 AND nbpprinc <= 1 THEN 1 ELSE 0 END),0) AS nbapt1pp,
        COALESCE(sum(CASE WHEN codtyploc = 2 AND nbpprinc = 2 THEN 1 ELSE 0 END),0) AS nbapt2pp,
        COALESCE(sum(CASE WHEN codtyploc = 2 AND nbpprinc = 3 THEN 1 ELSE 0 END),0) AS nbapt3pp,
        COALESCE(sum(CASE WHEN codtyploc = 2 AND nbpprinc = 4 THEN 1 ELSE 0 END),0) AS nbapt4pp,
        COALESCE(sum(CASE WHEN codtyploc = 2 AND nbpprinc > 4 THEN 1 ELSE 0 END),0) AS nbapt5pp,
        COALESCE(sum(CASE WHEN codtyploc = 1 AND nbpprinc <= 1 THEN 1 ELSE 0 END),0) AS nbmai1pp,
        COALESCE(sum(CASE WHEN codtyploc = 1 AND nbpprinc = 2 THEN 1 ELSE 0 END),0) AS nbmai2pp,
        COALESCE(sum(CASE WHEN codtyploc = 1 AND nbpprinc = 3 THEN 1 ELSE 0 END),0) AS nbmai3pp,
        COALESCE(sum(CASE WHEN codtyploc = 1 AND nbpprinc = 4 THEN 1 ELSE 0 END),0) AS nbmai4pp,
        COALESCE(sum(CASE WHEN codtyploc = 1 AND nbpprinc > 4 THEN 1 ELSE 0 END),0) AS nbmai5pp,
        COALESCE(sum(sbati),0) AS sbati,
        COALESCE(sum(CASE WHEN codtyploc = 1 THEN sbati ELSE 0 END),0) AS sbatmai,
        COALESCE(sum(CASE WHEN codtyploc = 2 THEN sbati ELSE 0 END),0) AS sbatapt,
        COALESCE(sum(CASE WHEN codtyploc = 4 THEN sbati ELSE 0 END),0) AS sbatact,
        COALESCE(sum(CASE WHEN codtyploc = 2 AND nbpprinc <= 1 THEN sbati ELSE 0 END),0) AS sapt1pp,
        COALESCE(sum(CASE WHEN codtyploc = 2 AND nbpprinc = 2 THEN sbati ELSE 0 END),0) AS sapt2pp,
        COALESCE(sum(CASE WHEN codtyploc = 2 AND nbpprinc = 3 THEN sbati ELSE 0 END),0) AS sapt3pp,
        COALESCE(sum(CASE WHEN codtyploc = 2 AND nbpprinc = 4 THEN sbati ELSE 0 END),0) AS sapt4pp,
        COALESCE(sum(CASE WHEN codtyploc = 2 AND nbpprinc > 4 THEN sbati ELSE 0 END),0) AS sapt5pp,
        COALESCE(sum(CASE WHEN codtyploc = 1 AND nbpprinc <= 1 THEN sbati ELSE 0 END),0) AS smai1pp,
        COALESCE(sum(CASE WHEN codtyploc = 1 AND nbpprinc = 2 THEN sbati ELSE 0 END),0) AS smai2pp,
        COALESCE(sum(CASE WHEN codtyploc = 1 AND nbpprinc = 3 THEN sbati ELSE 0 END),0) AS smai3pp,
        COALESCE(sum(CASE WHEN codtyploc = 1 AND nbpprinc = 4 THEN sbati ELSE 0 END),0) AS smai4pp,
        COALESCE(sum(CASE WHEN codtyploc = 1 AND nbpprinc > 4 THEN sbati ELSE 0 END),0) AS smai5pp
    FROM (SELECT m.idmutation, l.idloc, l.codtyploc, l.nbpprinc, l.sbati 
			FROM {0}.mutation m 
			LEFT JOIN {0}.local l 
			ON m.idmutation = l.idmutation
			GROUP BY m.idmutation, l.idloc, l.codtyploc, l.nbpprinc, l.sbati) t      
    GROUP BY idmutation
); 

## CREER_INDEX_GIN_CHAMP_LCODINSEE
DROP INDEX IF EXISTS l_codinsee_idx_gin_{0};
CREATE INDEX --IF NOT EXISTS -- a partir de la 9.5
l_codinsee_idx_gin_{0} ON {0}.mutation USING GIN (l_codinsee);
DROP INDEX IF EXISTS datemut_mutation_idx_btree_{0};
CREATE INDEX --IF NOT EXISTS -- a partir de la 9.5
datemut_mutation_idx_btree_{0} ON {0}.mutation USING BTREE (datemut);
DROP INDEX IF EXISTS codcomm_idx_btree_{0};
CREATE INDEX --IF NOT EXISTS -- a partir de la 9.5
codcomm_idx_btree_{0} ON {0}.disposition_parcelle USING BTREE (codcomm);
DROP INDEX IF EXISTS datemut_parcelle_idx_btree_{0};
CREATE INDEX --IF NOT EXISTS -- a partir de la 9.5
datemut_parcelle_idx_btree_{0} ON {0}.disposition_parcelle USING BTREE (datemut);
DROP INDEX IF EXISTS idpar_local_idx_btree_{0};
CREATE INDEX --IF NOT EXISTS -- a partir de la 9.5
idpar_local_idx_btree_{0} ON {0}.local USING BTREE (idpar);
DROP INDEX IF EXISTS datemut_local_idx_btree_{0};
CREATE INDEX --IF NOT EXISTS -- a partir de la 9.5
datemut_local_idx_btree_{0} ON {0}.local USING BTREE (datemut);