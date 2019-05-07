## RECUPERER_COMMUNES_A_GEOLOCALISER
SELECT DISTINCT coddep || codcomm as codinsee FROM dvf.disposition_parcelle ORDER BY codinsee;

## CREER_EXTENSION_POSTGIS
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

## CREER_CHAMPS_GEOMETRIQUES
ALTER TABLE dvf.local ADD COLUMN geomloc geometry;
ALTER TABLE dvf.disposition_parcelle ADD COLUMN geomloc geometry;
ALTER TABLE dvf.disposition_parcelle ADD COLUMN geompar geometry;
ALTER TABLE dvf.mutation ADD COLUMN geomlocmut geometry;
ALTER TABLE dvf.mutation ADD COLUMN geomparmut geometry;
ALTER TABLE dvf.mutation ADD COLUMN geompar geometry;

## CREER_CHAMPS_TYPO_BIENS
ALTER TABLE dvf.mutation ADD COLUMN codtypbien VARCHAR(6);
ALTER TABLE dvf.mutation ADD COLUMN libtypbien TEXT;

## MISE_A_JOUR_GEOMETRIES_LOCAL_POUR_DEPARTEMENT_DEPUIS
UPDATE {2}.local l
SET geomloc = t.geomloc
FROM {0}.{1} t
WHERE l.idpar = t.idpar;

## MISE_A_JOUR_GEOMETRIES_DISPOSITION_PARCELLE_POUR_DEPARTEMENT_DEPUIS
UPDATE {2}.disposition_parcelle d
SET geomloc = t.geomloc, geompar = ST_MULTI(t.geompar)
FROM {0}.{1} t
WHERE d.idpar = t.idpar;

## MISE_A_JOUR_GEOMETRIES_MUTATION_POUR_DEPARTEMENT
UPDATE {0}.mutation m
SET geomlocmut = t.geomlocmut
FROM (SELECT idmutation, ST_UNION(geomloc) AS geomlocmut FROM {0}.local GROUP BY idmutation) t
WHERE m.idmutation = t.idmutation;

UPDATE {0}.mutation m
SET geomparmut = ST_MULTI(t.geomparmut)
FROM (SELECT idmutation, ST_UNION(geompar) AS geomparmut FROM {0}.disposition_parcelle WHERE parcvendue IS TRUE GROUP BY idmutation) t
WHERE m.idmutation = t.idmutation;

UPDATE {0}.mutation m
SET geompar = ST_MULTI(t.geompar)
FROM (SELECT idmutation, ST_UNION(geompar) AS geompar FROM {0}.disposition_parcelle GROUP BY idmutation) t
WHERE m.idmutation = t.idmutation;

## CREER_INDEX_GEOMETRIQUES
-- Table mutation
DROP INDEX IF EXISTS geompar_mutation_gist_{0};
CREATE INDEX --IF NOT EXISTS 
geompar_mutation_gist_{0} ON {0}.mutation USING gist (geompar);
DROP INDEX IF EXISTS geomparmut_mutation_gist_{0};
CREATE INDEX --IF NOT EXISTS 
geomparmut_mutation_gist_{0} ON {0}.mutation USING gist (geomparmut);
DROP INDEX IF EXISTS geomlocmut_mutation_gist_{0};
CREATE INDEX --IF NOT EXISTS 
geomlocmut_mutation_gist_{0} ON {0}.mutation USING gist (geomlocmut);
--Table disposition_parcelle
DROP INDEX IF EXISTS geompar_parcelle_gist_{0};
CREATE INDEX --IF NOT EXISTS 
geompar_parcelle_gist_{0} ON {0}.disposition_parcelle USING gist (geompar);
DROP INDEX IF EXISTS geomloc_parcelle_gist_{0};
CREATE INDEX --IF NOT EXISTS 
geomloc_parcelle_gist_{0} ON {0}.disposition_parcelle USING gist (geomloc);
-- Table local
DROP INDEX IF EXISTS geomloc_local_gist_{0};
CREATE INDEX --IF NOT EXISTS 
geomloc_local_gist_{0} ON {0}.local USING gist (geomloc);

## CREER_CONTRAINTES_GEOMETRIQUES
-- Table mutation
--ALTER TABLE {0}.mutation ADD CONSTRAINT enforce_dims_mutation_geomlocmut CHECK (st_ndims(geomlocmut) = 2);
--ALTER TABLE {0}.mutation ADD CONSTRAINT enforce_dims_mutation_geompar CHECK (st_ndims(geompar) = 2);
--ALTER TABLE {0}.mutation ADD CONSTRAINT enforce_dims_mutation_geomparmut CHECK (st_ndims(geomparmut) = 2);
--ALTER TABLE {0}.mutation ADD CONSTRAINT enforce_geotype_mutation_geomlocmut CHECK (geometrytype(geomlocmut) = 'MULTIPOINT'::text OR geometrytype(geomlocmut) = 'POINT'::text OR geomlocmut IS NULL);
--ALTER TABLE {0}.mutation ADD CONSTRAINT enforce_geotype_mutation_geompar CHECK (geometrytype(geompar) = 'MULTIPOLYGON'::text OR geompar IS NULL);
--ALTER TABLE {0}.mutation ADD CONSTRAINT enforce_geotype_mutation_geomparmut CHECK (geometrytype(geomparmut) = 'MULTIPOLYGON'::text OR geomparmut IS NULL);
ALTER TABLE {0}.mutation ADD CONSTRAINT enforce_srid_mutation_geomlocmut CHECK (st_srid(geomlocmut) = {1});
ALTER TABLE {0}.mutation ADD CONSTRAINT enforce_srid_mutation_geompar CHECK (st_srid(geompar) = {1});
ALTER TABLE {0}.mutation ADD CONSTRAINT enforce_srid_mutation_geomparmut CHECK (st_srid(geomparmut) = {1});
-- Table disposition_parcelle
--ALTER TABLE {0}.disposition_parcelle ADD CONSTRAINT enforce_dims_parcelle_geompar CHECK (st_ndims(geompar) = 2);
--ALTER TABLE {0}.disposition_parcelle ADD CONSTRAINT enforce_dims_parcelle_geomloc CHECK (st_ndims(geomloc) = 2);
--ALTER TABLE {0}.disposition_parcelle ADD CONSTRAINT enforce_geotype_parcelle_geompar CHECK (geometrytype(geompar) = 'MULTIPOLYGON'::text OR geompar IS NULL);
--ALTER TABLE {0}.disposition_parcelle ADD CONSTRAINT enforce_geotype_parcelle_geomloc CHECK (geometrytype(geomloc) = 'POINT'::text OR geomloc IS NULL);
ALTER TABLE {0}.disposition_parcelle ADD CONSTRAINT enforce_srid_parcelle_geompar CHECK (st_srid(geompar) = {1});
ALTER TABLE {0}.disposition_parcelle ADD CONSTRAINT enforce_srid_parcelle_geomloc CHECK (st_srid(geomloc) = {1});
-- Table local
--ALTER TABLE {0}.local ADD CONSTRAINT enforce_dims_local_geomloc CHECK (st_ndims(geomloc) = 2);
--ALTER TABLE {0}.local ADD CONSTRAINT enforce_geotype_local_geomloc CHECK (geometrytype(geomloc) = 'POINT'::text OR geomloc IS NULL);
ALTER TABLE {0}.local ADD CONSTRAINT enforce_srid_local_geomloc CHECK (st_srid(geomloc) = {1});

## CREER_TABLE_ANNEXE_TYPO
--
-- CREATION DE LA TABLE ANNEXE ann_typologie
--

DROP TABLE IF EXISTS dvf_annexe.typologie_tmp CASCADE;
CREATE TABLE dvf_annexe.typologie_tmp
(
	niv1 varchar(1),
	niv2 varchar(2),
	niv3 varchar(3),
	niv4 varchar(4),
	niv5 varchar(5),
	libniv1 varchar,
	libniv2 varchar,
	libniv3 varchar,
	libniv4 varchar,
	libniv5 varchar,
	definition text
);

-- niveau 1
insert into dvf_annexe.typologie_tmp (libniv1,niv1) select 'BATI','1';
insert into dvf_annexe.typologie_tmp (libniv1,niv1) select 'NON BATI','2';

-- niveau 2
insert into dvf_annexe.typologie_tmp (libniv2,niv2) select 'BATI - INDETERMINE','10';
insert into dvf_annexe.typologie_tmp (libniv2,niv2) select 'MAISON','11';
insert into dvf_annexe.typologie_tmp (libniv2,niv2) select 'APPARTEMENT','12';
insert into dvf_annexe.typologie_tmp (libniv2,niv2) select 'DEPENDANCE','13';
insert into dvf_annexe.typologie_tmp (libniv2,niv2) select 'ACTIVITE','14';
insert into dvf_annexe.typologie_tmp (libniv2,niv2) select 'BATI MIXTE','15';
insert into dvf_annexe.typologie_tmp (libniv2,niv2) select 'TERRAIN DE TYPE TAB','21';
insert into dvf_annexe.typologie_tmp (libniv2,niv2) select 'TERRAIN ARTIFICIALISE','22';
insert into dvf_annexe.typologie_tmp (libniv2,niv2) select 'TERRAIN NATUREL','23';
insert into dvf_annexe.typologie_tmp (libniv2,niv2) select 'TERRAIN NON BATIS INDETERMINE','20';

-- niveau 3
insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'BATI - INDETERMINE : Vente avec volume(s)','102';
insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'BATI - INDETERMINE : Vefa sans descriptif','101';
insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'MAISON - INDETERMINEE','110';
insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'UNE MAISON','111';
insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'DES MAISONS','112';
insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'UN APPARTEMENT','121';
insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'DEUX APPARTEMENTS','122';
--insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'DES APPARTEMENTS DANS LE MEME IMMEUBLE','123';
insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'APPARTEMENT INDETERMINE','120';
insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'UNE DEPENDANCE','131';
insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'DES DEPENDANCES','132';
--insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'ACTIVITE PRIMAIRE','141';
--insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'ACTIVITE SECONDAIRE','142';
--insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'ACTIVITE TERTIAIRE','143';
--insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'ACTIVITE MIXTE','149';
--insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'ACTIVITE INDETERMINEE','140'; -- (non rapatrié des Fichiers fonciers) 
insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'BATI MIXTE - LOGEMENTS','151';
insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'BATI MIXTE - LOGEMENT/ACTIVITE','152';
insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'TERRAIN D''AGREMENT','221';
insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'TERRAIN D''EXTRACTION','222';
insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'TERRAIN DE TYPE RESEAU','223';
insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'TERRAIN ARTIFICIALISE MIXTE','229';
insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'TERRAIN AGRICOLE','231';
insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'TERRAIN FORESTIER','232';
insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'TERRAIN LANDES ET EAUX','233';
insert into dvf_annexe.typologie_tmp (libniv3,niv3) select 'TERRAIN NATUREL MIXTE','239';


-- niveau 4
--insert into dvf_annexe.typologie_tmp (libniv4,niv4) select 'UNE MAISON A USAGE PROFESSIONNEL','1114';
--insert into dvf_annexe.typologie_tmp (libniv4,niv4) select 'UNE MAISON VEFA OU NEUVE','1111';
--insert into dvf_annexe.typologie_tmp (libniv4,niv4) select 'UNE MAISON RECENTE','1112';
--insert into dvf_annexe.typologie_tmp (libniv4,niv4) select 'UNE MAISON ANCIENNE','1113';
--insert into dvf_annexe.typologie_tmp (libniv4,niv4) select 'UNE MAISON AGE INDETERMINE','1110'; -- (les informations des Fichiers fonciers ne sont pas disponibles ou non renseignés pour la date de construction)   
--insert into dvf_annexe.typologie_tmp (libniv4,niv4) select 'UN APPARTEMENT A USAGE PROFESSIONNEL','1214';
--insert into dvf_annexe.typologie_tmp (libniv4,niv4) select 'UN APPARTEMENT AGE INDETERMINE','1210'; -- (les informations des Fichiers fonciers ne sont pas disponibles ou non renseignées pour la date de construction)   
--insert into dvf_annexe.typologie_tmp (libniv4,niv4) select 'UN APPARTEMENT VEFA OU NEUF','1211';
insert into dvf_annexe.typologie_tmp (libniv4,niv4) select 'TERRAIN VITICOLE','2311';
insert into dvf_annexe.typologie_tmp (libniv4,niv4) select 'TERRAIN VERGER','2312';
insert into dvf_annexe.typologie_tmp (libniv4,niv4) select 'TERRAIN DE TYPE TERRE ET PRE','2313';
insert into dvf_annexe.typologie_tmp (libniv4,niv4) select 'TERRAIN AGRICOLE MIXTE','2319';
--insert into dvf_annexe.typologie_tmp (libniv4,niv4) select 'UN APPARTEMENT RECENT','1212';
--insert into dvf_annexe.typologie_tmp (libniv4,niv4) select 'UN APPARTEMENT ANCIEN','1213';
--insert into dvf_annexe.typologie_tmp (libniv4,niv4) select 'DEUX APPARTEMENTS A USAGE PROFESSIONNEL','1224';
--insert into dvf_annexe.typologie_tmp (libniv4,niv4) select 'DEUX APPARTEMENTS A USAGE MIXTE','1229'; -- : un seul des appartements vendu a un usage professionnel
--insert into dvf_annexe.typologie_tmp (libniv4,niv4) select 'DEUX APPARTEMENTS ANCIENS','1223';
--insert into dvf_annexe.typologie_tmp (libniv4,niv4) select 'DEUX APPARTEMENTS VEFA OU NEUFS','1221';
--insert into dvf_annexe.typologie_tmp (libniv4,niv4) select 'DEUX APPARTEMENTS RECENTS','1222';
--insert into dvf_annexe.typologie_tmp (libniv4,niv4) select 'DEUX APPARTEMENTS INDETERMINES','1220'; -- (les appartements sont habités avec des anciennetés différentes /  les informations des Fichiers fonciers ne sont pas totalement disponibles ou partiellement renseignées pour la date de construction)   
--insert into dvf_annexe.typologie_tmp (libniv4,niv4) select 'UN GARAGE','1311';
--insert into dvf_annexe.typologie_tmp (libniv4,niv4) select 'UNE DEPENDANCE AUTRE','1312';

-- niveau 5
--insert into dvf_annexe.typologie_tmp (libniv5,niv5) select 'UN APPARTEMENT VEFA OU NEUF T1','12111';
--insert into dvf_annexe.typologie_tmp (libniv5,niv5) select 'UN APPARTEMENT VEFA OU NEUF T2','12112';
--insert into dvf_annexe.typologie_tmp (libniv5,niv5) select 'UN APPARTEMENT VEFA OU NEUF T3','12113';
--insert into dvf_annexe.typologie_tmp (libniv5,niv5) select 'UN APPARTEMENT VEFA OU NEUF T4','12114';
--insert into dvf_annexe.typologie_tmp (libniv5,niv5) select 'UN APPARTEMENT VEFA OU NEUF T5 ou +','12115';
--insert into dvf_annexe.typologie_tmp (libniv5,niv5) select 'UN APPARTEMENT VEFA OU NEUF nombre de pièces indéterminé','12110';
--insert into dvf_annexe.typologie_tmp (libniv5,niv5) select 'UN APPARTEMENT RECENT T1','12121';
--insert into dvf_annexe.typologie_tmp (libniv5,niv5) select 'UN APPARTEMENT RECENT T2','12122';
--insert into dvf_annexe.typologie_tmp (libniv5,niv5) select 'UN APPARTEMENT RECENT T3','12123';
--insert into dvf_annexe.typologie_tmp (libniv5,niv5) select 'UN APPARTEMENT RECENT T4','12124';
--insert into dvf_annexe.typologie_tmp (libniv5,niv5) select 'UN APPARTEMENT RECENT T5 ou +','12125';
--insert into dvf_annexe.typologie_tmp (libniv5,niv5) select 'UN APPARTEMENT RECENT nombre de pièces indéterminé','12120';
--insert into dvf_annexe.typologie_tmp (libniv5,niv5) select 'UN APPARTEMENT ANCIEN T1','12131';
--insert into dvf_annexe.typologie_tmp (libniv5,niv5) select 'UN APPARTEMENT ANCIEN T2','12132';
--insert into dvf_annexe.typologie_tmp (libniv5,niv5) select 'UN APPARTEMENT ANCIEN T3','12133';
--insert into dvf_annexe.typologie_tmp (libniv5,niv5) select 'UN APPARTEMENT ANCIEN T4','12134';
--insert into dvf_annexe.typologie_tmp (libniv5,niv5) select 'UN APPARTEMENT ANCIEN T5 ou +','12135';
--insert into dvf_annexe.typologie_tmp (libniv5,niv5) select 'UN APPARTEMENT ANCIEN nombre de pièces indéterminé','12130';

alter table dvf_annexe.typologie_tmp add column niv varchar(5);
update dvf_annexe.typologie_tmp set niv=niv5 where niv5 IS not null;
update dvf_annexe.typologie_tmp set definition=libniv5 where libniv5 IS not null;

update dvf_annexe.typologie_tmp a
	set niv1 = b.niv1,
	    libniv1 = b.libniv1	     
	from dvf_annexe.typologie_tmp b 
	where a.niv5 IS NOT NULL and substring(a.niv,1,1)=b.niv1;

update dvf_annexe.typologie_tmp a
	set niv2 = b.niv2,
	    libniv2 = b.libniv2	     
	from dvf_annexe.typologie_tmp b 
	where a.niv5 IS NOT NULL and substring(a.niv,1,2)=b.niv2;

update dvf_annexe.typologie_tmp a
	set niv3 = b.niv3,
	    libniv3 = b.libniv3	     
	from dvf_annexe.typologie_tmp b 
	where a.niv5 IS NOT NULL and substring(a.niv,1,3)=b.niv3;
	
update dvf_annexe.typologie_tmp a
	set niv4 = b.niv4,
	    libniv4 = b.libniv4	     
	from dvf_annexe.typologie_tmp b 
	where a.niv5 IS NOT NULL and substring(a.niv,1,4)=b.niv4;
	

DELETE FROM dvf_annexe.typologie_tmp
WHERE niv5 IS NULL AND niv4 IN (SELECT DISTINCT niv4 FROm dvf_annexe.typologie_tmp WHERE niv5 IS NOT NULL);

update dvf_annexe.typologie_tmp set niv=niv4 where niv4 IS not null AND niv5 IS NULL;
update dvf_annexe.typologie_tmp set definition=libniv4 where niv4 IS not null AND niv5 IS NULL;

update dvf_annexe.typologie_tmp a
	set niv1 = b.niv1,
	    libniv1 = b.libniv1	     
	from dvf_annexe.typologie_tmp b 
	where a.niv4 IS NOT NULL AND a.niv5 IS NULL and substring(a.niv,1,1)=b.niv1;

update dvf_annexe.typologie_tmp a
	set niv2 = b.niv2,
	    libniv2 = b.libniv2	     
	from dvf_annexe.typologie_tmp b 
	where a.niv4 IS NOT NULL AND a.niv5 IS NULL and substring(a.niv,1,2)=b.niv2;

update dvf_annexe.typologie_tmp a
	set niv3 = b.niv3,
	    libniv3 = b.libniv3	     
	from dvf_annexe.typologie_tmp b 
	where a.niv4 IS NOT NULL AND a.niv5 IS NULL and substring(a.niv,1,3)=b.niv3;

update dvf_annexe.typologie_tmp a
	set niv5 = b.niv4,
	    libniv5 = b.libniv4	     
	from dvf_annexe.typologie_tmp b 
	where a.niv4 IS NOT NULL AND a.niv5 IS NULL and a.niv=b.niv4;

DELETE FROM dvf_annexe.typologie_tmp
WHERE niv4 IS NULL AND niv3 IN (SELECT DISTINCT niv3 FROm dvf_annexe.typologie_tmp WHERE niv4 IS NOT NULL);

update dvf_annexe.typologie_tmp set niv=niv3 where niv3 IS NOT null AND niv4 IS NULL;
update dvf_annexe.typologie_tmp set definition=libniv3 where niv3 IS not null AND niv4 IS NULL;

update dvf_annexe.typologie_tmp a
	set niv1 = b.niv1,
	    libniv1 = b.libniv1	     
	from dvf_annexe.typologie_tmp b 
	where a.niv3 IS NOT NULL AND a.niv4 IS NULL and substring(a.niv,1,1)=b.niv1;

update dvf_annexe.typologie_tmp a
	set niv2 = b.niv2,
	    libniv2 = b.libniv2	     
	from dvf_annexe.typologie_tmp b 
	where a.niv3 IS NOT NULL AND a.niv4 IS NULL and substring(a.niv,1,2)=b.niv2;

update dvf_annexe.typologie_tmp a
	set niv4 = b.niv3,
	    libniv4 = b.libniv3	     
	from dvf_annexe.typologie_tmp b 
	where a.niv3 IS NOT NULL AND a.niv4 IS NULL and a.niv=b.niv3;

update dvf_annexe.typologie_tmp a
	set niv5 = b.niv3,
	    libniv5 = b.libniv3	     
	from dvf_annexe.typologie_tmp b 
	where a.niv3 IS NOT NULL AND a.niv5 IS NULL and a.niv=b.niv3;

DELETE FROM dvf_annexe.typologie_tmp
WHERE niv3 IS NULL AND niv2 IN (SELECT DISTINCT niv2 FROm dvf_annexe.typologie_tmp WHERE niv3 IS NOT NULL);

update dvf_annexe.typologie_tmp set niv=niv2 where niv2 IS NOT null AND niv3 IS NULL;
update dvf_annexe.typologie_tmp set definition=libniv2 where niv2 IS not null AND niv3 IS NULL;

update dvf_annexe.typologie_tmp a
	set niv1 = b.niv1,
	    libniv1 = b.libniv1	     
	from dvf_annexe.typologie_tmp b 
	where a.niv2 IS NOT NULL AND a.niv3 IS NULL and substring(a.niv,1,1)=b.niv1;

update dvf_annexe.typologie_tmp a
	set niv3 = b.niv2,
	    libniv3 = b.libniv2	     
	from dvf_annexe.typologie_tmp b 
	where a.niv2 IS NOT NULL AND a.niv3 IS NULL and a.niv=b.niv2;

update dvf_annexe.typologie_tmp a
	set niv4 = b.niv2,
	    libniv4 = b.libniv2	     
	from dvf_annexe.typologie_tmp b 
	where a.niv2 IS NOT NULL AND a.niv4 IS NULL and a.niv=b.niv2;

update dvf_annexe.typologie_tmp a
	set niv5 = b.niv2,
	    libniv5 = b.libniv2	     
	from dvf_annexe.typologie_tmp b 
	where a.niv2 IS NOT NULL AND a.niv5 IS NULL and a.niv=b.niv2;


DELETE FROM dvf_annexe.typologie_tmp
WHERE niv2 IS NULL AND niv1 IN (SELECT DISTINCT niv1 FROm dvf_annexe.typologie_tmp WHERE niv2 IS NOT NULL);

DROP TABLE IF EXISTS dvf_annexe.ann_typologie CASCADE;
CREATE TABLE dvf_annexe.ann_typologie AS
(
	 SELECT niv as codtypbien,
		definition AS libtypbien,
		niv1,
		libniv1,
		niv2,
		libniv2,
		niv3,
		libniv3,
		niv4,
		libniv4,
		niv5,
		libniv5
	FROM dvf_annexe.typologie_tmp
	ORDER BY codtypbien
);

DROP TABLE IF EXISTS dvf_annexe.typologie_tmp CASCADE;

## MISE_A_JOUR_CHAMPS_TYPO_BIENS_POUR_DEPARTEMENT
UPDATE {0}.mutation m
SET codtypbien = (CASE 
				-- BATI (1)
					WHEN nblocmut > 0 or vefa IS TRUE OR nbvolmut > 0 THEN	
						CASE 
						-- BATI - INDETERMINE (10) : ventes avec volume ou vefa sans information sur le local
							WHEN (nblocmut = 0 AND vefa IS TRUE) OR nbvolmut > 0 THEN 
								CASE 
								-- BATI - INDETERMINE : vente avec volume(s) (102)
									WHEN nbvolmut > 0 THEN '102'
								-- BATI - INDETERMINE : Vefa sans descriptif (101) 
									WHEN (nblocmut = 0 AND vefa IS TRUE) THEN '101'
								-- Ne doit pas exister
									ELSE '100'
								END
						-- MAISON (11)
							WHEN nblocmai > 0 and nblocapt = 0 AND nblocact = 0 THEN
								CASE
								-- MAISON - INDETERMINEE : la surface batie est inférieure à 9 m2 (110)
									WHEN nblocmai = 1 AND sbati < 9 THEN '110'
								-- MAISON INDIVIDUELLE (vendue seule) (111)
									WHEN nblocmai = 1 THEN '111'						
								-- MAISONS INDIVIDUELLES (112) 
									WHEN nblocmai > 1 THEN '112'
									ELSE 'PROBLEME'
								END

						-- APPARTEMENT (12)
							WHEN nblocapt > 0 and nblocmai = 0 AND nblocact = 0 THEN
								CASE
								-- 1 APPARTEMENT (121)
									WHEN nblocapt = 1 AND sbati > 9 THEN '121'					
								-- 2 APPARTEMENTS (122)
									WHEN nblocapt = 2 THEN '122'
								-- APPARTEMENTS INDETERMINES (1 appartement de - de 9 m2 / plusieurs apparts dans plusieurs bâtiments / non rapatriement des Fichiers fonciers) (120)
									ELSE '120'
								END
						-- DEPENDANCE (13)
							WHEN nblocdep > 0 AND nblocapt = 0 and nblocmai = 0 AND nblocact = 0 THEN
								CASE 
								-- UNE DEPENDANCE (131)
									WHEN nblocdep = 1 THEN '131'
								-- DEPENDANCES (132)
									WHEN nblocdep > 1 THEN '132'
									ELSE 'PROBLEME'
								END
						-- ACTIVITE (14)
							WHEN nblocact > 0 AND nblocapt = 0 and nblocmai = 0  THEN '14'
						-- MIXTE BATI (15)
							-- MIXTE - LOGEMENTS (151)
							WHEN nblocact = 0 AND nblocmai > 0 AND nblocapt > 0 THEN '151'
							-- MIXTE - LOGEMENT/ACTIVITE (152)
							WHEN nblocact >0 AND (nblocmai > 0 OR nblocapt > 0) THEN '152'
							ELSE 'PROBLEME'
						END 
				-- NON BATI (2)
					ELSE 
						CASE
						-- TERRAINS DE TYPE TAB (21)
							WHEN l_dcnt[10] > 0 
								OR libnatmut = 'Vente terrain à bâtir' 
								OR l_artcgi && ARRAY['1594D*2','257-7-1*3','278 sexies I.1','691bis', '1594OGA']::VARCHAR[] 				
								THEN '21'
						-- TERRAINS ARTIFICIALISES (22)
							WHEN l_dcnt[7] + l_dcnt[9] + l_dcnt[11] + l_dcnt[12] + l_dcnt[13] = sterr THEN
								CASE
								-- TERRAINS AGREEMENTS (221)
									WHEN  l_dcnt[9] + l_dcnt[11] = sterr THEN '221'
								-- TERRAINS D'EXTRACTION (222)
									WHEN  l_dcnt[7] = sterr THEN '222'
								-- TERRAINS DE TYPE RESEAU (223)
									WHEN  l_dcnt[12] = sterr THEN '223'
								-- TERRAINS ARTIFICIALISES MIXTES (229)
									ELSE '229'
								END 
						-- TERRAINS NATURELS (23)
							WHEN l_dcnt[1] + l_dcnt[2] + l_dcnt[3] + l_dcnt[4] + l_dcnt[5] + l_dcnt[6] + l_dcnt[8] = sterr THEN
							-- TERRAINS AGRICOLES (231)
								CASE
								-- TERRAINS VITICOLES (2311)
									WHEN  l_dcnt[4] >= sterr * 0.25 THEN '2311'
								-- TERRAINS VERGERS (2312)
									WHEN  l_dcnt[3] >= sterr * 0.35 THEN '2312'
								-- TERRAINS DE TYPE TERRES ET PRES (2313)
									WHEN  l_dcnt[1] + l_dcnt[2] >= sterr * 0.40 THEN '2313'
								-- TERRAINS AGRICOLES MIXTES (2319)
									WHEN l_dcnt[1] + l_dcnt[2] +  l_dcnt[3] + l_dcnt[4] >= sterr * 0.40 THEN '2319'
							-- TERRAINS FORESTIERS (232)
									WHEN  l_dcnt[5] > sterr * 0.60 THEN '232'
							-- TERRAINS LANDES ET EAUX (233)
									WHEN  l_dcnt[6] + l_dcnt[8] > sterr * 0.60 THEN '233'
							-- TERRAINS NATURELS MIXTES (239)
									ELSE '239'
								END
						-- TERRAINS NON BATIS INDETERMINES (20)
							ELSE '20' 
						END
					END);
					
UPDATE {0}.mutation m
SET libtypbien = t.libtypbien 
FROM dvf_annexe.ann_typologie t
WHERE m.codtypbien = t.codtypbien;