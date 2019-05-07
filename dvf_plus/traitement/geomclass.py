from datetime import datetime

from pg.pgbasics import *

class GeometrieParcelle(PgOutils):
    
    START_SCRIPT = '''---
-- SCRIPT DE CREATION DU MODELE DV3F 
-- 
-- Auteur: 
--    CEREMA "PCI Foncier et Stratégies Foncières"
--    Direction Territoriale Nord-Picardie
--    Antoine HERMAN
--
--
-- Début d'exécution le {0:%d}/{0:%m}/{0:%Y} à {0:%H}:{0:%M}:{0:%S}
---

'''
    
    def __init__(self, hote, base, port, utilisateur, motdepasse, departements, script = 'sorties/script_prepa_geometries.sql'):
        super().__init__(hote, base, port, utilisateur, motdepasse, script)         
        self.departements = departements
        self.prefixe_schemas_departementaux = 'dvf_d'
        self.schemas_departementaux = [self.prefixe_schemas_departementaux + dep.lower() for dep in self.departements]
        self.motif_table_pci = 'pcidata_dep{0}_parcelles'
         
    def rapatrier_tables_departementales_pci(self, hote_bdpar, base_bdpar, port, utilisateur, motdepasse, schema_pci):
        for schema in self.schemas_departementaux:          
            success, _ = self.copier_table_distante(hote_bdpar, base_bdpar, port, utilisateur, motdepasse, 
                                       schema_pci, self.motif_table_pci.format(self.departement(schema).rjust(3, '0')), 
                                       schema, 'geometrie_pci')
            if not success:
                return False
        return True
    
    def effacer_tables_departementales_pci(self):
        for schema in self.schemas_departementaux:          
            success, _ = self.effacer_table(schema, 'geometrie_pci')
            if not success:
                return False
        return True
    
    def departement(self, schema):
        return schema[len(self.prefixe_schemas_departementaux):].lower()


class ComplementDVFPlus(PgOutils):
    
    def __init__(self, hote, base, port, utilisateur, motdepasse, departements, script = 'sorties/script.sql'):
        super().__init__(hote, base, port, utilisateur, motdepasse, script)                
        self.departements = [dep.lower() for dep in departements]
        self.schema_principal = 'dvf'
        self.schema_annexe = 'dvf_annexe'
        self.prefixe_schemas_departementaux = 'dvf_d'
        self.schemas_departementaux = [self.prefixe_schemas_departementaux + dep.lower() for dep in self.departements]
     
    @requete_sql
    def creer_extension_postgis(self):
        pass   
                
    @requete_sql
    def creer_champs_geometriques(self):
        pass
    
    @requete_sql
    def creer_champs_typo_biens(self):
        pass
    
    def ajouter_commentaires_champs_geométriques(self):
        for schema in [self.schema_principal] + self.schemas_departementaux:
            valid, nb = self.ajouter_commentaire_sur_champ(schema, 'mutation', 'geompar', "geométrie de l'ensemble des contours des parcelles concernées par la mutation")
            valid1, nb = self.ajouter_commentaire_sur_champ(schema, 'mutation', 'geomparmut', "géométrie de l'ensemble des contours des parcelles ayant muté")
            valid2, nb = self.ajouter_commentaire_sur_champ(schema, 'mutation', 'geomlocmut', "géométrie de l'ensemble des localisants correspondant à des parcelles surlesquelles un local a muté")
            valid3, nb = self.ajouter_commentaire_sur_champ(schema, 'disposition_parcelle', 'geompar', "géométrie du contour de la parcelle")
            valid4, nb = self.ajouter_commentaire_sur_champ(schema, 'disposition_parcelle', 'geomloc', "géométrie du localisant de la parcelle")
            valid5, nb = self.ajouter_commentaire_sur_champ(schema, 'local', 'geomloc', "géométrie du localisant")
            if not (valid and valid1 and valid2 and valid3 and valid4 and valid5):
                return False
        return True
    
    def ajouter_commentaires_champs_typo(self):
        for schema in [self.schema_principal] + self.schemas_departementaux:
            valid, nb = self.ajouter_commentaire_sur_champ(schema, 'mutation', 'codtypbien', "code de la typologie des biens du GnDVF")
            valid1, nb = self.ajouter_commentaire_sur_champ(schema, 'mutation', 'libtypbien', "libellé de la typologie des biens du GnDVF")
            if not (valid and valid1):
                return False
        return True
    
    def mise_a_jour_geometries_local_depuis(self):
        for schema_departemental in self.schemas_departementaux:
            valid, nb = self.mise_a_jour_geometries_local_pour_departement_depuis(schema_departemental, 'geometrie_pci', schema_departemental)
            if not valid:
                return False
        return True
    
    def mise_a_jour_geometries_disposition_parcelle_depuis(self):
        for schema_departemental in self.schemas_departementaux:
            valid, nb = self.mise_a_jour_geometries_disposition_parcelle_pour_departement_depuis(schema_departemental, 'geometrie_pci', schema_departemental)
            if not valid:
                return False
        return True
    
    def mise_a_jour_geometries_mutation(self):
        for schema_departemental in self.schemas_departementaux:
            valid, nb = self.mise_a_jour_geometries_mutation_pour_departement(schema_departemental)
            if not valid:
                return False
        return True
    
    def mise_a_jour_typologie_mutation(self):
        for schema_departemental in self.schemas_departementaux:
            valid, nb = self.mise_a_jour_champs_typo_biens_pour_departement(schema_departemental)
            if not valid:
                return False
        return True
    
    def creer_index_et_contraintes_geometriques(self):
        for schema_departemental in self.schemas_departementaux:
            valid, nb =self.creer_index_geometriques(schema_departemental)
            if not valid:
                return False
            # definition de l'epsg
            epsg = '2154'
            if schema_departemental.endswith('971') or schema_departemental.endswith('972'):
                epsg = '32620'
            elif schema_departemental.endswith('973'):
                epsg = '2972'
            elif schema_departemental.endswith('974'):
                epsg = '2975'
            valid, nb =self.creer_contraintes_geometriques(schema_departemental, epsg)
            if not valid:
                return False
        return True
    
    @requete_sql
    def mise_a_jour_geometries_local_pour_departement_depuis(self, schema, table, schema_departemental):
        pass
    
    @requete_sql
    def mise_a_jour_geometries_disposition_parcelle_pour_departement_depuis(self, schema, table, schema_departemental):
        pass

    @requete_sql
    def mise_a_jour_geometries_mutation_pour_departement(self,  schema_departemental):
        pass
    
    @requete_sql
    def creer_index_geometriques(self, schema_departemental):
        pass
    
    @requete_sql
    def creer_contraintes_geometriques(self, schema_departemental, epsg):
        pass
    
    @requete_sql
    def creer_table_annexe_typo(self):
        return
    
    @requete_sql
    def mise_a_jour_champs_typo_biens_pour_departement(self, schema_departemental):
        return
    
    
#eof