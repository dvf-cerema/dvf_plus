from datetime import datetime
from pg.pgbasics import *
from ._dvfgestion import GestionVariablesDVF


def log_compte(table):
    """décorateur pour compter les lignes avant et apres insertion dans une table puis les logger"""
    def compteur(fonction):        
        def interne(self, *args, **kwargs):        
            nb_initial = self.compter(self.schema_principal, table)
            resultat = fonction(self, *args, **kwargs)
            nb_final = self.compter(self.schema_principal, table)
            self.ecrire_dans_log(table, nb_initial, nb_final)
            return resultat
        return interne
    return compteur

class DVFMere(PgOutils):

    def __init__(self, hote, base, port, utilisateur, motdepasse, departements, script = 'sorties/script.sql'):
        super().__init__(hote, base, port, utilisateur, motdepasse, script)        
        self.gestionnaire = GestionVariablesDVF()        
        self.departements = [dep.lower() for dep in departements]
        self.schema_principal = 'dvf'
        self.schema_annexe = 'dvf_annexe'
        self.prefixe_schemas_departementaux = 'dvf_d'
        self.schemas_departementaux = [self.prefixe_schemas_departementaux + dep.lower() for dep in self.departements]

    def charger_gestionnaire_depuis_csv(self, fichier_csv):
        self.gestionnaire.charger_tables_depuis_csv(fichier_csv)

    def charger_gestionnaire_depuis_postgres(self, hote, base, port, utilisateur, motdepasse, schema, table_des_variables):
        self.gestionnaire.charger_tables_depuis_postgres(hote, base, port, utilisateur, motdepasse, schema, table_des_variables)

    def charger_gestionnaire_depuis_sqlite(self, fichier_sqlite, table_des_variables='docdv3f_variable'):
        self.gestionnaire.charger_tables_depuis_sqlite(fichier_sqlite, table_des_variables)

    def creation_tables_principales_et_departementales(self, tables, code_creation):
        if not self.creer_tables_principales_vides(tables, code_creation):
            return False
        if not self.creer_tables_departementales_vides(tables, code_creation):
            return False
        if not self.creer_insert_triggers(tables, code_creation):
            return False
        return True

    def creer_tables_principales_vides(self, tables, code_creation):
        for table in tables:
            if not self.creation_table_depuis_gestionnaire(self.schema_principal, table, code_creation):
                return False
            if not self._ajout_commentaire_table(self.schema_principal, table, code_creation):
                return False
            if not self._ajout_commentaires_champs(self.schema_principal, table, code_creation):
                return False
        return True
            
    def creation_table_depuis_gestionnaire(self, schema, table, code_creation):
        nom_table_dvf = self.nom_table_dvf(table, code_creation)
        table_obj = self.table(nom_table_dvf, code_creation)
        success, _ = self.effacer_table(schema, table)
        if not success:
            return False
        success, _ = self._creer_table(schema, table, table_obj.lister_variables_et_types())
        return success
    
    @requete_sql
    def _creer_table(self, schema, table, champs):
        pass
    
    def creer_tables_departementales_vides(self, tables, code_creation):
        for schema in self.schemas_departementaux:
            for table in tables:
                if not self.creation_table_heritee_depuis_gestionnaire(schema, table, code_creation):
                    return False
                if not self._ajout_commentaire_table(schema, table, code_creation):
                    return False
                if not self._ajout_commentaires_champs(schema, table, code_creation):
                    return False
        return True
    
    def creation_table_heritee_depuis_gestionnaire(self, schema, table, code_creation, schema_mere = 'dvf', table_mere = None):
        table_mere = table if not table_mere else table_mere
        success, _ = self.effacer_table(schema, table)
        if not success:
            return False
        success, _ = self._creer_table_heritee(schema, table, schema_mere, table_mere)
        if not success:
            return False
        success, _ = self._ajout_clef_primaire(schema, table, code_creation)
        if not success:
            return False
        success, _ = self._ajout_contrainte_unicite(schema, table, code_creation)
        if not success:
            return False
        success, _ = self._ajout_contrainte_check_departement(schema, table, code_creation)
        if not success:
            return False
        return True
    
    @requete_sql
    def _creer_table_heritee(self, schema, table, schema_mere, table_mere):       
        pass
    
    def creer_insert_triggers(self, tables, code_creation):
        for table in tables:
            success, _ = self._ajout_insert_trigger(self.schema_principal, table, code_creation)
            if not success:
                return False
        return True
    
    def _ajout_clef_primaire(self, schema, table, code_creation):
        champs = self.recuperer_contrainte(table, 'PK', code_creation)        
        if len(champs) > 0:
            return self.ajouter_clef_primaire(schema, table, champs)
        else:
            return True, 0
    
    def _ajout_contrainte_unicite(self, schema, table, code_creation):
        champs = self.recuperer_contrainte(table, 'U', code_creation)        
        if len(champs) > 0:
            return self.ajouter_contrainte_unicite(schema, table, champs)
        else :
            return True, 0
    
    def _ajout_contrainte_check_departement(self, schema, table, code_creation):
        champs = self.recuperer_contrainte(table, 'C', code_creation)
        if len(champs) > 0:
            expression = """{} = '{}'""".format(str(champs[0]), self.departement(schema).upper())
            return self.ajouter_contrainte_check(schema, table, expression)
        else:
            return True, 0
    
    def _ajout_commentaire_table(self, schema, table, code_creation):
        nom_table_dvf = self.nom_table_dvf(table, code_creation)
        commentaire = self.table(nom_table_dvf, code_creation).description    
        success, _ = self.ajouter_commentaire_sur_table(schema, table, commentaire)
        return success 
    
    def _ajout_commentaires_champs(self, schema, table, code_creation):
        for ch, commentaire in self.recuperer_commentaires(table, code_creation).items():
            success, _ = self.ajouter_commentaire_sur_champ(schema, table, ch, commentaire)
            if not success:
                return False
        return True
    
    @requete_sql_avec_modification_args
    def _ajout_insert_trigger(self, schema, table, code_creation):        
        champ_dpt = self.recuperer_contrainte(table, 'C', code_creation)[0]        
        condition = ''        
        for departement in self.departements:
            condition += 'IF' if condition == '' else 'ELSIF'            
            condition += ' (NEW.' + champ_dpt + '=\'' + departement.upper() + '\') THEN INSERT INTO ' + self.schema(departement) + '.'+ table +' VALUES(NEW.*); RETURN NULL;\n'
        #condition += 'ELSE RAISE EXCEPTION \'Erreur code dpt\'; '
        condition += 'ELSE RETURN NULL;'
        condition += 'END IF;'         
        return schema, table, condition
    
    def recuperer_contrainte(self, table, typ, code_creation):        
        nom_table_dvf = self.nom_table_dvf(table, code_creation)
        table_obj = self.table(nom_table_dvf, code_creation)                                       
        return [var.nom for var in table_obj.variables if var.contrainte == typ]
    
    def recuperer_commentaires(self, table, code_creation):
        nom_table_dvf = self.nom_table_dvf(table, code_creation)
        table_obj = self.table(nom_table_dvf, code_creation)            
        comment_champs = {}
        for variable in table_obj.variables:
            comment_champs[variable.nom] = variable.description           
        return comment_champs
    
    def renommage_tables(self, table, code_creation):
        nom_table_dvf = self.nom_table_dvf(table, code_creation)
        if nom_table_dvf in self.lister_tables(self.schema_principal): # on redefinit la valeur de départ pour l'identifiant
            ids = {'mutation' : 'idmutation', 'local':'iddispoloc', 'disposition_parcelle':'iddispopar'}
            id = ids[nom_table_dvf]
            self.redefinir_valeur_sequence(self.schema_principal, table, id, code_creation)       
        if not self.effacer_table_dvf(self.schema_principal, table, code_creation):
            return False
        if not self.renommer_table_dvf_XXXX(self.schema_principal, table, code_creation):
            return False
        for schema in self.schemas_departementaux:
            if not self.effacer_table_dvf(schema, table, code_creation):
                return False
            if not self.renommer_contraintes(schema, table, code_creation):
                return False
            if not self.renommer_table_dvf_XXXX(schema, table, code_creation):
                return False
        success = self.supprimer_trigger_table_dvf_XXXX(table, code_creation)
        return success
    
    def redefinir_valeur_sequence(self, schema, table, id, code_creation):
        nom_table_dvf = self.nom_table_dvf(table, code_creation)
        valeur = self._recuperer_curval_sequence(schema, nom_table_dvf, id)
        return self._affecter_curval_sequence(self.schema_principal, table, id, valeur)

    @select_sql_valeur_unique
    def _recuperer_curval_sequence(self, schema, table, id):
        pass
    
    @requete_sql
    def _affecter_curval_sequence(self, schema, table, id, valeur):
        pass        
    
    def effacer_table_dvf(self, schema, table, code_creation):
        nom_table_dvf = self.nom_table_dvf(table, code_creation)
        success, _ = self.effacer_table(schema, nom_table_dvf)
        return success
    
    def renommer_table_dvf_XXXX(self, schema, table, code_creation):
        nom_table_dvf = self.nom_table_dvf(table, code_creation)        
        success, _ = self.renommer_table(schema, table, nom_table_dvf)
        return success                
    
    def supprimer_trigger_table_dvf_XXXX(self, table, code_creation):
        if code_creation == 2:
            nom_table_dvf = self.nom_table_dvf(table, code_creation)
            success, _ = self._supprimer_trigger(self.schema_principal, nom_table_dvf, table)
            return success
        else:
            return True
    
    @requete_sql
    def _supprimer_trigger(self, schema, nom_table_dvf, table):
        pass
    
    @requete_sql
    def _renommer_trigger(self, schema, nom_table_dvf, table):
        pass
    
    def renommer_contraintes(self, schema, table, code_creation):  
        nv_nom_table = self.nom_table_dvf(table, code_creation)
        champs = self.recuperer_contrainte(table, 'PK', code_creation)              
        if len(champs) > 0:
            success, _ = self._renommer_contrainte(schema, table, nv_nom_table, '_pkey')
            if not success:
                return False
        champs = self.recuperer_contrainte(table, 'U', code_creation)        
        if len(champs) > 0:
            success, _ = self._renommer_contrainte(schema, table, nv_nom_table, '_unique')
            if not success:
                return False
        champs = self.recuperer_contrainte(table, 'C', code_creation)        
        if len(champs) > 0:
            success, _ = self._renommer_contrainte(schema, table, nv_nom_table, '_check')
            if not success:
                return False
        return True
    
    @requete_sql
    def _renommer_contrainte(self, schema, table, nv_nom_table, suffixe):
        pass
    
    def nom_table_dvf(self, table, code_creation):
        ''' Renvoie le nom de la table DVF du gestionnaire associé au code_creation (1 pour DVF, 2 pour DVF+ et 3 pour DV3F)'''
        return table[:-5] if code_creation > 1 and table != 'acheteur_vendeur' else table
    
    def table(self, nom_table_dvf, code_creation):
        ''' Renvoie l'objet Table associé au code_creation (1 pour DVF, 2 pour DVF+ et 3 pour DV3F)'''
        if code_creation == 1:         
            table_obj = self.gestionnaire.table(nom_table_dvf).tableDVF()
        elif code_creation == 2:
            table_obj = self.gestionnaire.table(nom_table_dvf).tableDVF_plus()
        elif code_creation == 3:
            table_obj = self.gestionnaire.table(nom_table_dvf)
        return table_obj
    
    def est_schema_departemental(self, schema):
        if schema.startswith(self.prefixe_schemas_departementaux):
            return True
        return False
    
    def departement(self, schema):
        return schema[len(self.prefixe_schemas_departementaux):].lower()
    
    def schema(self, departement):
        return self.prefixe_schemas_departementaux + departement.lower()
        
    

class DVF(DVFMere):
    
    START_SCRIPT = '''---
-- SCRIPT DE CREATION DU MODELE DVF+ 
-- ETAPE 1/2
-- 
-- Auteur: 
--    CEREMA "PCI Foncier et Stratégies Foncières"
--    Direction Territoriale Nord-Picardie
--    Antoine HERMAN
--    Jérôme DOUCHE
--
-- Ce script est fourni sous licence Creative Commons 3.0 CC-BY-SA
--
-- Début d'exécution le {0:%d}/{0:%m}/{0:%Y} à {0:%H}:{0:%M}:{0:%S}
---

'''

    TABLES = ['mutation', 'mutation_article_cgi', 'disposition', 'disposition_parcelle', 'parcelle',
            'local', 'adresse', 'suf', 'volume', 'lot', 'adresse_dispoparc', 'adresse_local']
    
    TABLES_ANNEXES = ['ann_nature_culture', 'ann_nature_culture_speciale', 'ann_cgi', 
                      'ann_nature_mutation', 'ann_type_local']


    def __init__(self, hote, base, port, utilisateur, motdepasse, departements, script ='sorties/script_dvf.sql', log = 'sorties/log.txt'):        
        DVFMere.__init__(self, hote, base, port, utilisateur, motdepasse, departements = departements, script = script)
        self.log = log
        self.creer_fonctions_sql_dvf()    
               
    def creer_fonctions_sql_dvf(self):
        success, _ = self.creer_fonction_array_supprimer_null()
        if not success:
                return False
        success, _ = self.creer_fonction_pgcd()
        if not success:
                return False
        success, _ = self.creer_fonction_aggregat_first()
        if not success:
                return False
        return True
    
    def effacer_et_creer_schemas_dvf(self):
        if not self.effacer_et_creer_schemas_dvf_departementaux():
            return False
        if not self.effacer_et_creer_schemas_dvf_principaux():
            return False
        return True
        
    def effacer_et_creer_schemas_dvf_departementaux(self):
        schemas = self.schemas_departementaux 
        success, _ = self.effacer_et_creer_schemas(schemas)
        return success
    
    def effacer_et_creer_schemas_dvf_principaux(self):
        schemas = [self.schema_principal, self.schema_annexe, 'source']
        success, _ = self.effacer_et_creer_schemas(schemas)
        if not success:
            return False
        success, _ = self.effacer_et_creer_sequence('source', 'seq_idloc_fictif')
        return success
        
    '''
    
    DVF - CREATION TABLES ANNEXES
    
    '''
    
    def creation_tables_annexes(self, fichierArtCGI, fichierNatCult, fichierNatCultSpe):
        if not self.creer_tables_annexes_vides():
            return False
        success, _ = self.inserer_donnees_tables_annexes()
        if not success:
                return False
        success, _ = self.creer_tables_annexes_temporaires()
        if not success:
                return False
        if not self.copier_fichiers_annexes_csv_dans_tables_annexes_temporaires(fichierArtCGI, fichierNatCult, fichierNatCultSpe):
            return False
        success, _ = self.maj_tables_annexes()
        if not success:
                return False
        success, _ = self.effacer_tables_commencant_par(self.schema_annexe, 'tmp_ann_')
        if not success:
                return False
        return True
    
    def creer_tables_annexes_vides(self):                
        for table in self.TABLES_ANNEXES:
            if not self.creation_table_depuis_gestionnaire(self.schema_annexe, table, 1):
                return False
            if not self._ajout_commentaire_table(self.schema_annexe, table, 1):
                return False
            if not self._ajout_commentaires_champs(self.schema_annexe, table, 1):
                return False
        return True        
    
    @requete_sql_avec_modification_args
    def inserer_donnees_tables_annexes(self):        
        return (self.schema_annexe,)
    
    @requete_sql_avec_modification_args
    def creer_tables_annexes_temporaires(self):
        return (self.schema_annexe,)
    
    def copier_fichiers_annexes_csv_dans_tables_annexes_temporaires(self, fichierArtCGI, fichierNatCult, fichierNatCultSpe):
        if not self.pgconn.copy_from_csv(fichierNatCult, '|', '{0}.tmp_ann_nature_culture'.format(self.schema_annexe), False):
            return False            
        if not self.pgconn.copy_from_csv(fichierNatCultSpe, '|', '{0}.tmp_ann_nature_culture_speciale'.format(self.schema_annexe), False):
            return False
        if not self.pgconn.copy_from_csv(fichierArtCGI, '|', '{0}.tmp_ann_cgi'.format(self.schema_annexe), False):
            return False
        self.redaction_script(self.script, '--\n--\n--\n--\n--\n--\n-- IMPORT DES FICHIERS ANNEXES '+', '.join([fichierNatCult, fichierNatCultSpe, fichierArtCGI])+' PAR UNE INSTRUCTION "COPY FROM"--\n--\n--\n--\n--\n--\n--\n--\n', False)
        return True            
         
    @requete_sql_avec_modification_args
    def maj_tables_annexes(self):
        return (self.schema_annexe,)
    
    
    '''
    
    DVF - CREATION TABLES
    
    '''
    
    def creation_tables(self, recreer_tables_principales = True):        
        if recreer_tables_principales:
            success = self.creation_tables_principales_et_departementales(self.TABLES, 1)
            return success
        else:
            if not self.creer_tables_departementales_vides(self.TABLES, 1):
                return False
            success = self.creer_insert_triggers(self.TABLES, 1)
            return success
    
    '''
    
    DVF - IMPORTATION DONNEES BRUTES
    
    '''
    
    def importer(self, fichier, table, recherche_differentielle = True):
        success, _ = self.creer_table_import_temporaire()
        if not success:
            return False
        if not self.pgconn.copy_from_csv(fichier, '|', 'source.tmp', True):
            return False
        if recherche_differentielle:
            if not self.creer_table_source_avec_recherche_differentielle(table):
                return False
        else:
            if not self.creer_table_source_sans_recherche_differentielle(table):
                return False
        if not self.effacer_table('source', 'tmp'):
            return False
        return True
    
    @requete_sql
    def creer_table_import_temporaire(self):
        pass
    
    @requete_sql
    def creer_table_source_sans_recherche_differentielle(self, table):
        pass
    
    @requete_sql
    def creer_table_source_avec_recherche_differentielle(self, table):
        pass
    
    '''
    
    DVF - INTEGRATION DONNEES
    
    '''
    
    def integration_dans_dvf(self, tables_sources): 
        self.ecrire_entete_log()       
        for table_src in tables_sources:
            for departement in self.departements:
                success, _ = self.creer_table_source_departementale(table_src, departement, decoup_departement=True)
                if not success:
                    return False
                self.ecrire_entete_table_import_dans_log('{0}_d{1}'.format(table_src, departement))
                if not self.maj_tables_avec('{0}_d{1}'.format(table_src, departement)):
                    return False
                #success, _ = self.effacer_table('source', '{0}_d{1}'.format(table_src, departement))
                #if not success:
                #    return False
        return True
    
    def ecrire_entete_log(self):
        self.redaction_script(self.log, 'LOG - INTEGRATION DES DONNEES BRUTES DVF\n', True)
            
    def ecrire_entete_table_import_dans_log(self, table_src):
        self.redaction_script(self.log, '{0}|Nombre de lignes avant intégration|Nombre de lignes après intégration|Nombre de lignes intégrées\n'.format(table_src), False)
    
    def ecrire_dans_log(self, table, nb_initial, nb_final):
        self.redaction_script(self.log, 'Table {0} : {1}|{2}|{3}\n'.format(table, str(nb_initial), str(nb_final), str(int(nb_final - nb_initial))), False)
    
    @requete_sql_avec_modification_args    
    def creer_table_source_departementale(self, table_src, departement, decoup_departement=True):

        condition = "WHERE code_departement='{0}'".format(departement.upper()) if decoup_departement else ''
        suffixe = '_d{0}'.format(departement) if decoup_departement else ''
        return table_src, departement, condition, suffixe
    
    def maj_tables_avec(self, table_src):
        # Première partie des maj
        success, _ = self.effacer_table('source', 'tmp_calcul_lot')
        if not success:
            return False
        success, _ = self.creer_table_calcul_lot(table_src)
        if not success:
            return False        
        #self.maj_table_ann_nature_mutation(table_src)  -->  ne sert plus, les données sont figées par la fonction inserer_donnees_tables_annexes
        success, _ = self.maj_table_mutation(table_src)
        #if not success:
        #    return False
        #success, _ = self.maj_table_mutation_art_cgi(table_src)
        if not success:
            return False
        success, _ = self.maj_table_disposition(table_src)
        if not success:
            return False
        success, _ = self.maj_table_parcelle(table_src)
        if not success:
            return False
        success, _ = self.maj_table_disposition_parcelle(table_src)
        if not success:
            return False
        success, _ = self.maj_table_adresse(table_src)
        if not success:
            return False
        # Deuxième partie des maj
        success, _ = self.effacer_table('source', table_src + '_tmp')
        if not success:
            return False
        success, _ = self.creer_table_temporaire_intermediaire(table_src)
        if not success:
            return False
        success, _ = self.maj_table_local(table_src)
        if not success:
            return False
        success, _ = self.maj_table_volume(table_src)
        if not success:
            return False
        success, _ = self.maj_table_suf(table_src)
        if not success:
            return False
        success, _ = self.maj_table_lot(table_src)
        if not success:
            return False
        success, _ = self.maj_tables_passages_adresses(table_src)
        if not success:
            return False        
        success, _ = self.effacer_table('source', 'tmp_calcul_lot')
        if not success:
            return False
        success, _ = self.effacer_table('source', table_src + '_tmp')
        if not success:
            return False
        return True
    
    @requete_sql
    def creer_table_calcul_lot(self, table_src):
        pass

    @requete_sql_avec_modification_args
    def maj_table_ann_nature_mutation(self, table_src):
        return self.schema_annexe, table_src
    
    @log_compte('mutation')
    @requete_sql_avec_modification_args
    def maj_table_mutation(self, table_src):
        return self.schema_principal, table_src, self.schema_annexe
    
    @log_compte('mutation_article_cgi')
    @requete_sql_avec_modification_args
    def maj_table_mutation_art_cgi(self, table_src):
        return self.schema_principal, table_src, self.schema_annexe
    
    @log_compte('disposition')
    @requete_sql_avec_modification_args
    def maj_table_disposition(self, table_src):
        return self.schema_principal, table_src
    
    @log_compte('parcelle')
    @requete_sql_avec_modification_args
    def maj_table_parcelle(self, table_src):
        return self.schema_principal, table_src
    
    @log_compte('disposition_parcelle')
    @requete_sql_avec_modification_args
    def maj_table_disposition_parcelle(self, table_src):
        return self.schema_principal, table_src
    
    @log_compte('adresse')
    @requete_sql_avec_modification_args
    def maj_table_adresse(self, table_src):
        return self.schema_principal, table_src

    @requete_sql_avec_modification_args
    def creer_table_temporaire_intermediaire(self, table_src):
        return self.schema_principal, table_src
    
    @log_compte('local')
    @requete_sql_avec_modification_args
    def maj_table_local(self, table_src):
        return self.schema_principal, table_src
    
    @log_compte('volume')
    @requete_sql_avec_modification_args
    def maj_table_volume(self, table_src):
        return self.schema_principal, table_src
    
    @log_compte('suf')
    @requete_sql_avec_modification_args
    def maj_table_suf(self, table_src):
        return self.schema_principal, table_src
    
    @log_compte('lot')
    @requete_sql_avec_modification_args
    def maj_table_lot(self, table_src):
        return self.schema_principal, table_src
    
    @log_compte('adresse_local')
    @log_compte('adresse_dispoparc')
    @requete_sql_avec_modification_args
    def maj_tables_passages_adresses(self, table_src):
        return self.schema_principal, table_src
   

class DVF_PLUS(DVFMere):
    
    START_SCRIPT = '''---
-- SCRIPT DE CREATION DU MODELE DVF+ 
-- ETAPE 2/2
-- 
-- Auteur: 
--    CEREMA "PCI Foncier et Stratégies Foncières"
--    Direction Territoriale Nord-Picardie
--    Antoine HERMAN
--    Jérôme DOUCHE
--
-- Ce script est fourni sous licence Creative Commons 3.0 CC-BY-SA
--
-- Début d'exécution le {0:%d}/{0:%m}/{0:%Y} à {0:%H}:{0:%M}:{0:%S}
---

'''

    TABLES = ['local_plus', 'disposition_parcelle_plus', 'mutation_plus']
    
    def __init__(self, hote, base, port, utilisateur, motdepasse, departements, script ='sorties/script_dvf_plus_principal.sql'):        
        DVFMere.__init__(self, hote, base, port, utilisateur, motdepasse, departements, script = script)
    
    '''
    
    DVF+ - CREATION TABLES
    
    '''
    
    def creation_tables_dvf_plus(self, recreer_tables_principales = True):
        if recreer_tables_principales:
            if not self.creation_tables_principales_et_departementales(self.TABLES, 2):
                return False
        else:
            for table in self.TABLES: # on renomme les tables du schema principal avec l'extension _plus 
                success, _ = self.renommer_table(self.schema_principal, self.nom_table_dvf(table, 2), table)
                if not success:
                    return False
                success, _ = self._renommer_trigger(self.schema_principal, self.nom_table_dvf(table, 2), table)
                if not success:
                    return False
            success = self.creer_tables_departementales_vides(self.TABLES, 2)
            if not success:
                return False
        return True

    '''

    DVF+ - REMPLACEMENT TABLES

    '''

    def calcul_et_construction_tables_dvf_plus(self):
        variables_jointure = {'local' : 'iddispoloc', 
                              'disposition_parcelle': 'iddispopar', 
                              'mutation' : 'idmutation'}
        for table in self.TABLES:
            nom_table_dvf = self.nom_table_dvf(table, 2)
            if nom_table_dvf == 'local':
                if not self.effectuer_calculs_local():
                    return False
            elif nom_table_dvf == 'disposition_parcelle':
                if not self.effectuer_calculs_parcelle():
                    return False
            elif nom_table_dvf == 'mutation':
                if not self.effectuer_calculs_mutation():
                    return False
            if not self.construire_tables_dvf_plus(nom_table_dvf, variables_jointure[nom_table_dvf]):
                return False
        return True

    def transformation_tables_dvf(self):
        for table in self.TABLES:
            if not self.renommage_tables(table, 2):
                return False
        if not self.creation_index_dvf_plus():
            return False
        return True
    
    '''
    
    DVF+ - CALCULS TEMPORAIRES
    
    '''
        
    def effectuer_calculs_local(self):
        for schema in self.schemas_departementaux:
            valid, nb = self.effacer_tables_commencant_par(schema, 'tmp_')
            valid2 = self.creation_tables_calculs_temporaires_local(schema)
            if not (valid and valid2):
                return False
        return True
    
    def effectuer_calculs_parcelle(self):
        for schema in self.schemas_departementaux:
            success, _ = self.effacer_tables_commencant_par(schema, 'tmp_')
            if not success:
                return False
            if not self.creation_tables_calculs_temporaires_parcelle(schema):
                return False
        return True
    
    def effectuer_calculs_mutation(self):
        for schema in self.schemas_departementaux:
            success, _ = self.effacer_tables_commencant_par(schema, 'tmp_')
            if not success:
                return False
            if not self.creation_tables_calculs_temporaires_mutation(schema):
                return False
        return True
    
    def creation_tables_calculs_temporaires_mutation(self, schema):
        success, _ = self.creer_table_calcul_ann_nature_mutation_idmutation(schema)
        if not success:
            return False
        success, _ = self.creer_table_calcul_mutation_article_cgi_idmutation(schema)
        if not success:
            return False
        success, _ = self.creer_table_calcul_vefa_idmutation(schema)
        if not success:
            return False
        success, _ = self.creer_table_calcul_disposition_idmutation(schema)
        if not success:
            return False
        success, _ = self.creer_table_calcul_disposition_parcelle_idmutation(schema, '_plus')
        if not success:
            return False
        success, _ = self.creer_table_calcul_suf_idmutation(schema, '_plus')
        if not success:
            return False
        success, _ = self.creer_table_calcul_volume_idmutation(schema)
        if not success:
            return False
        success, _ = self.creer_table_calcul_local_idmutation(schema)
        if not success:
            return False
        return True
    
    def creation_tables_calculs_temporaires_local(self, schema):
        success, _ = self.creer_table_calcul_local_iddispoloc(schema)
        return success
    
    def creation_tables_calculs_temporaires_parcelle(self, schema):
        success, _ = self.creer_table_calcul_parcelle_iddispopar(schema)
        if not success:
            return False
        success, _ = self.creer_table_calcul_suf_iddispopar(schema)
        return success
    
    @requete_sql
    def creer_table_calcul_parcelle_iddispopar(self, schema):
        pass
    
    @requete_sql
    def creer_table_calcul_suf_iddispopar(self, schema):
        pass
    
    @requete_sql
    def creer_table_calcul_local_iddispoloc(self, schema):
        pass
    
    @requete_sql_avec_modification_args
    def creer_table_calcul_ann_nature_mutation_idmutation(self, schema):
        return schema, self.schema_annexe
    
    @requete_sql
    def creer_table_calcul_mutation_article_cgi_idmutation(self, schema):
        pass
    
    @requete_sql
    def creer_table_calcul_vefa_idmutation(self, schema):
        pass
    
    @requete_sql
    def creer_table_calcul_disposition_idmutation(self, schema):
        pass
    
    @requete_sql
    def creer_table_calcul_disposition_parcelle_idmutation(self, schema, suffixe):
        pass
    
    @requete_sql
    def creer_table_calcul_suf_idmutation(self, schema, suffixe):
        pass
    
    @requete_sql
    def creer_table_calcul_volume_idmutation(self, schema):
        pass
    
    @requete_sql
    def creer_table_calcul_local_idmutation(self, schema):
        pass
    
    '''
    
    DVF+ - REMPLACEMENT TABLE DVF PAR TABLE DVF+
    
    '''
    
    def construire_tables_dvf_plus(self, nom_table_dvf, variable_jointure):        
        for schema in self.schemas_departementaux:
            success, _ = self.executer_requete_jointure(schema, nom_table_dvf, variable_jointure)
            if not success:
                return False
            success, _ = self.effacer_tables_commencant_par(schema, 'tmp_')
            if not success:
                return False
        return True

    def executer_requete_jointure(self, schema, table, variable_jointure):        
        
        champs = self.recuperer_champ_calcule(table)        
        liste_variable = [var.nom for var in self.gestionnaire.table(table).tableDVF_plus().variables]
        
        sql = '''
INSERT INTO %(schema)s.%(table)s
(
    %(variable)s
)
(
SELECT \n\t''' % {'schema':schema, 'table':table + '_plus', 'variable':', '.join(liste_variable)}        
        
        for v in liste_variable:
            indice_table = ''
            for l in champs:
                if v in l[1]:
                    indice_table = str(champs.index(l))
            sql += 't' + indice_table + '.' + v + ',\n\t'        
        sql = sql[:-3]
        sql += '\nFROM '+schema+'.' + table + ' t \n'
        count = 0
        for l in champs:
            sql += 'LEFT JOIN '+schema+'.tmp_calcul_' + l[0] + '_'+variable_jointure+' t' + str(count) + '\n\tON t.'+variable_jointure+' = t' + str(count) +'.'+variable_jointure+'\n'
            count +=1        
        sql += ');\n'
        return self.execution_et_ecriture_script(sql)
        
    def recuperer_champ_calcule(self, table):        
        champs = []
        tableDVF = self.gestionnaire.table(table).tableDVF_plus()        
        for table_pour_creation in tableDVF.lister_tables_pour_creation():
            champs.append([table_pour_creation, tableDVF.lister_nom_variables_ayant_table_creation(table_pour_creation)])        
        return champs
    
    def creation_index_dvf_plus(self):
        for schema in self.schemas_departementaux:
            success, _ = self.creer_index_gin_champ_lcodinsee(schema)
            if not success:
                return False
        return True
    
    @requete_sql
    def creer_index_gin_champ_lcodinsee(self, schema):
        pass

# eof