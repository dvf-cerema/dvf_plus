import os, sys, threading
from .traitement.dvfclass import DVF
from .traitement.dvfclass import DVF_PLUS
from .traitement import BASE_SQLITE
from .traitement import FICHIERS_ANNEXES
from .controle import repartition_departements
from .controle import RepertoireDonneesDVF

def generer_dvf_plus(contexte):    
    repertoire = RepertoireDonneesDVF(contexte.repertoire)
    if not repertoire.a_un_fichier_valide():
        sys.exit(repertoire.erreurs[0])
    sous_groupes_departements = repartition_departements(repertoire.departements)    
    for sous_groupe_departements in sous_groupes_departements:
        if creation_dvf(contexte, repertoire, sous_groupe_departements):
            creation_dvf_plus(contexte, sous_groupe_departements)
        contexte.effacer_schemas_dvf_existants = False
    return True

def creation_dvf(contexte, repertoire, departements):
    '''
    CREATION DVF
    '''    
    # initialisation
    script_sql = contexte.chemin_sortie('script_dvf.sql')
    log_integration =  contexte.chemin_sortie('log.txt')
    dvf = DVF(*contexte.parametres_connexion, departements=departements, script=script_sql, log=log_integration) 
    dvf.charger_gestionnaire_depuis_sqlite(BASE_SQLITE)
    dvf.start_script()

    # préparation des tables
    if contexte.effacer_schemas_existants:
        success, _ = dvf.effacer_schemas_commencant_par(dvf.prefixe_schemas_departementaux)
        if not success:
            _code_erreur('dvf1')
        if not dvf.effacer_et_creer_schemas_dvf():
            _code_erreur('dvf1')
        if not dvf.creation_tables():
            _code_erreur('dvf1')
    else:
        if not dvf.effacer_et_creer_schemas_dvf_departementaux():
            _code_erreur('dvf1')
        if not dvf.creation_tables(recreer_tables_principales=False):
            _code_erreur('dvf1')

    if not dvf.creation_tables_annexes(*FICHIERS_ANNEXES):
        _code_erreur('dvf1')   
    
    # import des nouvelles données brutes dans le schema source
    for fichier, nom_table_source in zip(repertoire.fichiers_sources, repertoire.tables_sources):
        if not dvf.importer(fichier, nom_table_source, recherche_differentielle=False):
            _code_erreur('dvf2', fichier)
            
    # creation DVF
    if not dvf.integration_dans_dvf(repertoire.tables_sources):
        _code_erreur('dvf3')
                
    dvf.end_script()
    dvf.deconnecter()
    return True

def creation_dvf_plus(contexte, departements):
    '''
    CREATION DVF+
    '''
    # initialisation du dvf_plus principal (qui reprend tous les départements)
    script = contexte.chemin_sortie('script_dvf_plus.sql')
    dvf_plus = DVF_PLUS(*contexte.parametres_connexion, departements=departements, script=script)
    dvf_plus.charger_gestionnaire_depuis_sqlite(BASE_SQLITE)
    dvf_plus.start_script()  

    if not dvf_plus.creation_tables_dvf_plus(recreer_tables_principales=contexte.effacer_schemas_existants):
        _code_erreur('dvf_plus1')
    
    # répartition des départements en fonction du nombre de threads prévus
    div_departements = repartition_departements(departements)    
    # creation et lancement des threads (avec des dvf_plus secondaires) qui vont se répartir les départements
    threads = []
    for i, div_departement in enumerate(div_departements):
        script = contexte.chemin_sortie('script_dvf_plus_secondaire{0}.sql'.format(str(i)))
        d = DVF_PLUS(*contexte.parametres_connexion, departements=div_departement, script=script)
        thr = DVF_PLUSThread(d, gestionnaire=dvf_plus.gestionnaire)
        threads.append(thr)
        thr.start()

    # attente execution de l'ensemble des threads
    validations_threads =[]
    for thr in threads:
        validations_threads.append(thr.join())
    if False in validations_threads:
        _code_erreur('dvf_plus2')
        
    if not dvf_plus.transformation_tables_dvf():
        _code_erreur('dvf_plus3')

    dvf_plus.end_script()
    dvf_plus.deconnecter()
    return True


class DVF_PLUSThread(threading.Thread):

    def __init__(self, dvf_plus, gestionnaire):
        super().__init__()
        self.dvf_plus = dvf_plus
        self.dvf_plus.gestionnaire = gestionnaire
        self.success = False

    def run(self):
        self.dvf_plus.start_script()
        success = self.dvf_plus.calcul_et_construction_tables_dvf_plus()
        self.dvf_plus.end_script()
        self.dvf_plus.deconnecter()
        self.success = success

    def join(self):
        super().join()
        return self.success

def _code_erreur(code, fichier=None):
    message_erreur = 'Code erreur non défini.'
    if code == 'dvf1':
        message_erreur = '''TRAITEMENT NON ABOUTI - PHASE DE PREPARATION DES TABLES DVF INACHEVEE'''
    elif code == 'dvf2':
        message_erreur = '''TRAITEMENT NON ABOUTI - PHASE D'IMPORT DU FICHIER {0} IMPOSSIBLE'''.format(fichier or '_')
    elif code == 'dvf3':
        message_erreur = '''TRAITEMENT NON ABOUTI - PHASE D'INTEGRATION INACHEVEE'''
    elif code =='dvf_plus1':
        message_erreur = '''TRAITEMENT NON ABOUTI - PHASE DE PREPARATION DES TABLES DVF+ INACHEVEE'''
    elif code =='dvf_plus2':
        message_erreur = '''TRAITEMENT NON ABOUTI - PHASE DE CALCUL DES TABLES DVF+ INACHEVEE'''
    elif code =='dvf_plus3':
        message_erreur = '''TRAITEMENT NON ABOUTI - PHASE DE TRANFORMATION DVF+ FINALE INACHEVEE'''
    sys.exit(message_erreur)
    
#eof