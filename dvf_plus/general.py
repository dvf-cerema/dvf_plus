import os, sys, threading
from .traitement.dvfclass import DVF
from .traitement.dvfclass import DVF_PLUS
from .traitement import BASE_SQLITE
from .traitement import FICHIERS_ANNEXES
from .controle import repartition_departements, detection_fichiers_sources

def generer_dvf_plus(parametres_connexion, repertoire_donnees, effacer_schemas_dvf_existants=True, repertoire_scripts = 'sorties'):
    reussite, erreurs, fichiers_sources, tables_sources, departements = detection_fichiers_sources(repertoire_donnees)
    if not reussite:
        sys.exit(erreurs[0])
    sous_groupes_departements = repartition_departements(departements)    
    for sous_groupe_departements in sous_groupes_departements:
        valid_dvf = creation_dvf(parametres_connexion, 
                                 fichiers_sources, 
                                 tables_sources, 
                                 sous_groupe_departements, 
                                 effacer_schemas_dvf_existants=effacer_schemas_dvf_existants, 
                                 repertoire_scripts = repertoire_scripts)
        if valid_dvf:
            valid_dvfplus = creation_dvf_plus(parametres_connexion, 
                                              sous_groupe_departements, 
                                              effacer_schemas_dvf_existants,
                                              repertoire_scripts = repertoire_scripts)
        effacer_schemas_dvf_existants = False
    return True

def creation_dvf(parametres_connexion, fichiers_sources, tables_sources, departements, effacer_schemas_dvf_existants, repertoire_scripts = 'sorties'):
    '''
    CREATION DVF
    '''    
    # initialisation
    dvf = DVF(*parametres_connexion, 
              departements=departements, 
              script = os.path.join(repertoire_scripts, 'script_dvf.sql'), 
              log = os.path.join(repertoire_scripts, 'log.txt'))
    dvf.charger_gestionnaire_depuis_sqlite(BASE_SQLITE)
    dvf.start_script()

    # préparation des tables
    valid_preparation = False
    valid_creation_table = False
    if effacer_schemas_dvf_existants:
        valid, nb = dvf.effacer_schemas_commencant_par(dvf.prefixe_schemas_departementaux)
        if valid:
            valid2 = dvf.effacer_et_creer_schemas_dvf()
            if valid2:
                valid_creation_table = dvf.creation_tables()
    else:
        valid = dvf.effacer_et_creer_schemas_dvf_departementaux()
        if valid:
            valid_creation_table = dvf.creation_tables(recreer_tables_principales=False)
    if valid_creation_table:
        valid_preparation = dvf.creation_tables_annexes(*FICHIERS_ANNEXES)

    if not valid_preparation:
        sys.exit('''TRAITEMENT NON ABOUTI - PHASE DE PREPARATION DES TABLES DVF INACHEVEE''')    
    
    # import des nouvelles données brutes dans le schema source
    for fichier, nom_table_source in fichiers_sources.items():
        valid_import = dvf.importer(fichier, nom_table_source, recherche_differentielle=False)
        if not valid_import:
            sys.exit('''TRAITEMENT NON ABOUTI - PHASE D'IMPORT DU FICHIER {0} IMPOSSIBLE'''.format(fichier))

    # creation DVF
    valid_integration = dvf.integration_dans_dvf(tables_sources)
    if not valid_integration:
        sys.exit('''TRAITEMENT NON ABOUTI - PHASE D'INTEGRATION INACHEVEE''')
        
    dvf.end_script()
    dvf.deconnecter()
    return True

def creation_dvf_plus(parametres_connexion, departements, effacer_schemas_dvf_existants, repertoire_scripts = 'sorties'):
    '''
    CREATION DVF+
    '''
    # initialisation du dvf_plus principal (qui reprend tous les départements)
    dvf_plus = DVF_PLUS(*parametres_connexion, 
                        departements = departements, 
                        script = os.path.join(repertoire_scripts, 'script_dvf_plus.sql'))
    dvf_plus.charger_gestionnaire_depuis_sqlite(BASE_SQLITE)
    dvf_plus.start_script()  

    valid_creation_table = dvf_plus.creation_tables_dvf_plus(recreer_tables_principales = effacer_schemas_dvf_existants)
    if not valid_creation_table:
        sys.exit('''TRAITEMENT NON ABOUTI - PHASE DE PREPARATION DES TABLES DVF+ INACHEVEE''')
    
    # répartition des départements en fonction du nombre de threads prévus
    div_departements = repartition_departements(departements)    
    # creation et lancement des threads (avec des dvf_plus secondaires) qui vont se répartir les départements
    threads = []
    for i, div_departement in enumerate(div_departements):
        d = DVF_PLUS(*parametres_connexion, 
                     departements = div_departement, 
                     script = os.path.join(repertoire_scripts, 'script_dvf_plus_secondaire{0}.sql'.format(str(i))))
        thr = DVF_PLUSThread(d, gestionnaire = dvf_plus.gestionnaire)
        threads.append(thr)
        thr.start()

    # attente execution de l'ensemble des threads
    validations_threads =[]
    for thr in threads:
        validations_threads.append(thr.join())

    if False in validations_threads:
        sys.exit('''TRAITEMENT NON ABOUTI - PHASE DE CALCUL DES TABLES DVF+ INACHEVEE''')

    valid_transformation = dvf_plus.transformation_tables_dvf()
    if not valid_transformation:
        sys.exit('''TRAITEMENT NON ABOUTI - PHASE DE TRANFORMATION DVF+ FINALE INACHEVEE''')

    dvf_plus.end_script()
    dvf_plus.deconnecter()
    return True


class DVF_PLUSThread(threading.Thread):

    def __init__(self, dvf_plus, gestionnaire):
        super().__init__()
        self.dvf_plus = dvf_plus
        self.dvf_plus.gestionnaire = gestionnaire
        self.valid = False

    def run(self):
        self.dvf_plus.start_script()
        valid = self.dvf_plus.calcul_et_construction_tables_dvf_plus()
        self.dvf_plus.end_script()
        self.dvf_plus.deconnecter()
        self.valid = valid

    def join(self):
        super().join()
        return self.valid
    
#eof