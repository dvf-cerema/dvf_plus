'''
@author: antoine.herman
'''
from pg.pgbasics import *
from pg.pgsqlite import SqliteConn
import csv

# Dictionnaire décrivant les tables DVF+ et DV3F
TABLES = {}
TABLES[''] = ''
TABLES['mutation'] = 'table des mutations'
TABLES['disposition_parcelle' ] = 'table des parcelles attachées à une disposition'
TABLES['local'] = 'table des locaux'
TABLES['acheteur_vendeur'] = 'table anonymisée des acheteurs et des vendeurs'
TABLES['adresse'] = 'table contenant les adresses (provenant des parcelles et des locaux'
TABLES['adresse_dispoparc'] = 'table de liaison entre la table adresse et la table disposition_parcelle'
TABLES['adresse_local'] = 'table de liaison entre la table adresse et la table local'
TABLES['disposition'] = 'table des dispositions'
TABLES['lot'] = 'table des lots (seuls les 5 premiers lots sont mentionnés)'
TABLES['mutation_article_cgi'] = 'table des articles du code général des impôts (CGI) attachés à la mutation'
TABLES['parcelle'] = 'table des parcelles'
TABLES['suf'] = 'table des subdivisions fiscales'
TABLES['volume'] = 'table des volumes (division de l\'espace dans la hauteur pour certaines co-propriétés verticales'
TABLES['ann_cgi'] = 'table contenant les différents articles CGI'
TABLES['ann_nature_culture'] = 'table contenant les différentes natures de culture'
TABLES['ann_nature_culture_speciale'] = 'table contenant les différentes natures de culture spéciale'
TABLES['ann_nature_mutation'] = 'table contenant les natures de mutation'
TABLES['ann_type_local'] = 'table contenant les types de locaux'

class VariableDVF():
    '''
    Objet représentant une variable des modèles DVF+ ou DV3F
    '''
    
    ETAPES = {1 : 'DVF', 2 : 'DVF+', 3 : 'DV3F'}

    def __init__(self, position, nom, type, table, contrainte, description, table_pour_creation, code_etape):
        '''
        Constructeur
        '''
        self.position = position
        self.nom = nom.strip()
        self.type = type
        self.table = table.strip()
        self.contrainte = contrainte
        self.description = description              
        self.table_pour_creation = table_pour_creation.strip() if table_pour_creation else table_pour_creation
        self.code_etape = code_etape # 1 pour DVF / 2 pour DVF+ / 3 pour DV3F
    
    def __repr__(self):
        return '''Variable {0}({1}) - table {2}({3}) '''.format(self.nom, self.type, self.table, self.position)
    
    def __lt__(self, autre):
        return int(self.position) < int(autre.position)
    
    def creer_depuis_ligne_csv(self, ligne):
        self.nom, self.description, self.type, self.contrainte, self.table_pour_creation = ligne[:5]
        self.code_etape = self.coder_etape(ligne[5])
        self.table = ligne[6]
        self.position = int(ligne[7])
    
    def est_seulement_DVF(self):
        '''
        Renvoie vrai si la variable est uniquement du modèle DVF
        '''
        verification = True if self.code_etape == 1 else False
        return verification
    
    def est_seulement_DVF_plus(self):
        '''
        Renvoie vrai si la variable est du modèle DVF+ mais pas DV3F
        '''
        verification = True if (self.code_etape <= 2) else False
        return verification
    
    def est_DV3F(self):
        '''
        Renvoie vrai si la variable est du modèle DV3F
        '''
        verification = True if (self.code_etape <= 3) else False
        return verification
    
    def traduire_code_etape(self):
        '''
        Renvoie la valeur littérale du code_etape
        '''               
        return self.ETAPES[self.code_etape]
    
    def coder_etape(self, valeur):
        return [c for c,v in self.ETAPES.items() if v==valeur][0]

    def format_csv(self, separateur):
        '''
        Renvoie les caractéristiques de la variable en format csv
        '''
        champs = [champ if champ else '' for champ in self._liste_champs_pour_csv()]        
        return separateur.join(champs) + '\n'

    def _liste_champs_pour_csv(self):
        '''
        Renvoie la liste des valeurs ordonnée selon l'ordre d'apparition dans une ligne d'un fichier csv.
        '''
        return [self.nom, self.description, self.type, self.contrainte, self.table_pour_creation, self.traduire_code_etape(), self.table, str(self.position)]
    
        
class TableDVF():
    '''
    Objet représentant une table des modèles DVF+ ou DV3F
    '''
    
    def __init__(self, nom, variables = [], description = ''):
        '''
        Constructeur
        '''
        self.nom = nom.strip()
        self.description = TABLES[self.nom]
        self.variables = variables
        self.trier_variables()
        
    def __repr__(self):
        return '''Table {0} - {1} champs\n{2} '''.format(self.nom, str(len(self.variables)), '\n'.join([var.__repr__() for var in self.variables]))
    
    def creer_depuis_entete_csv(self, ligne):
        self.nom = ligne[0].split(':')[1].strip()
        self.description = ligne[1]
    
    def ajouter_variable(self, variable):
        self.variables.append(variable)
        self.trier_variables()
        
    def trier_variables(self):
        self.variables = sorted(self.variables)
        
    def tableDVF(self):
        '''
        Renvoie un objet table avec uniquement les variables du modèle DVF
        '''
        variables_dvf = [var for var in self.variables if var.est_seulement_DVF()]
        return TableDVF(self.nom, variables_dvf)
    
    def tableDVF_plus(self):
        '''
        Renvoie un objet table avec uniquement les variables du modèle DVF plus. Le nom de la table est modifié est suffixé avec un "_plus" 
        '''
        variables_dvf_plus = [var for var in self.variables if var.est_seulement_DVF_plus()]
        return TableDVF(self.nom, variables_dvf_plus)
    
    def lister_variables_et_types(self):
        return ',\n'.join([var.nom + ' ' + var.type for var in self.variables])
    
    def lister_tables_pour_creation(self):
        return set([var.table_pour_creation for var in self.variables if var.table_pour_creation])
    
    def lister_nom_variables_ayant_table_creation(self, table):
        return [var.nom for var in self.variables if var.table_pour_creation == table]
    
    def format_csv(self, separateur ='|'):
        '''
        Renvoie le contenu de l'objet TableDVF sous format csv.
        '''
        return self._creer_entete_en_csv(separateur) + self._creer_variables_en_csv(separateur)
        
    def _creer_entete_en_csv(self, separateur):
        '''
        Renvoie l'entete csv de la table TableDVF
        '''
        return 'table : {0} {1} {2}\n'. format(self.nom, separateur, self.description)
    
    def _creer_variables_en_csv(self, separateur):
        '''
        Renvoie les variables en format csv de la table TableDVF
        '''
        lignes =''
        for variable in self.variables:
            lignes += variable.format_csv(separateur)
        return lignes  

        
class GestionVariablesDVF():
    '''
    Classe permettant de gérer les tables et variables de DVF+ ou DV3F à partir de la table docmanager_variable ou depuis un csv.
    '''
    
    def __init__(self, tables = []):
        '''
        Constructeur
        '''
        self.tables = tables
    
    def charger_tables_depuis_postgres(self, hote, base, port, utilisateur, motdepasse, schema, table_des_variables):
        '''
        Créer les objets TableDVF à partir des données de la table
        '''
        pgconn = PgConn(hote, base, port, utilisateur, motdepasse)
        
        sql = '''SELECT DISTINCT table_associee FROM {0}.{1}'''.format(schema, table_des_variables)
        reponse = pgconn.execute_recupere(sql)
        tables = [r[0] for r in reponse]
        
        for table in tables:
            variables = self._recuperer_variables_depuis_postgres(table, pgconn, schema, table_des_variables)
            self.tables.append(TableDVF(table, variables))
    
    def charger_tables_depuis_sqlite(self, fichier_sqlite, table_des_variables = 'docdv3f_variable'):
        '''
        Créer les objets TableDVF à partir des données de la table
        '''
        sqliteconn = SqliteConn(fichier_sqlite)
        
        sql = '''SELECT DISTINCT table_associee FROM {0}'''.format(table_des_variables)
        reponse = sqliteconn.execute_recupere(sql)
        tables = [r[0] for r in reponse]
        
        for table in tables:
            variables = self._recuperer_variables_depuis_sqlite(table, sqliteconn, table_des_variables)
            self.tables.append(TableDVF(table, variables))
        
    def charger_tables_depuis_csv(self, fichier, separateur = '|', encodage = 'utf-8'):
        
        tables_vides = []
        variables = []
        for ligne in self._lire_donnees_csv(fichier, separateur, encodage):
            objet = self._traitement_ligne_csv(ligne)
            if type(objet) is TableDVF:
                tables_vides.append(objet)
            elif type(objet) is VariableDVF:
                variables.append(objet)
        self._integrer_tables_vides_et_variables(tables_vides, variables)
    
    def table(self, nom):
        for table in self.tables:
            if table.nom == nom:
                return table
    
    def _lire_donnees_csv(self, fichier, separateur, encodage):    
        with open(fichier, 'rt', encoding = encodage) as f:
            lignes = csv.reader(f, delimiter = separateur)
            for ligne in lignes:
                yield ligne
    
    def _traitement_ligne_csv(self, ligne):
        if self._est_ligne_entete_table(ligne):
            t = TableDVF('')
            t.creer_depuis_entete_csv(ligne)
            return t
        elif self._est_ligne_variable(ligne):
            v = VariableDVF(0, '', '', '', '', '', '', 0)
            v.creer_depuis_ligne_csv(ligne)
            return v
        else:
            return None
    
    def _est_ligne_entete_table(self, ligne):
        return (len(ligne) == 2 and ligne[0].startswith('table :'))          
    
    def _est_ligne_variable(self, ligne):
        return len(ligne) == 8 
    
    def _integrer_tables_vides_et_variables(self, tables, variables):
        for variable in variables:
            for table in tables:
                if variable.table == table.nom:
                    table.ajouter_variable(variable)
        self.tables = tables
        for table in self.tables:
            print(table)
            

    def creer_csv(self, fichier, separateur = '|', encodage = 'utf-8'):
        '''
        Creer un fichier csv avec les données des tables (et des variables) qui ont été chargées dans l'objet.
        '''
        contenu_csv = ''
        for table in self.tables:
            contenu_csv += table.format_csv(separateur)
        with open(fichier, 'wt', encoding = encodage) as f:
            print(contenu_csv, end='\n', file=f)
        
    
    def _recuperer_variables_depuis_postgres(self, table, pgconn, schema, table_des_variables):
        '''
        Envoie une requete SQL pour récupérer les données des variables et les renvoie sous forme de liste d'objet "VariableDVF".
        '''
        sql = '''
            SELECT 
                    position,
                    nom,
                    REPLACE(REPLACE(replace(replace(replace(replace(replace(type, 'Entier', 'integer'), 'Texte', 'text'), 'Caractère','varchar'), 'Vrai/Faux', 'boolean'), 'Décimal', 'numeric'), 'Géométrie','geometry'),'Série', 'serial'),                    
                    table_associee,
                    contrainte,
                    description,                    
                    table_pour_creation,
                    code_etape_pour_creation
            FROM {0}.{1} 
            WHERE table_associee = '{2}' 
            ORDER BY position;'''.format(schema, table_des_variables, table)
        reponse = pgconn.execute_recupere(sql)
        
        variables = []
        for ligne in reponse:
            variables.append(VariableDVF(*ligne))
        return variables

    def _recuperer_variables_depuis_sqlite(self, table, sqliteconn, table_des_variables = 'docdv3f_variable'):            
        sql = '''
        SELECT 
            position,
            nom,
            type,                    
            table_associee,
            contrainte,
            description_simplifiee,                    
            table_pour_creation,
            code_modele
        FROM {0} 
        WHERE table_associee = '{1}' 
        ORDER BY position;
        '''.format(table_des_variables, table)
        reponse = sqliteconn.execute_recupere(sql)
        
        variables = []
        for ligne in reponse:
            variables.append(VariableDVF(*ligne))
        return variables
        