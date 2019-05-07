import os
import csv
from datetime import datetime
from operator import attrgetter
from builtins import property


class RepertoireDonneesDVF():
    
    def __init__(self, repertoire):
        self.repertoire = repertoire
        self.fichiers_txt = self.recuperer_fichiers_txt()

    def recuperer_fichiers_txt(self):
        fichiers_txt = [os.path.join(self.repertoire, fichier) for fichier in os.listdir(self.repertoire) if fichier.endswith('.txt')]
        return [FichierDonneesDVF(fichier) for fichier in fichiers_txt]
    
    @property
    def fichiers_valides(self):        
        return [fichier for fichier in self.fichiers_txt if fichier.valide]
    
    @property
    def departements(self):
        departements_tmp = []
        for fichier in self.fichiers_valides:
            departements_tmp.extend(fichier.departements)
        return list(set(departements_tmp))
    
    @property
    def fichiers_sources(self):
        '''
        Renvoie la liste des chemin des fichiers DVF valides du répertoire, classé du plus récent au plus ancien
        '''
        fichiers_valides_tries = sorted(self.fichiers_valides, key=attrgetter('opendata', 'date_max', 'chemin_fichier'), reverse=True)
        return [fichier.chemin_fichier for fichier in fichiers_valides_tries]
    
    @property
    def tables_sources(self):
        return ['tmp' + str(i) for i, fichier in enumerate(self.fichiers_sources)]
    
    @property
    def erreurs(self):
        if len(self.fichiers_txt) == 0:
            return ['Aucun fichier txt dans le répertoire selectionné.']
        if len(self.fichiers_valides) == 0:
            return ['Aucun fichier de données valide dans le répertoire selectionné.']
        return [fichier.erreur for fichier in self.fichiers_valides if fichier.erreur]
    
    def a_un_fichier_valide(self):
        for fichier in self.fichiers_valides:
            if fichier.erreur == '':
                return True
        return False
 
class FichierDonneesDVF():
    
    def __init__(self, chemin_fichier):
        self.chemin_fichier = chemin_fichier
        self._valide = None
        self._open_data = None
        self._departements = None
        self._date_max = None
        self._erreur = None
    
    def parcourir_fichier(self):
        if self._valide is None:
            self._open_data = 1
            self._departements = []
            self._date_max = datetime(1,1,1,0,0)
            with open(self.chemin_fichier, 'r', encoding='utf-8') as f:
                csv_reader = csv.reader(f, delimiter = '|')
                next(csv_reader)
                for n, ligne in enumerate(csv_reader):
                    if n == 0 and len(ligne[0]) > 0 and len(ligne[1]) > 0:
                        self._open_data = 2 
                    if len(ligne) != 43:
                        self._valide = False
                        self._erreur =  'La ligne {0} du fichier {1} ne possède pas le bon nombre de champs. Le fichier est ignoré.'.format(str(n+1), self.chemin_fichier)
                        return
                    if ligne[18] not in self._departements:
                        self._departements.append(ligne[18])
                    if datetime.strptime(ligne[8], '%d/%m/%Y') > self._date_max:
                        self._date_max =  datetime.strptime(ligne[8], '%d/%m/%Y')
            self._valide = True
            self._erreur = ''
    
    @property    
    def valide(self):
        self.parcourir_fichier()
        return self._valide
    
    @property
    def opendata(self):
        '''
        1 : opendata
        2 : non opendata
        '''
        self.parcourir_fichier()
        return self._open_data
    
    @property
    def departements(self):
        self.parcourir_fichier()
        return self._departements
        
    @property
    def date_max(self):
        self.parcourir_fichier()
        return self._date_max
    
    @property
    def erreur(self):
        self.parcourir_fichier()
        return self._erreur      

def repartition_departements(departements):
    '''
    permet de découper les départements en sous-listes 
    '''
    if len(departements) <= 2:
        nb_div = 1
    elif len(departements) <= 6:
        nb_div = 3
    else:
        nb_div = round(len(departements)/5)  
    div_departements = []
    for i in range(nb_div):
        div_departements.append(departements[int(i*len(departements)/nb_div):int((i+1)*len(departements)/nb_div)])
    return div_departements

def ajout_no_ligne(fichier_dvf, fichier_tmp):
    '''
    Ajoute le numéro de ligne au début du fichier source DVF
    '''
    with open(fichier_dvf, 'r', encoding='utf-8', newline='') as r:
        with open(fichier_tmp, 'w', encoding='utf-8', newline='') as w:
            datas = csv.DictReader(r, delimiter='|')
            champs = ['id'] + datas.fieldnames
            copy = csv.DictWriter(w, champs, delimiter='|')
            copy.writeheader()
            for i, data in enumerate(datas):
                data['id'] = i+1
                copy.writerow(data)
    return True

