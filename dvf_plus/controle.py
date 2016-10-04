import os
import csv
from datetime import datetime


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


def detection_fichiers_sources(repertoire_donnees):
    fichiers_sources = {}
    tables_sources = []
    reussite, fichiers, departements, erreurs = verification_donnees(repertoire_donnees)
    for i, fichier in enumerate(fichiers):
        fichiers_sources[fichier] = 'tmp' + str(i)
        tables_sources.append('tmp' + str(i))
    return reussite, erreurs, fichiers_sources, tables_sources, departements

def verification_donnees(repertoire):
    try:
        fichiers = _recuperer_fichiers_txt(repertoire)
        if len(fichiers) > 0:
            erreurs = []
            departements = []
            fichiers_controles = []
            noms_fichiers = []
            for fichier in fichiers:
                reussite, departements_tmp, date_max, msg_err = _controler_fichier_txt(fichier)
                if reussite:
                    fichiers_controles.append((date_max, fichier))
                    departements = list(set(departements + departements_tmp))
                else:
                    erreurs.append(msg_err)
            noms_fichiers = _ordonner_fichiers_txt(fichiers_controles)
            if len(noms_fichiers) > 0:
                return True, noms_fichiers, departements, erreurs
            else :
                return False, noms_fichiers, None, ['Aucun fichier txt valide dans le repertoire selectionné.']
        else:
            return False, [], None, ['Aucun fichier txt dans le répertoire selectionné.']  
    except Exception as e:
        return False, [], None, [str(e)]


def _recuperer_fichiers_txt(repertoire):
    return [os.path.join(repertoire, fichier) for fichier in os.listdir(repertoire) if fichier.endswith('.txt')]


def _controler_fichier_txt(fichier):
    departements = []
    date_max = datetime(1,1,1,0,0)
    with open(fichier, 'r', encoding='utf-8') as f:
        csv_reader = csv.reader(f, delimiter = '|')
        next(csv_reader)
        for n, ligne in enumerate(csv_reader):
            if len(ligne) != 43:
                return False, [], None, 'La ligne {0} du fichier {1} ne possède pas le bon nombre de champs. Le fichier est ignoré.'.format(str(n+1), fichier)
            if ligne[18] not in departements:
                departements.append(ligne[18])
            if datetime.strptime(ligne[8], '%d/%m/%Y') > date_max:
                date_max =  datetime.strptime(ligne[8], '%d/%m/%Y')
    return True, departements, date_max, ''


def _ordonner_fichiers_txt(fichiers):
    fichiers.sort()
    fichiers_ordonnes = [fichier for date, fichier in fichiers]
    fichiers_ordonnes.reverse()
    return fichiers_ordonnes
