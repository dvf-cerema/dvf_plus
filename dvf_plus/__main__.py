import sys
import os
import argparse
import psycopg2
from .general import generer_dvf_plus

def cmd_line(args):
    parser = argparse.ArgumentParser(description='Module permettant de générer une base DVF+')
    parser.add_argument('repertoire', action='store', default='.', help='Répertoire contenant les données sources')
    parser.add_argument('-H', '--host', action='store', help="hôte du serveur PostgreSQL", default='localhost')
    parser.add_argument('-d', '--database', action='store', help="nom de la base de données", required= True)
    parser.add_argument('-p', '--port', action='store', help="port d'écoute du serveur PostgreSQL", default='5432')
    parser.add_argument('-u', '--user', action='store', help="nom de l'utilisateur PostgreSQL", default='postgres')
    parser.add_argument('-w', '--password', action='store', help="mot de passe de l'utilisateur PostgreSQL", default='postgres')
    parser.add_argument('-N', '--no-delete', action='store_true', help="ne pas effacer les schemas dvf_dXX des autres départements existants",default=False)
    return parser.parse_args()

def validation_repertoire(repertoire):
    if repertoire == '.':
        repertoire = os.getcwd()
    if not os.path.isdir(repertoire):
        sys.exit("Erreur :{0} n'est pas un chemin de répertoire valide.".format(repertoire))
    return repertoire

def validation_connexion(hote, bdd, port, utilisateur, mdp):
    try:
        conn = psycopg2.connect(host=hote, database=bdd, port=port, user=utilisateur, password=mdp)
        return (hote, bdd, port, utilisateur, mdp)
    except Exception as e:
        sys.exit("Connexion à la base de données impossible : {0}".format(str(e)))

def main(args):
    args = cmd_line(args)
    repertoire = validation_repertoire(args.repertoire)       
    parametres_connexion = validation_connexion(args.host, args.database, args.port, args.user, args.password)        
    effacer_schemas_dvf_existants = not args.no_delete
    repertoire_sortie_scripts = os.path.join(repertoire, 'sorties')
    if not os.path.isdir(repertoire_sortie_scripts):
        os.mkdir(repertoire_sortie_scripts)
    generer_dvf_plus(parametres_connexion, 
                     repertoire, 
                     effacer_schemas_dvf_existants = effacer_schemas_dvf_existants,
                     repertoire_scripts = repertoire_sortie_scripts)

if __name__=='__main__':
    args = sys.argv
    main(args)
    print('''TRAITEMENT TERMINE''')
    
#eof