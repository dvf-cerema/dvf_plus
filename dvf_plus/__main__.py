import sys
import os
import argparse
import psycopg2
from .general import generer_dvf_plus

def main():
    args = sys.argv
    contexte = Contexte(args)
    generer_dvf_plus(contexte)

class Contexte():
    
    def __init__(self, args):
        args = self.cmd_line(args)
        self.repertoire = self.validation_repertoire(args.repertoire)
        self.parametres_connexion = self.validation_connexion(host=args.host, bdd=args.database, port=args.port, user=args.user, pwd=args.password)
        self.effacer_schemas_existants = not args.no_delete

    def cmd_line(self, args):
        parser = argparse.ArgumentParser(description='Module permettant de générer une base DVF+')
        parser.add_argument('repertoire', default='.', help='Répertoire contenant les données sources')
        parser.add_argument('-H', '--host', help="hôte du serveur PostgreSQL", default='localhost')
        parser.add_argument('-d', '--database', help="nom de la base de données", required= True)
        parser.add_argument('-p', '--port', help="port d'écoute du serveur PostgreSQL", default='5432')
        parser.add_argument('-u', '--user', help="nom de l'utilisateur PostgreSQL", default='postgres')
        parser.add_argument('-w', '--password', help="mot de passe de l'utilisateur PostgreSQL", default='postgres')
        parser.add_argument('-N', '--no-delete', action='store_true', help="ne pas effacer les schemas dvf_dXX des autres départements existants",default=False)
        return parser.parse_args()
        
    def validation_repertoire(self, repertoire):
        repertoire = os.path.abspath(repertoire)
        if not os.path.isdir(repertoire):
            sys.exit("Erreur :{0} n'est pas un chemin de répertoire valide.".format(repertoire))
        return repertoire
    
    def validation_connexion(self, host, bdd, port, user, pwd):
        try:
            conn = psycopg2.connect(host=host, database=bdd, port=port, user=user, password=pwd)
            return (host, bdd, port, user, pwd)
        except Exception as e:
            sys.exit("Connexion à la base de données impossible : {0}".format(str(e)))
    
    def chemin_sortie(self, nom_fichier):
        repertoire_scripts = os.path.join(self.repertoire, 'sorties')
        if not os.path.isdir(repertoire_scripts):
            os.mkdir(repertoire_scripts)
        return os.path.join(repertoire_scripts, nom_fichier)

if __name__=='__main__':    
    main()
    print('''TRAITEMENT TERMINE''')
    
#eof