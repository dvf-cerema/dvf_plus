import os

REPERTOIRE_MODULE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_SQLITE = os.path.join(REPERTOIRE_MODULE, 'ressources', 'table_variables.sqlite3')
FICHIERS_ANNEXES = (os.path.join(REPERTOIRE_MODULE, 'ressources', 'artcgil135b.csv'), 
                    os.path.join(REPERTOIRE_MODULE, 'ressources', 'natcult.csv'), 
                    os.path.join(REPERTOIRE_MODULE, 'ressources','natcultspe.csv'))