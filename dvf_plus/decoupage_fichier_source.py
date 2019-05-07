import os
import csv
 
def decoupage_departemental(repertoire, fichier_src, departements):
    chemin_src= os.path.join(repertoire, fichier_src)
    chemin_out = os.path.join(repertoire, fichier_src[:-4] + '_d{0}.txt'.format('-'.join(departements)))
    with open(chemin_src, 'r', encoding='utf-8', newline='') as r:
        with open(chemin_out, 'w', encoding='utf-8', newline='') as w:
            datas = csv.DictReader(r, delimiter='|')
            champs = datas.fieldnames
            copy = csv.DictWriter(w, champs, delimiter='|')
            copy.writeheader()
            for data in datas:
                if data['Code departement'] in departements:
                    copy.writerow(data)
    return True

if __name__ == '__main__':
    decoupage_departemental('F:/test_data', "valeursfoncieres-2014.txt", ['22', '29','35', '56'])