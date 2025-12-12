# -*- coding: utf-8 -*-

from pylpp import LPPDatabase, download_current_db_online

# Téléchargement de la base .dbf
db_info = download_current_db_online('./data', LPPDatabase.getLogger())
filepath = db_info['lpp_fiche_tot']

# Chemin du fichier CSV de sortie
output_lppcsvfile = './lpp_test.csv'

# Enrichissement en ligne et écriture du fichier de base de données
lppdb = LPPDatabase(filepath)
lppdb.filter(exclude_outdated=True, arbo1_exclude=[2, 3], limit=200)
lppdb.populate_online(thread_count=25, batch_size=8)
lppdb.write_CSV(output_lppcsvfile)
