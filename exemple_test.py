# -*- coding: utf-8 -*-

from pylpp import LPPDatabase


filepath = './data/lpp_fiche_tot861.dbf'
output_lppcsvfile = './lpp_test.csv'

lppdb = LPPDatabase(filepath)
lppdb.filter(exclude_outdated=True, arbo1_exclude=[2, 3], limit=200)
print(len(lppdb.database))

lppdb.populate_online(thread_count=25, batch_size=8)

lppdb.write_CSV(output_lppcsvfile)
