# -*- coding: utf-8 -*-

from pylpp import LPPDatabase


filepath = './data/lpp_fiche_tot861.dbf'
output_lppcsvfile = './lpp.csv'

lppdb = LPPDatabase(filepath)
lppdb.filter(exclude_outdated=False)

lppdb.populate_online()

lppdb.write_CSV(output_lppcsvfile)
