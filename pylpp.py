from datetime import date
from dbfread import DBF
from lxml import etree
from time import sleep
from urllib import request
from urllib.error import HTTPError
import concurrent.futures
import csv

def cutlist(iterable, n):
    for i in range(0, len(iterable), n):
        yield iterable[i:i+n]

def _lpp_data_process(element):
    if len(element) == 0:
        return None
    else:
        return element[0].text.replace('\n', '').replace('<br>', '').strip()

# traiter erreur. xpath avec recherche "tarif" ?
lpp_fields_config = [
  {'field': 'Description', 'xpath':'/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td/table[3]/tr/td/font', 'processing_function': _lpp_data_process}
 ,{'field': 'Date début validité', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "Date début validité"]/../../td[3]', 'processing_function': _lpp_data_process}
 ,{'field': 'Date fin validité', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "Date fin validité"]/../../td[3]', 'processing_function': _lpp_data_process}
 ,{'field': 'Tarif', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "Tarif"]/../../td[3]', 'processing_function': _lpp_data_process}
 #,{'field': 'Prix unitaire réglementé', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "Prix unitaire réglementé"]/../../td[3]', 'processing_function': _lpp_data_process}
 ,{'field': 'Montant max remboursement', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "Montant max remboursement"]/../../td[3]', 'processing_function': _lpp_data_process}
 ,{'field': 'Quantité max remboursement', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "Quantité max remboursement"]/../../td[3]', 'processing_function': _lpp_data_process}
 ,{'field': 'Entente préalable', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "Entente préalable"]/../../td[3]', 'processing_function': _lpp_data_process}
 ,{'field': 'Indications', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "Indications"]/../../td[3]', 'processing_function': _lpp_data_process}
 ,{'field': 'Identifiant', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "Identifiant"]/../../td[3]', 'processing_function': _lpp_data_process}
 ,{'field': 'Age maxi', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "Age maxi"]/../../td[3]', 'processing_function': _lpp_data_process}
 ,{'field': 'Nature de prestation', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "Nature de prestation"]/../../td[3]', 'processing_function': _lpp_data_process}
 ,{'field': 'Type de prestation', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "Type de prestation"]/../../td[3]', 'processing_function': _lpp_data_process}
 ,{'field': 'GUADELOUPE', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "GUADELOUPE"]/../../td[2]', 'processing_function': _lpp_data_process}
 ,{'field': 'MARTINIQUE', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "MARTINIQUE"]/../../td[2]', 'processing_function': _lpp_data_process}
 ,{'field': 'GUYANE', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "GUYANE"]/../../td[2]', 'processing_function': _lpp_data_process}
 ,{'field': 'REUNION', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "REUNION"]/../../td[2]', 'processing_function': _lpp_data_process}
 ,{'field': 'SAINT-PIERRE-ET-MIQUELON', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "SAINT-PIERRE-ET-MIQUELON"]/../../td[2]', 'processing_function': _lpp_data_process}
 ,{'field': 'MAYOTTE', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "MAYOTTE"]/../../td[2]', 'processing_function': _lpp_data_process}
]

def filter_lpp_db(lpp_db, exclude_outdated=True, arbo1_exclude=None, limit=None):
    lpp_db_filtered = []
    i = 0
    for lpp_db_item in lpp_db:
        if exclude_outdated and lpp_db_item['DATE_FIN']:
            if lpp_db_item['DATE_FIN'] < date.today():
                continue
        if arbo1_exclude and int(lpp_db_item['ARBO1']) in arbo1_exclude:
            continue
        lpp_db_filtered.append(lpp_db_item)
        i += 1
        if limit and i >= limit:
            break
    return lpp_db_filtered

def get_lpp_data_online(lpp_db):
    augmented_lpp_records = []
    for lpp_db_item in lpp_db:
        cnamt_url = cnamt_baseurl.format(lppcode=lpp_db_item['CODE_TIPS'])
        query = request.Request(cnamt_url, method='GET')
        with request.urlopen(query) as result:
            if 200 <= result.status <= 204:
                result_data = result.read()
                tree = etree.HTML(result_data)
                for conf in lpp_fields_config:
                    field_element = tree.xpath(conf['xpath'])

                    if(type(field_element) == list and len(field_element) != 1): #a virer de la
                        pass #log.debug -> print("XPath query failed ({} results)".format(len(field_element)))
                    field_value_prcsed = conf['processing_function'](field_element)
                    lpp_db_item.update({conf['field']: field_value_prcsed})
                augmented_lpp_records.append(lpp_db_item)
            else:
                raise HTTPError(result.reason)
    return augmented_lpp_records


filepath = './lpp_fiche_tot861.dbf'
output_lppcsvfile = './lpp_allref.csv'
cnamt_baseurl = 'http://www.codage.ext.cnamts.fr/cgi/tips/cgi-fiche?p_code_tips={lppcode}&p_date_jo_arrete=%25&p_menu=FICHE&p_site=AMELI'

lpp_dbf0 = DBF(filepath, encoding='cp1252')
lpp_dbf = filter_lpp_db(lpp_dbf0, exclude_outdated=False) #, arbo1_exclude=[2, 3])
print(len(lpp_dbf))

# Parallel execution
executor = concurrent.futures.ThreadPoolExecutor(25)
threads = [executor.submit(get_lpp_data_online, group) for group in cutlist(lpp_dbf, 300)]
concurrent.futures.wait(threads)
lpp_records = [ lpp_record for thread in threads for lpp_record in thread.result() ]

with open(output_lppcsvfile, 'w') as csvfile:
    fieldnames = lpp_records[0].keys()
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';', quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    for lpp_record in lpp_records:
        writer.writerow(lpp_record)

