from dbfread import DBF
from urllib import request
from lxml import etree
from time import sleep
import csv

filepath = './lpp_fiche_tot861.dbf'
output_lppcsvfile = './lpp.csv'
cnamt_baseurl = 'http://www.codage.ext.cnamts.fr/cgi/tips/cgi-fiche?p_code_tips={lppcode}&p_date_jo_arrete=%25&p_menu=FICHE&p_site=AMELI'

lpp_records = []

lpp_dbf = DBF(filepath, encoding='cp1252')

# 35k print(len(lpp_dbf))

for record in lpp_dbf:
    #print(record['CODE_TIPS'])
    cnamt_url = cnamt_baseurl.format(lppcode=record['CODE_TIPS'])
    query = request.Request(cnamt_url, method='GET')
    with request.urlopen(query) as result:
        if 200 <= result.status <= 204:
            result_data = result.read()
            tree = etree.HTML(result_data)
            r = tree.xpath('/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td/table[3]/tr/td/font')
            if(len(r) == 1):
                #print(r[0].text.replace('\n', ''))
                record.update({'Description': r[0].text.replace('\n', '')})
            else:
                print('ERROR')
                exit(0)
            lpp_records.append(record)
        else:
            print(result.status)
            print(result.reason)
    #sleep(.01)
    #print(lpp_records)
    #print('.', end='')
    #if record['CODE_TIPS'] == '2776986':
    #    break
        #print(result_data)
        #exit(0)

with open(output_lppcsvfile, 'w') as csvfile:
    fieldnames = lpp_records[0].keys()
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';', quoting=csv.QUOTE_MINIMAL)

    writer.writeheader()
    for lpp_record in lpp_records:
        writer.writerow(lpp_record)
