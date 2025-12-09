# -*- coding: utf-8 -*-

from datetime import date
from dbfread import DBF
from lxml import etree
from time import sleep
from urllib import request
from urllib.error import HTTPError
import concurrent.futures
import csv
import logging
import re
import sys


def _lpp_data_process(element):
    if len(element) == 0:
        return None
    else:
        return element[0].text.replace('\n', '').replace('<br>', '').strip()

def _lpp_get_elec_from_descr(lpp_db_item):
    elec_payback = re.search('électricité à raison de ([0-9,]+) euro', lpp_db_item['Description'])
    if elec_payback:
        return elec_payback.group(1)
    else:
        return None

_lpp_fields_config = [
  {'field': 'Description', 'xpath':'/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td/table[3]/tr/td/font'}
 ,{'field': 'Date début validité', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "Date début validité"]/../../td[3]'}
 ,{'field': 'Date fin validité', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "Date fin validité"]/../../td[3]'}
 ,{'field': 'Tarif', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "Tarif"]/../../td[3]'}
 ,{'field': 'Montant max remboursement', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "Montant max remboursement"]/../../td[3]'}
 ,{'field': 'Quantité max remboursement', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "Quantité max remboursement"]/../../td[3]'}
 ,{'field': 'Entente préalable', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "Entente préalable"]/../../td[3]'}
 ,{'field': 'Indications', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "Indications"]/../../td[3]'}
 ,{'field': 'Identifiant', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "Identifiant"]/../../td[3]'}
 ,{'field': 'Age maxi', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "Age maxi"]/../../td[3]'}
 ,{'field': 'Nature de prestation', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "Nature de prestation"]/../../td[3]'}
 ,{'field': 'Type de prestation', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "Type de prestation"]/../../td[3]'}
 ,{'field': 'GUADELOUPE', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "GUADELOUPE"]/../../td[2]'}
 ,{'field': 'MARTINIQUE', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "MARTINIQUE"]/../../td[2]'}
 ,{'field': 'GUYANE', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "GUYANE"]/../../td[2]'}
 ,{'field': 'REUNION', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "REUNION"]/../../td[2]'}
 ,{'field': 'SAINT-PIERRE-ET-MIQUELON', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "SAINT-PIERRE-ET-MIQUELON"]/../../td[2]'}
 ,{'field': 'MAYOTTE', 'xpath': '/html/body/table/tr[2]/td/table/tr[1]/td[3]/table/tr/td//font[text() = "MAYOTTE"]/../../td[2]'}
]

_lpp_calculated_fields = {
  'Remboursement électricité': _lpp_get_elec_from_descr
}


_cnamt_baseurl = 'http://www.codage.ext.cnamts.fr/cgi/tips/cgi-fiche?p_code_tips={lppcode}&p_date_jo_arrete=%25&p_menu=FICHE&p_site=AMELI'

def populate_lpp_item_online(lpp_db, _logger=None):
    augmented_lpp_records = []
    for lpp_db_item in lpp_db:
        cnamt_url = _cnamt_baseurl.format(lppcode=lpp_db_item['CODE_TIPS'])
        query = request.Request(cnamt_url, method='GET')
        with request.urlopen(query) as result:
            if 200 <= result.status <= 204:
                result_data = result.read()
                tree = etree.HTML(result_data)
                for conf in _lpp_fields_config:
                    field_element = tree.xpath(conf['xpath'])
                    if(type(field_element) == list and len(field_element) != 1):
                        if _logger:
                            _logger.debug('Populating, XPath query found {} results (instead of 1)'.format(len(field_element)))
                    field_value_prcsed = _lpp_data_process(field_element)
                    lpp_db_item.update({conf['field']: field_value_prcsed})
                for new_field, calculation in _lpp_calculated_fields.items():
                    lpp_db_item[new_field] = calculation(lpp_db_item)
                augmented_lpp_records.append(lpp_db_item)
            else:
                if _logger:
                    _logger.error('Error {}: {}'.format(result.status, result.reason))
                raise HTTPError(result.reason)
    return augmented_lpp_records


class LPPDatabase:
    def _splitlist(iterable, n):
        for i in range(0, len(iterable), n):
            yield iterable[i:i+n]

    def __init__(self, dbf_filepath, debug=False):
        self._logger = logging.getLogger(__name__)
        if debug:
            self._logger.setLevel(logging.DEBUG)
        else:
            self._logger.setLevel(logging.INFO)
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.DEBUG)
        if debug:
            stdout_handler.setLevel(logging.DEBUG)
        else:
            stdout_handler.setLevel(logging.INFO)
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setLevel(logging.WARNING)
        formatter = logging.Formatter("[%(asctime)s][%(levelname)-8s] %(message)s")
        stdout_handler.setFormatter(formatter)
        stderr_handler.setFormatter(formatter)
        self._logger.addHandler(stdout_handler)
        self._logger.addHandler(stderr_handler)

        self._logger.info('Loading file "{}" ...'.format(dbf_filepath))
        self._database = DBF(dbf_filepath, encoding='cp1252')

    @property
    def database(self):
        return self._database

    def filter(self, exclude_outdated=True, arbo1_exclude=None, limit=None):
        lpp_db_filtered = []
        i = 0
        for lpp_db_item in self._database:
            if exclude_outdated and lpp_db_item['DATE_FIN']:
                if lpp_db_item['DATE_FIN'] < date.today():
                    continue
            if arbo1_exclude and int(lpp_db_item['ARBO1']) in arbo1_exclude:
                continue
            lpp_db_filtered.append(lpp_db_item)
            i += 1
            if limit and i >= limit:
                break
        self._logger.info('Filtered {} rows (out of {})'.format(len(lpp_db_filtered), len(self._database)))
        self._database = lpp_db_filtered

    def populate_online(self, thread_count=30, batch_size=300):
        self._logger.info('Populating online (thread_count={}, batch_size={}) ...'.format(thread_count, batch_size))
        executor = concurrent.futures.ThreadPoolExecutor(thread_count)
        threads = [executor.submit(populate_lpp_item_online, group, self._logger) for group in LPPDatabase._splitlist(self._database, batch_size)]
        for thread in threads:
            thread.add_done_callback( lambda t: self._logger.info('Populate online batch (from {} to {}) suceeded'.format(t.result()[0]['CODE_TIPS'], t.result()[-1]['CODE_TIPS'])) )
        concurrent.futures.wait(threads)
        self._database = [ lpp_record for thread in threads for lpp_record in thread.result() ]
        self._logger.info('Populating online suceeded')

    def write_CSV(self, csv_filepath):
        self._logger.info('Writing CSV "{}" ...'.format(csv_filepath))
        with open(csv_filepath, 'w') as csvfile:
            fieldnames = self._database[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';', quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            for lpp_record in self._database:
                writer.writerow(lpp_record)
