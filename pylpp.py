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
import os
import re
import sys
import zipfile


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


_cnamt_domain = 'http://www.codage.ext.cnamts.fr'
_cnamt_baseurl = 'http://www.codage.ext.cnamts.fr/cgi/tips/cgi-fiche?p_code_tips={lppcode}&p_date_jo_arrete=%25&p_menu=FICHE&p_site=AMELI'
_cnamt_lppversion_url = 'http://www.codage.ext.cnamts.fr/codif/tips/telecharge/index_tele.php?p_site=AMELI'


def populate_lpp_item_online(lpp_db, _logger=None):
    """
    Enrichis la base de données LPP fournie en récupérant les informations disponibles sur le site http://www.codage.ext.cnamts.fr/codif/tips/index.php.

    :param lpp_db: Base de données LPP.
    :type lpp_db: list[dict[str, Any]]
    :param _logger: The logger instance to use for logging (optional).
    :type _logger: logging.Logger
    """
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
                    if(type(field_element) != list or len(field_element) != 1):
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


def download_current_db_online(data_dir, _logger=None):
    """
    Télécharge la version courante de la base de données LPP.

    :param data_dir: Chemin du dossier dans lequel télécharger la base.
    :type data_dir: str
    :param _logger: The logger instance to use for logging (optional).
    :type _logger: logging.Logger
    """
    query = request.Request(_cnamt_lppversion_url, method='GET')
    with request.urlopen(query) as result:
        if 200 <= result.status <= 204:
            db_info = {}
            result_data = result.read()
            tree = etree.HTML(result_data)
            version_element = tree.xpath('/html/body/table/tr[2]/td/table/tr[1]/td[3]/table//font[contains(text(),"Version du")]')
            version = re.search('[0-9]+\/[0-9]+\/[0-9]+', version_element[0].text.strip()).group(0)
            db_info.update({'version': version})
            link_element = tree.xpath('/html/body/table/tr[2]/td/table/tr[1]/td[3]/table//a[contains(text(),".zip")]/@href')
            dl_file = re.search('LPP[0-9]+\.zip', link_element[0].strip()).group(0)
            db_info.update({'archive_file': dl_file})
            dl_filepath = os.path.join(data_dir,dl_file)
            dl_url = _cnamt_domain + link_element[0].strip()
            os.makedirs(data_dir, exist_ok=True)
            if not os.path.isfile(dl_filepath):
                if _logger:
                    _logger.info('Downloading & Unziping LPP database from "{}" ...'.format(dl_url))
                request.urlretrieve(dl_url, dl_filepath)
                with zipfile.ZipFile(dl_filepath, "r") as z:
                    z.extractall(data_dir)
            else:
                if _logger:
                    _logger.debug('LPP database already exists : "{}".'.format(dl_filepath))
            for root, dirs, files in os.walk(data_dir):
                for file in files:
                    if file.endswith('.dbf'):
                        db_info.update({file[:-7]: os.path.join(root, file)})
            if _logger:
                _logger.info('Base LPP courante : {} du {}.'.format(dl_file, version))
            return db_info
        else:
            if _logger:
                _logger.error('Error {}: {}'.format(result.status, result.reason))
            raise HTTPError(result.reason)


class LPPDatabase:
    """
    Réceptable de la base de donnée des codes LPP et des informations qui y sont liées.
    """

    def _splitlist(iterable, n):
        for i in range(0, len(iterable), n):
            yield iterable[i:i+n]

    def getLogger(debug=False):
        """
        Récupère un logger pré-configuré pour écrire sur la console.

        :param debug: Active le mode debug (verbeux)
        :type debug: bool
        """
        logger = logging.getLogger(__name__)
        if not logger.handlers:
            if debug:
                logger.setLevel(logging.DEBUG)
            else:
                logger.setLevel(logging.INFO)
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
            logger.addHandler(stdout_handler)
            logger.addHandler(stderr_handler)
        return logger

    def __init__(self, dbf_filepath, debug=False):
        """
        Créée l'objet base de données LPP à partir d'un fichier ".dbf" du CNAMTS.

        :param dbf_filepath: Chemin vers le fichier ".dbf" téléchargé sur le site www.codage.ext.cnamts.fr.
        :type dbf_filepath: str
        :param debug: Active le mode debug (verbeux)
        :type debug: bool
        """
        self._logger = LPPDatabase.getLogger(debug)

        self._logger.info('Loading file "{}" ...'.format(dbf_filepath))
        self._database = DBF(dbf_filepath, encoding='cp1252')

    @property
    def database(self):
        """
        Base de données LPP.

        :returns: La base de données dans son état actuel (ie. filtrée, enrichie, ...).
        :rtype: list[dict[str, Any]]
        """
        return self._database

    def filter(self, exclude_outdated=True, arbo1_exclude=None, limit=None):
        """
        Filtre la base de données LPP selon plusieurs critères.

        :param exclude_outdated: Critère d'obsolecence - permet d'exclure les codes LPP dont la date de fin est dans le passé.
        :type exclude_outdated: bool
        :param arbo1_exclude: Critère hiérarchique - permet d'exclure les codes LPP appartenant à un "chapitre" donné (on utilise la numérotataion de chapitres suivante http://www.codage.ext.cnamts.fr/codif/tips//chapitre/index_chap.php?p_ref_menu_code=1&p_site=AMELI).
        :type arbo1_exclude: list[int]
        :param limit: Critère de nombre - permet de ne retourner que les N premières lignes (idéal pour les tests).
        :type limit: int
        """
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
        """
        Enrichis la base de données LPP en récupérant les informations disponibles sur le site http://www.codage.ext.cnamts.fr/codif/tips/index.php, avec plusieurs unités de traitement (thread) parallèles.

        :param thread_count: Nombre d'unitées de traitement parallèles (pour la récupération d'informations sur le site).
        :type thread_count: int
        :param batch_size: Nombre de codes LPP donnés à chaque unité de traitement.
        :type batch_size: int
        """
        self._logger.info('Populating online (thread_count={}, batch_size={}) ...'.format(thread_count, batch_size))
        executor = concurrent.futures.ThreadPoolExecutor(thread_count)
        threads = [executor.submit(populate_lpp_item_online, group, self._logger) for group in LPPDatabase._splitlist(self._database, batch_size)]
        for thread in threads:
            thread.add_done_callback( lambda t: self._logger.info('Populate online batch (from {} to {}) suceeded'.format(t.result()[0]['CODE_TIPS'], t.result()[-1]['CODE_TIPS'])) )
        concurrent.futures.wait(threads)
        self._database = [ lpp_record for thread in threads for lpp_record in thread.result() ]
        self._logger.info('Populating online suceeded')

    def write_CSV(self, csv_filepath):
        """
        Ecris la base de données (dans son état actuel, ie. filtrée, enrichie, ...) vers un fichier CSV.

        :param csv_filepath: Path of the CSV file to write.
        :type csv_filepath: str
        """
        self._logger.info('Writing CSV "{}" ...'.format(csv_filepath))
        with open(csv_filepath, 'w') as csvfile:
            fieldnames = self._database[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';', quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            for lpp_record in self._database:
                writer.writerow(lpp_record)
