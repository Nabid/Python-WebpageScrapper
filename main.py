import csv
import os, sys
import configparser as cs
import logging
from bs4 import BeautifulSoup as soup
from urllib.request import urlopen as uReq
from urllib.error import HTTPError, URLError
import socket
from random import randint
from time import sleep


class Logger:
    @staticmethod
    def info(msg):
        logging.info(f'[INFO] {msg}')
    @staticmethod
    def error(msg):
        logging.error(f'[ERROR] {msg}')
    @staticmethod
    def debug(msg):
        logging.debug(f'[DEBUG] {msg}')

class ReadConfig:
    yellow_pages_csv = ""
    output_csv = ""
    col_scrap = 0
    contains_header = False
    delay_request = True
    min_delay = 500
    max_delay = 2000
    request_timeout = 10000
    search_tag = ''
    search_attr = ''
    search_attr_value = ''

    def __init__(self):
        configParser = cs.RawConfigParser()
        configParser.read(r'./config.cfg')
        ReadConfig.yellow_pages_csv = configParser.get('settings', 'path')
        ReadConfig.output_csv = configParser.get('settings', 'out_path')
        ReadConfig.col_scrap = int(configParser.get('settings', 'column')) - 1
        ReadConfig.contains_header = bool(configParser.get('settings', 'header'))
        ReadConfig.delay_request = bool(configParser.get('settings', 'delay_request'))
        ReadConfig.min_delay = int(configParser.get('settings', 'min_delay'))
        ReadConfig.max_delay = int(configParser.get('settings', 'max_delay'))
        ReadConfig.request_timeout = int(configParser.get('settings', 'request_timeout'))
        ReadConfig.search_tag = configParser.get('settings', 'search_tag')
        ReadConfig.search_attr = configParser.get('settings', 'search_attr')
        ReadConfig.search_attr_value = configParser.get('settings', 'search_attr_value')

class ReadCsv:
    def __init__(self, file_name):
        self.file_name = file_name

    def isValidFile(self, file_name, file_type):
        if file_type == "csv":
            ret = os.path.isfile(file_name) and file_name.endswith(".csv")
            return ret
        else:
            return False

    def read(self):
        if not self.isValidFile(self.file_name, "csv"):
            return None
        with open(self.file_name, 'r') as file:
            reader = csv.reader(file)
        return reader

    def getColumn(self, column):
        if not self.isValidFile(self.file_name, "csv"):
            return None
        list_urls =[]
        with open(self.file_name, 'r') as file:
            reader = csv.reader(file)
            list_urls = [row[column] for row in reader]
            if ReadConfig.contains_header:
                list_urls.pop(0)
        return list_urls
        
class FetchAndParse:
    def __init__(self, urls):
        self.urls = urls

    def yellowPagesEmail(self, attr):
        email = attr[0].attrs['href']
        # if email does not exist
        if email is None:
            email = ''
        # if email exist
        else:
            if 'mailto' in email:
                email = email.split(':')[1]
        return email

    def schoolPerformanceServiceGovUk(self, attr):
        website = ''
        try:
            website = attr[0].findAll('dd')[12].findAll('a')[0].attrs['href']
        except:
            website = ''
        return website

    def fetch(self, url, currentIndex, totalUrls):
        if ReadConfig.delay_request:
            delay = randint(ReadConfig.min_delay, ReadConfig.max_delay)/float(1000)
            Logger.debug(f'delay: {delay} second(s)')
            sleep(delay)
        Logger.debug(f'sending request [{currentIndex}/{totalUrls}]: {url}')
        isError, html = False, None
        try:
            client = uReq(url, timeout=ReadConfig.request_timeout/1000)
        except HTTPError as error:
            Logger.error('data not retrieved because %s\nURL: <%s>', error, url)
            isError = True
        except URLError as error:
            if isinstance(error.reason, socket.timeout):
                logging.error('socket timed out - URL <%s>', url)
            else:
                logging.error('some other error happened')
            isError = True
        except socket.timeout:
            logging.error('socket timed out - URL <%s>', url)
            isError = True
        except ValueError:
            logging.error('invalid URL <%s>', url)
            isError = True
        else:
            html = client.read()
            client.close()
            Logger.debug(f'received response [{currentIndex}/{totalUrls}]: {url}')

        return isError, html

    def parse(self):
        foundValues = []
        for i, url in enumerate(self.urls):
            isError, html = self.fetch(url, i+1, len(self.urls))

            if isError:
                foundValues.append("")
                continue

            Logger.debug('parsing html response ...')
            page = soup(html, "html.parser")
            foundAttr = page.findAll(ReadConfig.search_tag, {ReadConfig.search_attr:ReadConfig.search_attr_value})
            # if attribute exists
            if len(foundAttr):
                value = self.schoolPerformanceServiceGovUk(foundAttr)
                Logger.debug(f"search value found: {value}")
                foundValues.append(value)
            else:
                Logger.debug('search value not found')
                foundValues.append("")
        return foundValues

class WriteCsv:
    def __init__(self, file_name, mode):
        self.file_name = file_name
        self.mode = mode

    def writeColumn(self, read_file, col_data):
        with open(read_file, "r") as reader_file, \
        open(self.file_name, self.mode) as writer_file:
            reader = csv.reader(reader_file)
            writer = csv.writer(writer_file)
            for i, row in enumerate(reader):
                row.append(col_data[i])
                writer.writerow(row)

if __name__ == "__main__":
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    config = ReadConfig()

    reader = ReadCsv(file_name=config.yellow_pages_csv)
    urls = reader.getColumn(column=config.col_scrap)

    Logger.debug(f"total urls: {len(urls)}")

    parser = FetchAndParse(urls)
    parsedValues = parser.parse()

    Logger.debug(f"total values scrapped: {len(parsedValues)-parsedValues.count('')}")

    if ReadConfig.contains_header:
        parsedValues.insert(0, 'Parsed values')
    
    Logger.debug("dumping output csv ...")
    writer = WriteCsv(file_name=ReadConfig.output_csv, mode="w+")
    writer.writeColumn(read_file=ReadConfig.yellow_pages_csv, col_data=parsedValues)
    Logger.debug("csv dump successful.")
