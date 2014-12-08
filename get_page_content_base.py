# -*- coding: utf-8 -*-
import pandas as pd
from pymongo import Connection
import pymongo
import logging
from bson.code import Code
import time
import requests
from bs4 import BeautifulSoup
import re

logging.basicConfig(filename='log.log', level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s:%(message)s')

headers = {'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2224.3 Safari/537.36'}


def get_content_title_keyword(input_file_name, input_file_column, input_file_encode, output_file_name, db_name, db_table):
    db=Connection()[db_name]
    db[db_table].remove({})
    read_url = pd.read_csv(input_file_name, sep=',', error_bad_lines=False)[[input_file_column]]

    # insert into mongodb
    for row in read_url.iterrows():
        db[db_table].insert(row[1].to_dict())

    # insert title into empty title
    i = 0
    for record in db[db_table].find({'title': {"$exists": False}}, timeout=False):
        title, keyword = get_title_keyword(record[input_file_column])
        db[db_table].update(record,{"$set": {"title": title, 'keyword': keyword}})
        i += 1
        logging.info('success %s', i)

        time.sleep(5)

    # read db and optput 
    records = []
    for record in db[db_table].find({}, timeout=False):
        records.append(record)
    # df = pd.DataFrame(records)
    df.to_csv(output_file_name, encoding='utf-8')
    df.to_csv(output_file_name, encoding='gb18030')
    return records

def get_title_keyword(url, encoding=None, cookies=[]):
    r = requests.get(url, headers=headers, cookies=cookies)
    if not encoding:
        r.encoding = 'gbk'
    soup = BeautifulSoup(r.text)
    # logging.info('url %s download success', url)
    try:
        title = soup.title.text 
    except:
        title = ''
    try:
        keyword = soup.keyword.text 
    except:
        keyword = ''
    return title, keyword
    




