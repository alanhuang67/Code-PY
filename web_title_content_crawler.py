import pandas as pd
from pymongo import Connection
import pymongo
import logging
from bson.code import Code
import time
import requests
from bs4 import BeautifulSoup
import re

db = Connection().ec_raw
logging.basicConfig(filename='log.log', level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s:%(message)s')
headers = {'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2224.3 Safari/537.36'}

cookies = {'_l_g_':'Ug%3D%3D', '_nk_':'cloga', '_tb_token_': 'ee53876777e17', 'ck1':'', 'cna':'BqAxDAkT7WcCAXf+9QH0+YIm',
            'cookie1': 'W5xgADmZQ0F9Qe4Cecq%2BpDyvPyRQOaV3b4QNBfr9YpU%3D', 'cookie17': 'UU8M9p2yM6Y%3D', 'cookie2': '1ce327120639590194a1e205e9b25dde',
            'isg': '4B29DFC935427B407A72D3FA66453BDB', 'lgc': 'cloga', 'login': 'ture', 'skt':'2b0a4e51a1540ac6', 't':'37f6b3bbb31ff02417e2d1edc09ba0b7',
            'tracknick': 'cloga', 'uc1': 'lltime=1417538830&cookie14=UoW29D1iKrFx6w%3D%3D&existShop=false&cookie16=VT5L2FSpNgq6fDudInPRgavC%2BQ%3D%3D&cookie21=UtASsssme%2BBq&tag=3&cookie15=VT5L2FSpMGV7TQ%3D%3D',
            'uc3': 'nk2=AHYtyo0%3D&id2=UU8M9p2yM6Y%3D&vt3=F8dATkawqrYOR04oWSg%3D&lg2=Vq8l%2BKCLz3%2F65A%3D%3D', 'unb': '27444559'
            }


###插入数据
# ec = pd.read_csv('beetle60.csv', sep='\t', encoding='gb18030', error_bad_lines=False)
ec = pd.read_csv('beele60.csv', sep='\t', error_bad_lines=False, header=None)
ec_raw = ec[[2, 3, 4]]
ec_raw.columns = ['site', 'url', 'pv']
ec_raw['site'] = ec_raw['site'].map(lambda x: x.decode('gb18030'))
ec_raw['sub'] = ec_raw['url'].str.extract('http://(.*?)\.')

for row in ec_raw.iterrows():
    db.beetle.insert(row[1].to_dict())
###

### Groupby site & sub
db.beetle.find_one()

mapper = Code("""
               function () {
                 emit({site:this.site, sub:this.sub}, this.pv);
               }
               """)
reducer = Code("""
                function (key, values) {
                    return Array.sum(values);
                }
                """)

result = db.beetle.map_reduce(mapper, reducer, "site_sub")
###

        
## 按照PV降序更新title

def get_title(url, encoding=None, cookies=[]):
    try:
        r = requests.get(url, headers=headers, cookies=cookies)
        if encoding:
            r.encoding = 'gbk'
        soup = BeautifulSoup(r.text)
        logging.info('url %s download success', url)
        title = soup.title.text 
        logging.info('url %s title is %s ', url, title)
        return title
    except:
        return None


def get_tmall_sub_title(sub):
    url = u'http://' + sub + u'.tmall.com/'
    return get_title(url)

''' Mongo shell
db.site_sub.find({'_id.site': '京东商城'}).sort({'value':-1})
db.site_sub.find({'_id.site': '天猫', 'title': {'$ne': null}}).count()
db.beetle.ensureIndex({"site" : 1})
db.jd_channel.remove({})

'''

## 更新 天猫 Title
for record in db.site_sub.find({'_id.site': u'天猫'}, timeout=False).sort('value',pymongo.DESCENDING).limit(1000):
    if record.get('title', '') in [None, u'天猫 - 上天猫，就够了', u'\xc7\xd7\xa3\xac\xb7\xc3\xce\xca\xca\xdc\xcf\xde\xc1\xcb']:
        title = get_title(record['url'], cookies)
        db.site_sub.update(record,{"$set": {"title": title}})
        time.sleep(5)
    else:
        continue


db.site_sub.find_one({'url': 'http://yifutang.tmall.com/'})
## 测试title抓取

url = 'http://www.paipai.com/promote/2014/4205/index.shtml?ptag=20442.3.1'
r = requests.get(url, headers=headers)
r.encoding = 'gbk'
soup = BeautifulSoup(r.text)
title = soup.title.text 
title.encode('gbk')
get_title(url)

## 更新 京东 title
for record in db.site_sub.find({'_id.site': u'京东商城'}, timeout=False).sort('value',pymongo.DESCENDING).limit(1000):
    title = get_title(record['url'])
    db.site_sub.update(record,{"$set": {"title": title}})
    time.sleep(5)

## 更新 京东 url
for record in db.site_sub.find({'_id.site': u'京东商城'}, timeout=False):
    url = u'http://' + record['_id']['sub'] + u'.jd.com/'
    db.site_sub.update(record,{"$set": {"url": url}})

## 更新 拍拍 url
for record in db.site_sub.find({'_id.site': u'拍拍网'}, timeout=False):
    url = u'http://' + record['_id']['sub'] + u'.paipai.com/'
    db.site_sub.update(record,{"$set": {"url": url}})

## 更新 拍拍 title
for record in db.site_sub.find({'_id.site': u'拍拍网'}, timeout=False).sort('value',pymongo.DESCENDING).limit(1000):
    if record.get('title', ''):
        continue
    title = get_title(record['url'], encoding='gbk')
    db.site_sub.update(record,{"$set": {"title": title}})
    time.sleep(5)

## 更新 大众点评 url
for record in db.site_sub.find({'_id.site': u'大众点评'}, timeout=False):
    url = u'http://' + record['_id']['sub'] + u'.dianping.com/'
    db.site_sub.update(record,{"$set": {"url": url}})


## 更新 大众点评 title
for record in db.site_sub.find({'_id.site': u'大众点评'}, timeout=False).sort('value',pymongo.DESCENDING).limit(1000):
    title = get_title(record['url'], encoding='gbk')
    db.site_sub.update(record,{"$set": {"title": title}})
    time.sleep(5)

## 查询数据
def generate_excel(site):
    items = []
    for record in db.site_sub.find({'_id.site': site}, timeout=False).sort('value',pymongo.DESCENDING).limit(1000):
        item = {}
        item['site'] = record['_id'].get('site', '')
        item['url'] = record.get('url', '')
        item['title'] = record.get('title', '')
        item['pv'] = record.get('value', '')
        items.append(item)
    df = pd.DataFrame(items)
    return df



## 提取title
site = u'京东商城'
df = generate_excel(site)
df['store'] = df['title'].str.extract(u'-(.*?)-天猫Tmall.com')
df.to_excel(site + '_title.xlsx', index=False)
      
## 原始数据
site = u'拍拍网'
items = []

# db.collectionname.find({'files':{'$regex':'^File'}})
items = []
for record in db.beetle.find({'url': {'$regex':r'^http://(mall|channel)\.jd\.com/'}}, timeout=False).sort('pv',pymongo.DESCENDING).limit(1000):
    items.append(record)
df = pd.DataFrame(items)
df = df.drop('_id', 1)
df.to_excel(site + u'_raw.xlsx', index=False)

## 京东商城 channel
items = []
for record in db.beetle.find({'url': {'$regex':r'^http://channel\.jd\.com/'}}, timeout=False).sort('pv',pymongo.DESCENDING):
    items.append(record)
df = pd.DataFrame(items)
df = df.drop('_id', 1)
df['url_m'] = df['url'].str.extract('^(.*?\.html)')
df.to_excel('jd_channel_raw.xlsx', index=False)


## 京东商城 Mall 直接在Raw Data上更新title
for record in db.beetle.find({'url': {'$regex':r'^http://mall\.jd\.com/'}}, timeout=False).sort('pv',pymongo.DESCENDING).limit(1000):
    title = get_title(record['url'])
    db.beetle.update(record,{"$set": {"title": title}})
    time.sleep(5)

df = pd.DataFrame(items)
df = df.drop('_id', 1)
df['url_m'] = df['url'].str.extract('^(.*?\.html)')
df.to_excel('jd_mall_raw.xlsx', index=False)


## channel Title
df = pd.read_excel(u'京东Channel.xlsx', 0)
for row in df.iterrows():
    db.jd_channel.insert(row[1].to_dict())

for record in db.jd_channel.find({}, timeout=False):
    title = get_title(record['URL'])
    db.jd_channel.update(record,{"$set": {"Title": title}})
    time.sleep(5)

items = []
for record in db.jd_channel.find({}, timeout=False):
    items.append(record)
df = pd.DataFrame(items)
df = df.drop('_id', 1)
df.to_excel('jd_channel_title.xlsx', index=False)

## 淘宝网
items = []
for record in db.beetle.find({'site': u'淘宝'}, timeout=False).sort('pv',pymongo.DESCENDING).limit(1000):
    items.append(record)

df = pd.DataFrame(items)
df = df.drop('_id', 1)
df.to_excel('taobao_raw.xlsx', index=False)


## 天猫单品：http://detail.tmall.com/item.htm?id=41575432609
url = 'http://detail.tmall.com/item.htm?spm=0.0.0.0.VOTdAw&id=41234029217&rn=ba8b83b255722950fa18e531ae020164&abbucket=14&gccpm=1408118.102.2.subject-0.39010'
re.findall(r'^http://detail\.tmall\.com/item\.htm.*?id=(\d*)', url)[0]

for record in db.beetle.find({'url': {'$regex':r'^http://detail\.tmall\.com/item\.htm'}}, timeout=False):
    item_id = re.findall(r'^http://detail\.tmall\.com/item\.htm.*?id=(\d*)', record.get('url', ''))
    if item_id:
        logging.info('item_id is %s', str(item_id[0]))
        db.beetle.update(record,{"$set": {"item_id": item_id}})

items = []
    
df = pd.DataFrame(items)

df['id'] = df['url'].str.extract(r'^http://detail\.tmall\.com/item\.htm.*?id=(\d*)')
df = df.drop('_id', 1)
df.to_excel('taobao_raw.xlsx', index=False)

## 淘宝单品：http://item.taobao.com/item.htm?id=15468695610
for record in db.beetle.find({'url': {'$regex':r'^http://item\.taobao\.com/item\.htm'}}, timeout=False):
    item_id = re.findall(r'^http://item\.taobao\.com/item\.htm.*?id=(\d*)', record.get('url', ''))
    if item_id:
        logging.info('taobao item_id is %s', str(item_id[0]))
        db.beetle.update(record,{"$set": {"item_id": item_id}})


## map reduce item_id

mapper = Code("""
               function () {
                 emit({site:this.site, item_id:this.item_id[0]}, this.pv);
               }
               """)
reducer = Code("""
                function (key, values) {
                    return Array.sum(values);
                }
                """)

result = db.beetle.map_reduce(mapper, reducer, "item_id", query={"item_id": {"$exists": True}}).count()

db.beetle.find({"item_id": {"$exists": True}}).count()

## Top items
items = []
for record in db.item_id.find({}, timeout=False).sort('value',pymongo.DESCENDING).limit(1000):
    item = {}
    item['site'] = record['_id'].get('site', '')
    item['item_id'] = record['_id'].get('item_id', '')
    item['pv'] = record.get('value', '')
    items.append(item)
    
df = pd.DataFrame(items)
df.to_excel('top_items.xlsx', index=False)
df['tmall_url'] = df['item_id'].map(lambda x: 'http://detail.tmall.com/item.htm?id=' + x)
df['taobao_url'] = df['item_id'].map(lambda x: 'http://item.taobao.com/item.htm?id=' + x)
