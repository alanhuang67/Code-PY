# -*- coding: utf-8 -*-
#import networkx as nx
#cd /Users/cloga/Dropbox/CRM/Owner/Beetle
from urllib2 import URLError, HTTPError
from httplib import BadStatusLine, IncompleteRead
import urllib2
import simplejson as json
import math
from pandas import DataFrame
import pandas as pd
import os
import logging
import jieba
import re
import numpy as np
import collections
import networkx as nx
import urllib
import socket
import multiprocessing as mul
from pymongo import Connection
import os
import requests
from gensim import corpora, models, similarities
from multiprocessing.pool import MaybeEncodingError
import calendar


socket.setdefaulttimeout(300)  # urllib2，urlopen超时默认值
# mongodb相关数据
# db = Connection().weibo

# u = conver to unicode
# stop_word = 分词停用词
stop_words = u'''#、#\n、1、2、3、4、5、6、7、8、9、0、啊、阿、哎、哎呀、哎哟、的、
            吗、唉、俺、俺们、按、按照、吧、吧哒、把、罢了、被、本、本着、比、比方、比如、
            鄙人、彼、彼此、边、别、别的、别说、并、并且、不比、不成、不单、不但、不独、
            不管、不光、不过、不仅、不拘、不论、不怕、不然、不如、不特、不惟、不问、不只、
            朝、朝着、趁、趁着、乘、冲、除、除此之外、除非、除了、此、此间、此外、从、从而、
            打、待、但、但是、当、当着、到、得、的、的话、等、等等、地、第、叮咚、对、对于、
            多、多少、而、而况、而且、而是、而外、而言、而已、尔后、反过来、反过来说、反之、
            非但、非徒、否则、嘎、嘎登、该、赶、个、各、各个、各位、各种、各自、给、根据、
            跟、故、故此、固然、关于、管、归、果然、果真、过、哈、哈哈、呵、和、何、何处、
            何况、何时、嘿、哼、哼唷、呼哧、乎、哗、还是、还有、换句话说、换言之、或、或是、
            或者、极了、及、及其、及至、即、即便、即或、即令、即若、即使、几、几时、己、既、
            既然、既是、继而、加之、假如、假若、假使、鉴于、将、较、较之、叫、接着、结果、借、
            紧接着、进而、尽、尽管、经、经过、就、就是、就是说、据、具体地说、具体说来、开始、
            开外、靠、咳、可、可见、可是、可以、况且、啦、来、来着、离、例如、哩、连、连同、
            两者、了、临、另、另外、另一方面、论、嘛、吗、慢说、漫说、冒、么、每、每当、们、
            莫若、某、某个、某些、拿、哪、哪边、哪儿、哪个、哪里、哪年、哪怕、哪天、哪些、
            哪样、那、那边、那儿、那个、那会儿、那里、那么、那么些、那么样、那时、那些、那样、
            乃、乃至、呢、能、你、你们、您、宁、宁可、宁肯、宁愿、哦、呕、啪达、旁人、呸、凭、
            凭借、其、其次、其二、其他、其它、其一、其余、其中、起、起见、起见、岂但、恰恰相反、
            前后、前者、且、然而、然后、然则、让、人家、任、任何、任凭、如、如此、如果、如何、
            如其、如若、如上所述、若、若非、若是、啥、上下、尚且、设若、设使、甚而、甚么、甚至、
            省得、时候、什么、什么样、使得、是、是的、首先、谁、谁知、顺、顺着、似的、虽、虽然、
            虽说、虽则、随、随着、所、所以、他、他们、他人、它、它们、她、她们、倘、倘或、倘然、
            倘若、倘使、腾、替、通过、同、同时、哇、万一、往、望、为、为何、为了、为什么、为着、
            喂、嗡嗡、我、我们、呜、呜呼、乌乎、无论、无宁、毋宁、嘻、吓、相对而言、像、向、
            向着、嘘、呀、焉、沿、沿着、要、要不、要不然、要不是、要么、要是、也、也罢、也好、
            一、一般、一旦、一方面、一来、一切、一样、一则、依、依照、矣、以、以便、以及、以免、
            以至、以至于、以致、抑或、因、因此、因而、因为、哟、用、由、由此可见、由于、有、
            有的、有关、有些、又、于、于是、于是乎、与、与此同时、与否、与其、越是、云云、哉、
            再说、再者、在、在下、咱、咱们、则、怎、怎么、怎么办、怎么样、怎样、咋、照、照着、
            者、这、这边、这儿、这个、这会儿、这就是说、这里、这么、这么点儿、这么些、这么样、
            这时、这些、这样、正如、吱、之、之类、之所以、之一、只是、只限、只要、只有、至、
            至于、诸位、着、着呢、自、自从、自个儿、自各儿、自己、自家、自身、综上所述、
            总的来看、总的来说、总的说来、总而言之、总之、纵、纵令、纵然、纵使、遵照、作为、
            兮、呃、呗、咚、咦、喏、啐、喔唷、嗬、嗯、嗳、\n、\n\n'''.split(u'、')

logging.basicConfig(filename='weibo_data.log', level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s:%(message)s')

token = '2.00Hk5I5B3mz1gE8da971743b1lm93B'


# from stakoverflow http://stackoverflow.com/questions/
# 1119722/base-62-conversion-in-python
ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


def base62_encode(num, alphabet=ALPHABET):
    """Encode a number in Base X
    `num`: The number to encode
    `alphabet`: The alphabet to use for encoding
    """
    if num == 0:
        return alphabet[0]
    arr = []
    base = len(alphabet)
    while num:
        rem = num % base
        num = num // base
        arr.append(alphabet[rem])
    arr.reverse()
    return ''.join(arr)


def base62_decode(string, alphabet=ALPHABET):
    # from stackoverflow
    """Decode a Base X encoded string into the number
    Arguments:
    - `string`: The encoded string
    - `alphabet`: The alphabet to use for encoding
    """
    base = len(alphabet)
    strlen = len(string)
    num = 0

    idx = 0
    for char in string:
        power = (strlen - (idx + 1))
        num += alphabet.index(char) * (base ** power)
        idx += 1
    return num


def url_to_mid(url):
    # from http://qinxuye.me/article/mid-and-url-in-sina-weibo/
    '''
    >>> url_to_mid('z0JH2lOMb')
    3501756485200075L
    >>> url_to_mid('z0Ijpwgk7')
    3501703397689247L
    >>> url_to_mid('z0IgABdSn')
    3501701648871479L
    >>> url_to_mid('z08AUBmUe')
    3500330408906190L
    >>> url_to_mid('z06qL6b28')
    3500247231472384L
    >>> url_to_mid('yCtxn8IXR')
    3491700092079471L
    >>> url_to_mid('yAt1n2xRa')
    3486913690606804L
    '''
    url = str(url)[::-1]
    size = len(url) / 4 if len(url) % 4 == 0 else len(url) / 4 + 1
    result = []
    for i in range(size):
        s = url[i * 4: (i + 1) * 4][::-1]
        s = str(base62_decode(str(s)))
        s_len = len(s)
        if i < size - 1 and s_len < 7:
            s = (7 - s_len) * '0' + s
        result.append(s)
    result.reverse()
    return int(''.join(result))


def mid_to_url(midint):
    # from http://qinxuye.me/article/mid-and-url-in-sina-weibo/
    '''
    >>> mid_to_url(3501756485200075)
    'z0JH2lOMb'
    >>> mid_to_url(3501703397689247)
    'z0Ijpwgk7'
    >>> mid_to_url(3501701648871479)
    'z0IgABdSn'
    >>> mid_to_url(3500330408906190)
    'z08AUBmUe'
    >>> mid_to_url(3500247231472384)
    'z06qL6b28'
    >>> mid_to_url(3491700092079471)
    'yCtxn8IXR'
    >>> mid_to_url(3486913690606804)
    'yAt1n2xRa'
    '''
    midint = str(midint)[::-1]
    size = len(midint) / 7 if len(midint) % 7 == 0 else len(midint) / 7 + 1
    result = []
    for i in range(size):
        s = midint[i * 7: (i + 1) * 7][::-1]
        s = base62_encode(int(s))
        s_len = len(s)
        if i < size - 1 and len(s) < 4:
            s = '0' * (4 - s_len) + s
        result.append(s)
    result.reverse()
    return ''.join(result)


def get_repost_timeline(id, count=200, page=1, **keys):
    query_args = {'id': id, 'count': count, 'page': page,
                  'access_token': token}
    query_args.update(keys)
    url = 'https://api.weibo.com/2/statuses/repost_timeline.json?'
    encoded_args = urllib.urlencode(query_args)
    content = urllib2.urlopen(url + encoded_args).read()
    # 改为用urllib避免BadStatusLine
    # content = urllib.urlopen(url + encoded_args).read()
    return json.loads(content)


def get_followers_ids(uid=0, screen_name='', count=5000):
    if uid:
        url = 'https://api.weibo.com/2/friendships/followers/ids.json?count={:s}&access_token={:s}&uid={:s}'.format(
            str(count), str(token), str(uid))
    else:
        url = 'https://api.weibo.com/2/friendships/followers/ids.json?count={:s}&access_token={:s}&screen_name={:s}'.format(str(count), str(token),
                                                                                                                            screen_name.encode('utf-8', 'ignore'))
    # print url
    content = urllib2.urlopen(url).read()
    return json.loads(content)


def get_followers(uid=0, screen_name='', count=200, page=1, trim_status=1):
    ''' http://open.weibo.com/wiki/2/friendships/followers
    粉丝列表
    '''
    if uid:
        url = 'https://api.weibo.com/2/friendships/followers.json?count=' + \
            str(count) + '&access_token=' + token + '&uid=' + str(uid) + \
            '&page=' + str(page) + '&trim_status=' + str(trim_status)
    else:
        url = 'https://api.weibo.com/2/friendships/followers.json?count=' + \
            str(count) + '&access_token=' + token + '&screen_name=' + screen_name.encode(
                'utf-8', 'ignore') + '&page=' + str(page) + '&trim_status=' + str(trim_status)
    # print url
    content = urllib2.urlopen(url).read()
    return json.loads(content)


def get_friends_ids(uid=0, screen_name='', count=5000, page=1, trim_status=1):
    ''' http://open.weibo.com/wiki/2/friendships/friends/ids
    关注列表
    '''
    if uid:
        url = 'https://api.weibo.com/2/friendships/friends/ids.json?count=' + \
            str(count) + '&access_token=' + token + '&uid=' + str(uid) + \
            '&page=' + str(page) + '&trim_status=' + str(trim_status)
    else:
        url = 'https://api.weibo.com/2/friendships/friends/ids.json?count=' + \
            str(count) + '&access_token=' + token + '&screen_name=' + screen_name.encode(
                'utf-8', 'ignore') + '&page=' + str(page) + '&trim_status=' + str(trim_status)
    # print url
    content = urllib2.urlopen(url).read()
    return json.loads(content)


def get_friends(uid=0, screen_name='', count=200, page=1, trim_status=1):
    # http://open.weibo.com/wiki/2/friendships/friends
    if uid:
        url = 'https://api.weibo.com/2/friendships/friends.json?count=' + \
            str(count) + '&access_token=' + token + '&uid=' + str(uid) + \
            '&page=' + str(page) + '&trim_status=' + str(trim_status)
    else:
        url = 'https://api.weibo.com/2/friendships/friends.json?count=' + \
            str(count) + '&access_token=' + token + '&screen_name=' + screen_name.encode(
                'utf-8', 'ignore') + '&page=' + str(page) + '&trim_status=' + str(trim_status)
    # print url
    content = urllib2.urlopen(url).read()
    return json.loads(content)


def get_comment_show(id, count=200, page=1, method='requests', **keys):
    # http://open.weibo.com/wiki/2/comments/show
    query_args = {'id': id, 'count': count, 'page': page,
                  'access_token': token}
    query_args.update(keys)
    if method == 'requests':
        url = 'https://api.weibo.com/2/comments/show.json'
        content = requests.get(url, params=query_args)
        return content.json()
    elif method == 'urllib2':
        url = 'https://api.weibo.com/2/comments/show.json?'
        encoded_args = urllib.urlencode(query_args)
        content = urllib2.urlopen(url + encoded_args).read()
        return json.loads(content)
    else:
        raise Exception('available of method are only requests and urllib2!')


def get_followers_info(uid=0, screen_name='', store_in_mongo=0, csv=0, file_name='temp', inc_type=0, pool_num=None):
    # uid = 1272766635
    if uid:
        f = get_followers_ids(uid=uid)
    if screen_name:
        f = get_followers_ids(screen_name=screen_name)
    followers = get_users_info(f['ids'], store_in_mongo=store_in_mongo,
                               csv=csv, file_name=file_name, inc_type=inc_type, pool_num=pool_num)
    return followers


def get_friends_info(uid=0, screen_name='', store_in_mongo=0):
    # uid = 1272766635
    if uid:
        f = get_friends_ids(uid=uid)
    if screen_name:
        f = get_friends_ids(screen_name=screen_name)
    total = f['total_number']
    logging.info('%s followers number is %s ', str(uid), str(total))
    page_number = 25 if total > 5000 else total / 200 + 1
    ids = f['ids']
    if store_in_mongo:
        # 把关注关系插入的Mongodb
        db.weibo_friends.update(
            {'uid': uid}, {'uid': uid, 'followers_ids': ids}, {'upsert': True})
    friends = []
    friends_ids_de = []
    for i in range(page_number):
        friends += get_friends(
            uid=uid, screen_name=screen_name, count=200, page=i + 1, trim_status=1)['users']
    friends_ids = [f['id'] for f in friends]
    friends_ids_de = [i for i in ids if i not in friends_ids]
    logging.info('%s followers %s del', str(uid), str(len(friends_ids_de)))
    # for f in friends_ids_de:
    #     friends.append(get_user_show(uid = f))
    # 删除的用户不需要抓取
    if store_in_mongo:
        for f in friends:
            # 把粉丝插入的Mongodb
            db.weibo_user.update({'id': f['id']}, f, {'upsert': True})
    return friends


def get_statuses_show(post_id):
    url = 'https://api.weibo.com/2/statuses/show.json?access_token=' + \
        token + '&id=' + str(post_id)
    # https://api.weibo.com/2/statuses/show.json?access_token=2.00Hk5I5B3mz1gEdaf1f0cb3bCXBvsB&id=2736617043
    # print url
    content = urllib2.urlopen(url).read()
    return json.loads(content)


def get_user_show(uid=0, screen_name=''):
    # http://open.weibo.com/wiki/2/users/show
    url0 = 'https://api.weibo.com/2/users/show.json?' + 'access_token=' + token
    if uid:
        url = url0 + '&uid=' + str(uid)
    if screen_name:
        url = url0 + '&screen_name=' + \
            str(screen_name.encode('utf-8', 'ignore'))
    try:
        content = urllib2.urlopen(url).read()
    except HTTPError as i:
        # 存在用户不存在的情况，打一个删除标记
        logging.info('%s error occured %s', str(HTTPError), str(i))
        logging.info('%s deleted %s', str(uid), str(i))
        return {'id': uid, 'is_deleted': True}
    return json.loads(content)


def get_user_tag(uid):
    '''http://open.weibo.com/wiki/2/tags
    '''
    url = 'https://api.weibo.com/2/tags.json?access_token=' + \
        token + '&uid=' + str(uid)
    # print url
    content = urllib2.urlopen(url).read()
    return json.loads(content)


def get_user_tags_batch(uids):
    '''https://api.weibo.com/2/tags/tags_batch.json
    '''
    url = 'https://api.weibo.com/2/tags/tags_batch.json?access_token=' + \
        token + '&uid=' + str(uid)
    content = urllib2.urlopen(url).read()
    return json.loads(content)


def get_user_timeline(uid=0, screen_name='', count=100, page=1):
    url0 = 'https://api.weibo.com/2/statuses/user_timeline.json?access_token={:s}&count={:s}&page={:s}'.format(
        str(token), str(count), str(page))
    if uid:
        url = url0 + '&uid={:s}'.format(str(uid))
        # print url
        content = urllib2.urlopen(url).read()
        return json.loads(content)
    if screen_name:
        url = url0 + '&screen_name=' + \
            str(screen_name.encode('utf-8', 'ignore'))
        print url
        content = urllib2.urlopen(url).read()
        return json.loads(content)

def get_timeline_batch(uids='', screen_names='', count=100, page=1):
    #http://open.weibo.com/wiki/2/statuses/timeline_batch
    url0 = 'https://api.weibo.com/2/statuses/timeline_batch.json?access_token={:s}&count={:s}&page={:s}'.format(
        str(token), str(count), str(page))
    if uids:
        url = url0 + '&uids={:s}'.format(str(uids))
        # print url
        content = urllib2.urlopen(url).read()
        return json.loads(content)
    if screen_names:
        url = url0 + '&screen_names=' + \
            str(screen_names.encode('utf-8', 'ignore'))
        print url
        content = urllib2.urlopen(url).read()
        return json.loads(content)





def get_user_timeline_ids(uid=0, screen_name='', count=100, page=1):
    '''http://open.weibo.com/wiki/2/statuses/user_timeline/ids
    '''
    url0 = 'https://api.weibo.com/2/statuses/user_timeline/ids.json?access_token={:s}&count={:s}&page={:s}'.format(
        str(token), str(count), str(page))
    if uid:
        url = url0 + '&uid={:s}'.format(str(uid))
        # print url
        content = urllib2.urlopen(url).read()
        return json.loads(content)
    if screen_name:
        url = url0 + '&screen_name=' + \
            str(screen_name.encode('utf-8', 'ignore'))
        # print url
        content = urllib2.urlopen(url).read()
        return json.loads(content)


def get_posts(screen_name='', uid=0, csv=0, page=20, store_in_mongo=0, pool_num=None):
    '''获得指定作者的2000条微博，输入微博名称和uid都可以
    返回dict of list用于其他处理
    输出CSV时才需要转成DataFrame
    中文为utf-8 str
    不能携带时区信息否则会报错
    '''
    posts = []
    results = []
    contents = []
    if not screen_name:
        screen_name = get_user_show(uid=uid)[u'screen_name']
        # 确实有些人的screen_name为空
    total_number = get_timeline_batch(uid, screen_name)['total_number']
    page_number = int(math.ceil(total_number / 100.0))  # 每一页的微博上限是100，不是200
    logging.info('%s total_number: %s page_number: %s', str(
        screen_name.encode('utf-8', 'ignore')), str(total_number), str(page_number))
    if page_number > 20:
        page_number = 20  # 上限提供2000条
    if page < page_number:
        page_number = page
    # to do:增加一个启用多进程抓取
    # 多进程分支
    if pool_num > 0:
        pool = mul.Pool(pool_num)
        for i in range(0, page_number):
            logging.info('crawling %s %s / %s', str(
                screen_name.encode('utf-8', 'ignore')), str(i + 1), str(page_number))
            results.append(
                pool.apply_async(get_timeline_batch, args=(uid, screen_name), kwds=dict(page=i + 1, count=100)))
        # contents = reduce(lambda x,y : x.get()['statuses'] + y.get()['statuses'], contents_list)
        pool.close()
        pool.join()
        for result in results:
            try:
                contents += result.get()['statuses']
            except URLError as e:
                logging.info('error occured %s', str(e))
    # 串行分支
    else:
        for i in range(0, page_number):
            logging.info('crawling %s %s / %s', str(
                screen_name.encode('utf-8', 'ignore')), str(i + 1), str(page_number))
            contents += get_timeline_batch(
                uid, screen_name, page=i + 1, count=100)['statuses']
    for c in contents:
        if store_in_mongo:
            # 插入到mongodb
            db.weibos.update({'mid': c['mid']}, c, {'upsert': True})
        # print c['id']
        # post = {k:v for k,v in c if type(v) not in [dict,list]  }
        post = dict(
            post_content=c['text'],
            #post_created_at=time.strftime('%Y/%m/%d %H:%M:%S',time.strptime(c['created_at'].encode('gbk','ignore'),'%a %b %d %H:%M:%S +0800 %Y')),
            #post_created_at=time.mktime(time.strptime(c['created_at'].encode('gbk','ignore'),'%a %b %d %H:%M:%S +0800 %Y')),
            post_created_at=pd.to_datetime(
                c['created_at']),
            post_mid=str(c['mid']),
            post_source=c['source'],
            post_user_screen_name=c['user']['screen_name'],
            post_user_id=c['user']['id'],
            reposts=c['reposts_count'],
            comments=c['comments_count'],
            post_url='http://api.t.sina.com.cn/' +
            str(c['user']['id']) + '/statuses/' + str(
            c['mid']),
        )
        retweeted = c.get('retweeted_status', {})
        if retweeted:
            post['retweeted_mid'] = str(retweeted['mid'])
            post['retweeted_or_not'] = True
            post['retweeted_content'] = retweeted['text']
            topic = re.compile(ur'#[^#]+?#')
            topics = re.findall(topic, retweeted['text'])
            if topics:
                post['topic'] = True
            post['retweeted_created_at'] = pd.to_datetime(
                retweeted['created_at'])
            if not retweeted.get('deleted', ''):
                post['retweeted_url'] = 'http://api.t.sina.com.cn/' + str(
                    retweeted['user']['id']) + '/statuses/' + '/' + str(retweeted['mid'])
                post['retweeted_user_screen_name'] = retweeted[
                    'user']['screen_name']
        posts.append(post)
    if csv:
        df = pd.DataFrame(posts)
        df['post_mid'] = df['post_mid'].apply(lambda x: '\'' + str(x))
        df['retweeted_mid'] = df[
            'retweeted_mid'].apply(lambda x: '\'' + str(x))
        # 处理mid长于15的问题
        df.to_excel(screen_name.encode('utf-8', 'ignore')
                    + '_post_utf8.xlsx', index=False)
        print 'generated', screen_name, 'posts csv completed!'
    logging.info('%s : %s posts crawled!', str(
        screen_name.encode('utf-8', 'ignore')), str(uid))
    return posts

def collect_users_post_data(names, raw=0, file_name='Temp', page=1, store_in_mongo=0):
    raw_data_list = []
    for n in names:
        raw_data_list += get_posts(
            screen_name=n, page=page, store_in_mongo=store_in_mongo)
        print n, 'completed'
    raw_data = pd.DataFrame(raw_data_list)
    if raw:
        raw_data.to_excel(file_name + '_raw', index=False)
    groups = raw_data.groupby('post_user_screen_name')
    data = {}
    data['posts'] = groups['post_user_id'].count()
    data['reposts_total'] = groups['reposts'].sum()
    data['comments_total'] = groups['comments'].sum()
    data['reposts_avg'] = data['reposts_total'] / data['posts']
    data['comments_avg'] = data['comments_total'] / data['posts']
    df = pd.DataFrame(data)
    df.reindex_axis(
        ['posts', 'reposts_total', 'comments_total', 'reposts_avg', 'comments_avg'], axis=1, copy=False)
    df.reset_index(inplace=True)
    df.to_excel(file_name + '.xlsx', index=False)


def get_posts_ids(screen_name='', uid=0, csv=0, page=20, store_in_mongo=0, pool_num=None):
    '''获得指定作者的所有微博id，似乎不受2000条的限制
    输入微博名称和uid都可以
    返回dict用于其他处理
    输出CSV时才需要转成DataFrame
    中文为utf-8 str
    '''
    posts = []
    results = []
    contents = []
    if not screen_name:
        screen_name = get_user_show(uid=uid)[u'screen_name']
        # 确实有些人的screen_name为空
    total_number = get_user_timeline(uid, screen_name)['total_number']
    page_number = int(math.ceil(total_number / 100.0))  # 每一页的微博上限是100，不是200
    logging.info('%s total_number: %s page_number: %s', str(
        screen_name.encode('utf-8', 'ignore')), str(total_number), str(page_number))
    # if page_number > 20:
    # page_number = 20  # 上限提供2000条
    if page < page_number:
        page_number = page
    # to do:增加一个启用多进程抓取
    # 多进程分支
    if pool_num > 0:
        pool = mul.Pool(pool_num)
        for i in range(0, page_number):
            logging.info('crawling %s %s / %s', str(
                screen_name.encode('utf-8', 'ignore')), str(i + 1), str(page_number))
            results.append(
                pool.apply_async(get_user_timeline, args=(uid, screen_name), kwds=dict(page=i + 1, count=100)))
        # contents = reduce(lambda x,y : x.get()['statuses'] + y.get()['statuses'], contents_list)
        pool.close()
        pool.join()
        for result in results:
            try:
                contents += result.get()['statuses']
            except URLError as e:
                logging.info('error occured %s', str(e))
    # 串行分支
    else:
        for i in range(0, page_number):
            logging.info('crawling %s %s / %s', str(
                screen_name.encode('utf-8', 'ignore')), str(i + 1), str(page_number))
            contents += get_user_timeline(
                uid, screen_name, page=i + 1, count=100)['statuses']
    for c in contents:
        if store_in_mongo:
            # 插入到mongodb
            db.weibos.update({'mid': c['mid']}, c, {'upsert': True})
        # print c['id']
        # post = {k:v for k,v in c if type(v) not in [dict,list]  }
        post = dict(
            post_content=c['text'],
            #post_created_at=time.strftime('%Y/%m/%d %H:%M:%S',time.strptime(c['created_at'].encode('gbk','ignore'),'%a %b %d %H:%M:%S +0800 %Y')),
            #post_created_at=time.mktime(time.strptime(c['created_at'].encode('gbk','ignore'),'%a %b %d %H:%M:%S +0800 %Y')),
            post_created_at=pd.to_datetime(
                c['created_at'], utc=True).tz_convert('Asia/Shanghai'),
            post_mid=str(c['mid']),
            post_source=c['source'],
            post_user_screen_name=c['user']['screen_name'],
            post_user_id=c['user']['id'],
            reposts=c['reposts_count'],
            comments=c['comments_count'],
            post_url='http://api.t.sina.com.cn/' +
            str(c['user']['id']) + '/statuses/' + str(
            c['mid']),
        )
        retweeted = c.get('retweeted_status', {})
        if retweeted:
            post['retweeted_mid'] = str(retweeted['mid'])
            post['retweeted_or_not'] = True
            post['retweeted_content'] = retweeted['text']
            topic = re.compile(ur'#[^#]+?#')
            topics = re.findall(topic, retweeted['text'])
            if topics:
                post['topic'] = True
            post['retweeted_created_at'] = pd.to_datetime(
                retweeted['created_at'], utc=True).tz_convert('Asia/Shanghai')
            if not retweeted.get('deleted', ''):
                post['retweeted_url'] = 'http://api.t.sina.com.cn/' + str(
                    retweeted['user']['id']) + '/statuses/' + '/' + str(retweeted['mid'])
                post['retweeted_user_screen_name'] = retweeted[
                    'user']['screen_name']
        posts.append(post)
    if csv:
        df = DataFrame(posts)
        df['post_mid'] = df['post_mid'].apply(lambda x: '\'' + str(x))
        df['retweeted_mid'] = df[
            'retweeted_mid'].apply(lambda x: '\'' + str(x))
        # 处理mid长于15的问题
        df.to_excel(screen_name.encode('utf-8', 'ignore')
                    + '_post_utf8.xlsx', index=False)
        print 'generated', screen_name, 'posts csv completed!'
    logging.info('%s : %s posts crawled!', str(
        screen_name.encode('utf-8', 'ignore')), str(uid))
    return posts


def get_repost_edges(post_id, csv=0, store_in_mongo=1, max_id=0, pool_num=None):
    '''
    获得转发路径
    使用max_id和since_id获得全量转发
    这里没用多进程因为全量转发需要依赖maxid
    '''
    weibo_del = []
    edges = []
    reposts = []
    post_id = int(post_id)
    total_number = get_repost_timeline(id=post_id, count=200)['total_number']
    logging.info(
        'get_repost_edges start : %s total number is %s , pid is %s', post_id, str(total_number), str(os.getpid()))
    # flag参数有问题，翻页获得所有转发,有可能缺失
    page_number = total_number / 200 + 1  # 保留两位小数
    for i in range(page_number / 10 + 1):
        # 每10页为一组，更新max_id
        for j in range(1, 11):
            if i * 10 + j > page_number:
                break
            try:
                page_repost = get_repost_timeline(
                    id=post_id, count=200, page=j, max_id=max_id)['reposts']
            except URLError as e:
                # 存在微博不存在的情况
                logging.info('%s error:', str(e))
                continue
            logging.info(
                '----------------%s page %s / %s, pid is %s--------------------', str(post_id), str(i * 10 + j), str(page_number), str(os.getpid()))
            if len(page_repost) > 0:
                reposts += page_repost
                # 存在返回是一个list的情况
            logging.info(
                '----------------%s %s share crawled, pid is %s--------------------', str(post_id), str(len(reposts)), str(os.getpid()))
        max_id = reposts[-1]['mid']
    screen_names = {int(r['mid']): r['user']['screen_name']
                    for r in reposts}  # pid是int，而mid是str
    screen_names[post_id] = get_statuses_show(post_id)[
        'user']['screen_name'].encode('utf-8', 'ignore')  # 把作者的名字丢进去
    logging.info('%s Total_Number Crawled: %s', post_id, len(reposts))
    if len(reposts) == 0:
        return []
    for repost in reposts:
        if store_in_mongo:
            db.weibos.insert(repost)  # 插入到mongodb
    # print repost['id']
        pid = repost.get('pid', repost['retweeted_status']['id'])
        try:
            if pid in screen_names.keys():
                pid_screen_name = screen_names[pid]
            else:
                pid_post = get_statuses_show(pid)
                reposts.append(pid_post)  # 未知原因没有找到被转发的帖子，补充到帖子列表中
                pid_screen_name = str(
                    pid_post['user']['screen_name'].encode('utf-8', 'ignore'))
                screen_names[pid] = pid_screen_name  # 把未抓到的转发写进screen_names中
                logging.info(
                    'add reposts : %s total number %s, pid is %s ', pid, str(len(reposts)), str(os.getpid()))
        except URLError as i:
            # 存在微博不存在的情况
            logging.info('%s error: %s', str(pid), i)
            weibo_mid = str(pid)
            screen_names[pid] = str(pid)
            weibo_del.append(pid)
            weibo_d = {'weibo_mid': weibo_mid, 'poster': weibo_mid}
            edges.append(weibo_d)
            continue
        if repost['retweeted_status'].get('deleted', False):
            # 存在转发被删除的情况
            edge = dict(
                weibo_mid=repost['id'],
                weibo_url='',
                original_mid=str(repost['retweeted_status']['id']),
                pid=str(pid),
                original_poster_id='',
                original_poster='',
                original_weibo_url='',
                # original_content=repost['retweeted_status']['text'].encode('utf-8','ignore'),
                poster=repost['user'][
                    'screen_name'].encode('utf-8', 'ignore'),
                poster_fans=repost['user']['followers_count'],
                poster_id=repost['user']['id'],
                content=repost['text'].encode('utf-8', 'ignore'),
                #created_at=time.strftime('%Y-%m-%d %H:%M:%S',time.strptime(repost['created_at'].encode('gbk','ignore'),'%a %b %d %H:%M:%S +0800 %Y')),
                created_at=pd.to_datetime(repost['created_at'].encode(
                    'gbk', 'ignore'), utc=True).tz_convert('Asia/Shanghai'),
                repost=repost['reposts_count'],
                comment=repost['comments_count'],
                pid_screen_name=pid_screen_name
            )
        else:
            edge = dict(
                weibo_mid=repost['id'],
                weibo_url='http://api.t.sina.com.cn/' +
                str(repost['user']['id']) + '/statuses/' + str(
                    repost['id']),
                original_mid=str(repost['retweeted_status']['id']),
                pid=str(pid),
                original_poster_id=repost[
                    'retweeted_status']['user']['id'],
                original_poster=repost['retweeted_status'][
                    'user']['screen_name'].encode('utf-8', 'ignore'),
                original_weibo_url='http://api.t.sina.com.cn/' + str(repost['retweeted_status']['user']['id']) +
                '/statuses/' +
                str(repost[
                'retweeted_status']['id']),
                # original_content=repost['retweeted_status']['text'].encode('utf-8','ignore'),
                poster=repost['user'][
                'screen_name'].encode('utf-8', 'ignore'),
                poster_fans=repost['user']['followers_count'],
                poster_id=repost['user']['id'],
                content=repost['text'].encode('utf-8', 'ignore'),
                #                    created_at=time.strftime('%Y-%m-%d %H:%M:%S',time.strptime(repost['created_at'].encode('gbk','ignore'),'%a %b %d %H:%M:%S +0800 %Y')),
                created_at=pd.to_datetime(repost['created_at'].encode(
                'gbk', 'ignore'), utc=True).tz_convert('Asia/Shanghai'),
                repost=repost['reposts_count'],
                comment=repost['comments_count'],
                pid_screen_name=pid_screen_name
            )
        edges.append(edge)
    logging.info('%s weibo crawled %s', str(post_id), str(len(edges)))
    logging.info('weibo deleted: %s', str(len(weibo_del)))
    if csv:
        df = pd.DataFrame(edges)
        df['weibo_mid'] = df['weibo_mid'].apply(lambda x: '\'' + str(x))
        df['original_mid'] = df['original_mid'].apply(lambda x: '\'' + str(x))
        df['pid'] = df['pid'].apply(lambda x: '\'' + str(x))
        df.to_excel(str(post_id) + '_edges_utf8.xlsx', index=False)
    # 每个转发为一个Dict
    return edges


def get_post_comments(post_id, csv=0, store_in_mongo=0, max_id=0):
    '''
    获得帖子的comment
    似乎用max_id也无法获得全量转发
    这里没用多进程因为全量转发需要依赖maxid
    '''
    comments = []
    comments_df = []
    post_id = int(post_id)
    total_number = get_comment_show(id=post_id, count=200)['total_number']
    logging.info(
        'get_post_comments start : %s total number is %s ', post_id, str(total_number))
    # flag参数有问题，翻页获得所有转发,有可能缺失, 只能抓取近2000条
    page_number = total_number / 200 + \
        1 if total_number / 200 < 10 else 10  # 保留两位小数
    for i in range(page_number):
        try:
            page_comments = get_comment_show(
                id=post_id, count=200, page=i + 1)['comments']
        except URLError as i:
            # 存在微博不存在的情况
            logging.info('%s error: %s', str(pid), i)
            continue
        logging.info(
            '----------------%s comments page %s / %s --------------------', str(post_id), str(i + 1), str(page_number))
        if len(page_comments) > 0:
            comments += page_comments
            # 存在返回是一个空list的情况
        logging.info(
            '----------------%s %s comments crawled  --------------------', str(post_id), str(len(comments)))
    logging.info(
        '%s Comments Total_Number Crawled: %s', post_id, len(comments))
    if len(comments) == 0:
        # 存在抓取不到comments的情况
        return []
    for comment in comments:
        if store_in_mongo:
            db.comments.insert(comment)  # 插入到mongodb
        cm = dict(
            text=comment['text'],
            created_at=pd.to_datetime(
                comment['created_at'], utc=True).tz_convert('Asia/Shanghai'),
            mid=comment['mid'],
            post_mid=comment['status']['mid'],
            source=comment['source'],
            screen_name=comment['user']['screen_name'],
            uid=comment['user']['id'],
            original_poster_name=comment['status']['user']['screen_name'],
            original_poster_uid=comment['status']['user']['id']
        )
        comments_df.append(cm)
    logging.info('%s comments crawled %s', str(post_id), str(len(comments)))
    if csv:
        df = pd.DataFrame(comments_df)
        # 处理mid长度为15
        # to do : user信息没输出
        df['mid'] = df['mid'].apply(lambda x: '\'' + str(x))
        df.to_excel(str(post_id) + '_comments_utf8.xlsx', index=False)
    # 返回的是个Dict of List
    return comments_df


def get_users_comments(screen_names='', csv=0, begin_time='', end_time='', pre='temp', store_in_mongo=0, pool_num=None):
    '''获得指定作者在一段时间内的所有转发
    返回dict用于其他处理
    输出CSV时才需要转成DataFrame
    中文为utf-8 str
    '''
    comments = []
    results = []
    posts = get_accounts_posts(
        screen_names, csv=csv, begin_time=begin_time, end_time=end_time, pre=pre, pool_num=pool_num)
    posts = posts.ix[posts['comments'] > 0]
    comments_num = sum(posts['comments'])
    # to do 丑陋的写法
    mids_screen_names = posts.set_index('post_mid')[
        [u'post_user_screen_name']].to_dict()['post_user_screen_name']
    # to do 丑陋的写法
    mids = mids_screen_names.keys()
    pool = mul.Pool(pool_num)
    leng = len(mids)
    logging.info(
        '%s weibos %s comments need to crawl comments', str(leng), str(comments_num))
    for n, i in enumerate(mids):
        logging.info(
            'crawling %s comments %s / %s', str(i), str(n + 1), str(leng))
        results.append(
            pool.apply_async(get_post_comments, kwds=dict(post_id=i, store_in_mongo=store_in_mongo)))
    pool.close()
    pool.join()
    for result in results:
        # print results.index(result)
        comments += result.get()
    comments_df = []
    for c in comments:
        # print comments.index(c)
        cm = dict(
            text=c['text'],
            created_at=pd.to_datetime(
                c['created_at'], utc=True).tz_convert('Asia/Shanghai'),
            mid=c['mid'],
            post_mid=c['status']['mid'],
            source=c['source'],
            screen_name=c['user']['screen_name'],
            uid=c['user']['id'],
            original_mid=c['status']['mid'],
            original_poster_name=c['status']['user']['screen_name'],
            original_poster_uid=c['status']['user']['id']
        )
        comments_df.append(cm)
    if csv:
        df = DataFrame(comments_df)
        # 处理mid长于15的问题
        df['mid'] = df['mid'].apply(lambda x: '\'' + str(x))
        df['original_mid'] = df['original_mid'].apply(lambda x: '\'' + str(x))
        df['post_mid'] = df['post_mid'].apply(lambda x: '\'' + str(x))
        # 处理mid长于15的问题
        df.to_excel(pre + '_comments_utf8.xlsx', index=False)
        print 'generated comments csv completed!'
    logging.info('%s : %s comments crawled!',
                 screen_names.encode('utf-8', 'ignore'), str(len(comments)))
    return comments_df


def insert_user(screen_name):
    user_info = get_user_show(screen_name=screen_name)
    followers_ids = get_followers_ids(screen_name=screen_name)['ids']
    user_info['followers_ids'] = followers_ids
    # print user_info
    db.weibo_user.update({'screen_name': screen_name}, user_info, True)
    # get_followers(followers_ids)


def store_users_in_mongo(followers_ids):
    for i, v in enumerate(followers_ids):
        try:
            user_info = get_user_show(uid=v)
            logging.info('%s / %s follower is %s', str(
                i + 1), str(len(followers_ids)), str(v))
            db.weibo_user.update({'id': v}, user_info, True)
        except:
            failed_user = {'id': v, 'failed': 1}
            db.weibo_user.update({'id': v}, failed_user, True)
            logging.info('%s / %s follower is %s failed',
                         str(i + 1), str(len(followers_ids)), str(v))


def main():
    f1 = db.weibo_user.find(
        {'crawled': {"$ne": 1}}, {'id': 1, 'screen_name': 1})
    logging.info('%s user need crawled weibos', f1.count())
    for i in f1:
        weibos = get_user_timeline(uid=i['id'])['statuses']
        if len(weibos) > 0:
            db.weibos.insert(weibos)
        db.weibo_user.update(i, {"$set": {"crawled": 1}})
        logging.info('user %s crawled', i['screen_name'])


def update_user_tag(user_info):
    tags = get_user_tag(user_info['id'])
    user_info['tags'] = tags
    return user_info


def generate_term_cloud(prefix, term_frequency):
    page_template = '''<html>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
            <script src="http://ajax.googleapis.com/ajax/libs/jquery/1.10.2/jquery.min.js"></script>
            <script src="http://timdream.org/wordcloud2.js/src/wordcloud2.js"></script>
            <div class="span12" id="canvas-container">
                            <canvas width="1300" height="800" id="canvas"></canvas>
                            </div>
            <script >
            WordCloud(document.getElementById('canvas'), {list:[%s]});
            </script>
            </html>
            '''
    # term_frequency是list of tuple，需要转换为list of list的字符串形式
    t_f = ''
    for i in term_frequency:
        t_f += '[\'' + i[0].encode(
            'utf-8', 'ignore') + '\',' + str(i[1]) + '],'
    with open(prefix.decode('utf-8', 'ignore') + '_term_cloud.html', 'wb') as f:
        f.write(page_template % t_f[:-1])
    print '当前路径为:' + os.getcwd()
    print prefix + '_term_cloud', 'generated!'


def generate_term_cloud_from_file(prefix, df, term_col, frequency_col):
    page_template = '''<html>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
            <script src="http://ajax.googleapis.com/ajax/libs/jquery/1.10.2/jquery.min.js"></script>
            <script src="http://timdream.org/wordcloud2.js/src/wordcloud2.js"></script>
            <div class="span12" id="canvas-container">
                            <canvas width="1300" height="800" id="canvas"></canvas>
                            </div>
            <script >
            WordCloud(document.getElementById('canvas'), {list:[%s]});
            </script>
            </html>
            '''
    # term_frequency是list of tuple，需要转换为list of list的字符串形式
    term = df[term_col].tolist()
    frequency = df[frequency_col].tolist()
    t_f = ''
    for i, t in enumerate(term):
        if type(t) is not unicode:
            break
        t_f += '[\'' + t.encode(
            'utf-8', 'ignore') + '\',' + str(frequency[i]) + '],'
    with open(prefix + '_term_cloud.html', 'wb') as f:
        f.write(page_template % t_f[:-1])
    print '当前路径为:' + os.getcwd()
    print prefix + '_term_cloud', 'generated!'

def get_term_list(text):
    '''增加了对URL，书目，微博表情的抽取
    text需为unicode编码
    '''
    if type(text) in [float, int]:
        text = str(text).decode('utf-8')  # 如果分数字会有问题
    # text = ' '.join(texts)
    url = re.compile(ur'http://[^\s]+')
    name = re.compile(ur'@[^\s!！"“”\'‘’()*+,.，。;《》\？@【】\|~：）]+')
    topic = re.compile(ur'#[^#]+?#')
    book1 = re.compile(ur'《.*?》')
    book2 = re.compile(ur'<.*?>')
    book3 = re.compile(ur'【.*?】')
    mood = re.compile(ur'\[.*?\]')
    app = re.compile(ur'[（(][分享自|来自] @.*?[）)]')
    urls = re.findall(url, text)
    # text = text.replace('.', '')
    apps = re.findall(app, text)
    names = re.findall(name, text)
    names = [n[:-1] for n in names if n[-1] == ':']  # 去掉名字后匹配出来的:
    topics = re.findall(topic, text)
    books = re.findall(book1, text) + re.findall(
        book2, text) + re.findall(book3, text)
    moods = re.findall(mood, text)
    text = re.sub(url, '', text)
    ##    text = re.sub(app, '', text)
    text = re.sub(name, '', text)
    #    text = re.sub(topic, '', text)
    ##    text = re.sub(book1, '', text)
    ##    text = re.sub(book2, '', text)
    ##    text = re.sub(book3, '', text)
    text = re.sub(mood, '', text)
    terms = [i.strip()
             for i in list(jieba.cut(text)) + urls + names + topics + books + moods + apps if i not in stop_words and len(i) > 1]
    return terms


def get_user_term_cloud(screen_name, csv=0, top_n=1000, store_in_mongo=0):
    posts = get_posts(screen_name=screen_name, store_in_mongo=store_in_mongo)
    contents = [i['post_content'] for i in posts]
    generate_content_term_cloud(
        contents, screen_name, csv=csv, top_n=top_n)


def generate_content_term_cloud(contents, file_name=u'content_tags', csv=0, top_n=1000, exclude_list=[]):
    # contents是一个内容list
    terms = [get_term_list(i) for i in contents]
    terms = reduce(lambda x, y: x + y, terms)
    c = collections.Counter(terms)
    topX = c.most_common()
    if csv:
        df = pd.DataFrame(topX, columns=['keywords', '#'])
        max_size = 200
        max_c = math.log(topX[0][1])
        min_c = math.log(topX[-1][1])
        df['weight_log'] = df['#'].apply(
            lambda x: max_size - 1.00 * (max_c - math.log(x)) * (1.00 * (max_size - 1) / (max_c - min_c)))
        max_c = topX[0][1]
        min_c = topX[-1][1]
        df['weight_linear'] = df['#'].apply(
            lambda x: max_size - 1.00 * (max_c - x) * (1.00 * (max_size - 1) / (max_c - min_c)))
        # df = df.applymap(lambda x : x.encode('utf-8','ignore') if type(x) == unicode else x)
        df.to_excel(file_name.encode('utf-8', 'ignore')
                    + '_term_cloud.xlsx', index=False)
    topX = topX[:top_n]
    max_c = topX[0][1]
    min_c = topX[-1][1]
    max_size = 200
    # 最大的频率大于200则按照频率做一个线性变化计算kws大小
    topX = [(i[0], (lambda x: max_size - 1.00 * (max_c - x) * (1.00 * (max_size - 1) / (max_c - min_c)))(i[1]))
                for i in topX if i[0] not in exclude_list]
    # topX = [(i[0],(lambda x: max_size-1.00*(max_c-math.log(x))*(1.00*(max_size-1)/(max_c-min_c)))(i[1])) for i in topX]
    # print topX
    generate_term_cloud(file_name.encode('utf-8', 'ignore'), topX)


def get_terms_coocurence(documents_list):
    # 给定一组文档转换为document_terms_list
    documents_terms = {}
    for document in documents_list:
        # 每个document的term list
        terms = get_term_list(document)
        # 每个document term count
        c = collections.Counter(terms)
        # terms_unique = set(terms)
        topX = c.most_common()
        # 一个document中的terms两两配对，取频率最小值作为文章中共同出现的次数
        for i, t in enumerate(topX):
            for j in topX[i + 1:]:
                documents_terms[(t[0], j[0])] = documents_terms.get(
                    (t[0], j[0]), 0) + topX[topX.index(t) + 1][1]
    return documents_terms


def generated_terms_coocurence_network(documents_list):
    # 生成terms_coocurence_matrix
    terms_coocurence = get_terms_coocurence(documents_list)
    # 生成terms_coocurence_network
    DG = nx.DiGraph()
    for e, v in terms_coocurence.items():
        DG.add_edge(e[0], e[1], weight=v)
    return DG


def terms_frequency(content):
    term_list = get_term_list(content)
    c = collections.Counter(term_list)
    topX = c.most_common()
    return {t: f for t, f in topX}


def calculate_tf_idf(contents):
    term_list_of_list = [get_term_list(c) for c in contents]
    tf_list = [terms_frequency(c) for c in contents]
    idf = {}
    terms_set = {
        term for term_list in term_list_of_list for term in term_list}
    for term in terms_set:
        for term_list in term_list_of_list:
            if term in term_list:
                idf[term] = idf.get(term, 0) + 1
    tf_idf = [{k: float(v) / idf[k] for k, v in tf.items()} for tf in tf_list]
    nums = len(contents)
    # 只选取10%-80%的term组word vector
    # terms_set = {term for term in terms_set if idf[term] / float(nums) >0.1 and idf[term] / float(nums)<0.8}
    return tf_idf, terms_set


def is_zombies(user):
    '''给定一个粉丝的基本信息判断是否是僵尸粉
    输入为一个dict
    Topic > 50%
    to do:需要增加KOL的判断
    '''
    if user.get('account_type', '') in [u'campaign fans', u'verified', u'master', u'normal']:
        logging.info('%s account_type already in mongodb %s',
                     int(user['id']), user['account_type'])
        return user
    if user.get('verified', '') == True:
        user['account_type'] = 'verified'
    elif user.get('verified_type', '') >= 200:
        user['account_type'] = 'master'
    elif user.get('friends_count', '') == 0:
        user['account_type'] = 'campaign fans'
    if user.get('account_type', '') in [u'campaign fans', u'verified', u'master', u'normal']:
        db.weibo_user.update({'id': user['id']}, user, True)
        logging.info('%s is %s', int(user['id']), user['account_type'])
        return user
    logging.info('count_topic %s start', int(user['id']))
    try:
        posts = pd.DataFrame(get_posts(uid=int(user['id']), page=1))
    except HTTPError as i:
        # 存在用户不存在的情况，打一个删除标记
        logging.info('%s error occured %s', str(HTTPError), str(i))
        user['is_deleted'] = True
        user['account_type'] = 'campaign fans'
        logging.info('error occured! %s is %s',
                     int(user['id']), user['account_type'])
        db.weibo_user.update({'id': user['id']}, user, True)
        return user
    if u'topic' not in posts.columns:
        posts['topic'] = False
    if u'retweeted_or_not' not in posts.columns:
        posts['retweeted_or_not'] = False
    user['topic'] = len(posts.ix[posts['topic'] == True])
    user['# of posts'] = len(posts)
    user['# of reposts'] = len(posts.ix[posts['retweeted_or_not'] == True])
    if user['topic'] == 0:
        user['topics_%'] = 0
        user['repost_%'] = 0
        user['topic/repost_%'] = 0
    elif user['# of reposts'] == 0:
        user['topic/repost_%'] = 0
    else:
        user['topics_%'] = user['topic'] * 1.0 / user['# of posts']
        user['repost_%'] = user['# of reposts'] * 1.0 / user['# of posts']
        user['topic/repost_%'] = user['topic'] * 1.0 / user['# of reposts']

    if user[u'topics_%'] >= 0.5:
        # & (user[u'followers_count'] / user[u'friends_count'] * 1.0 > 2) & (user[u'verified'] == False) & (not len(user['description'])) & (not len(user['domain'])) & (not len(user['weihao'])):
        user['account_type'] = 'campaign fans'
    elif user['topic/repost_%'] >= 0.5:
        user['account_type'] = 'campaign fans'
    else:
        user['account_type'] = 'normal'
    logging.info('%s is %s', int(user['id']), user['account_type'])
    db.weibo_user.update({'id': user['id']}, user, True)
    return user


def get_user_info(uid, store_in_mongo=1, inc_type=1):
    # 先尝试从mongodb中捞数据，没有的话再走API抓取
    logging.info('crawling %s , pid is %s', int(uid), str(os.getpid()))
    user_out = {}
    user = db.weibo_user.find({'id': int(uid)}).limit(1)
    if user.count() > 0:
        logging.info('%s is already in mongodb', int(uid))
        user = user[0]
    else:
        try:
            user = get_user_show(uid=int(uid))
        except HTTPError as e:
            logging.info('crawl %s failed, %s', int(uid), str(e))
            # failed_ids.append(uid)
            user = {'id': uid, 'is_deleted': True}
        except URLError as e:
            logging.info('crawl %s failed, %s', int(uid), str(e))
            # failed_ids.append(uid)
            user = {'id': uid, 'is_deleted': True}
        if store_in_mongo:
            # 插入到mongodb
            db.weibo_user.update({'id': user['id']}, user, True)
    if inc_type:
        user = is_zombies(user)
        # 是否包含类型
    for k, v in user.items():
        if (type(v) not in [list, dict]) & (k != u'_id'):
            user_out[k] = v
    return user_out


def get_users_info(uids, store_in_mongo=0, csv=0, file_name='temp', inc_type=1, pool_num=None):
    ''' 给定一组用户返回用户基本信息的list
    to do:
    '''
    users = []
    # failed_ids = []
    l = len(uids)
    pool = mul.Pool(pool_num)
    for index, i in enumerate(uids):
        logging.info(
            '---------crawling %s/%s %s user------', str(index + 1), str(l), int(i))
        # 先尝试从mongodb中捞数据，没有的话再走API抓取
        try:
            users.append(pool.apply(get_user_info, (i, store_in_mongo, inc_type)))
        except:
            logging.info(
                '---------crawling %s/%s %s user failed------', str(index + 1), str(l), int(i))
    pool.close()
    pool.join()
    if csv:
        pd.DataFrame(users).to_excel(
            file_name + '_users_info.xlsx', index=False)
    return users


def get_accounts_posts(screen_names, csv=0, begin_time='', end_time='', pre='temp', pool_num=None):
    '''names = [
    u'长安福特福克斯',
    u'潮-英朗',
    u'起亚汽车中国',
    u'上海汽车荣威',
    u'一汽-大众速腾'
    ]
    posts = get_accounts_posts(names, csv=1)
    '''
    posts_df = pd.DataFrame()
    for n in screen_names:
        posts_df = posts_df.append(get_posts(screen_name=n, pool_num=pool_num))
    if begin_time:
        posts_df = posts_df.ix[(
            posts_df['post_created_at'] > pd.to_datetime(begin_time, utc=True).tz_convert('Asia/Shanghai'))
            & (posts_df['post_created_at'] < pd.to_datetime(end_time, utc=True).tz_convert('Asia/Shanghai'))
            & (posts_df['reposts'] > 0)]
    leng = len(posts_df)
    if leng == 0:
        print 'no data in this date range'
        return posts_df
    if csv:
        posts_df_to_csv = posts_df.copy()
        # 处理mid长度为15的问题
        posts_df_to_csv['post_mid'] = posts_df_to_csv[
            'post_mid'].apply(lambda x: '\'' + str(x))
        posts_df_to_csv['retweeted_mid'] = posts_df_to_csv[
            'retweeted_mid'].apply(lambda x: '\'' + str(x))
        posts_df_to_csv.to_excel((pre + '_posts.xlsx'), index=False)
    return posts_df


def add_senti(forward):
    if type(forward['content']) == float:
        forward['content'] = str(forward['content']).decode('utf-8')
    s = SnowNLP(forward['content'])
    forward['sentiment'] = s.sentiments
    return forward


def get_fans_friends(uid=0, screen_name='', csv=0, store_in_mongo=0):
    '''to do :需要增加一个try catch finally 出错时保存状态。
    在mongodb中增加一个关注表和粉丝表
    '''
    if uid:
        followers_ids = get_followers_ids(uid=uid)['ids']
    else:
        followers_ids = get_followers_ids(screen_name=screen_name)['ids']
    friends = []
    followers_number = len(followers_ids)
    for i, f in enumerate(followers_ids):
        logging.info('crawling %s friends %s/%s',
                     str(f), str(i + 1), str(followers_number))
        friends += get_friends_info(uid=f, store_in_mongo=store_in_mongo)
        logging.info('# of friends is %s', str(len(friends)))
    if csv:
        friends = [{k: v for k, v in f.items() if type(v) not in [list, dict]}
                   for f in friends]
        df = pd.DataFrame(friends)
        file_name = screen_name.encode(
            'utf-8', 'ignore') if screen_name else uid
        df.to_csv(file_name + '_fans_friends.csv',
                  index=False, sep=';', encoding='gb18030')
    return friends


def get_user_type_from_edges(edges, csv=0, store_in_mongo=0, pre='temp', pool_num=None):
    '''
    给定一组edges获得转发用户的数据，包含账号类型判断
    '''
    uids = set([e['poster_id'] for e in edges if e.get('poster_id')])
    users = get_users_info(
        uids, store_in_mongo=store_in_mongo, pool_num=pool_num)
    edges = pd.DataFrame(edges)
    # 处理mid长度为15
    edges['weibo_mid'] = edges['weibo_mid'].apply(lambda x: '\'' + str(x))
    edges['original_mid'] = edges[
        'original_mid'].apply(lambda x: '\'' + str(x))
    edges['pid'] = edges['pid'].apply(lambda x: '\'' + str(x))
    # 处理mid长度为15
    edges['poster_id'] = edges['poster_id'].fillna(0)
    users_type = {u['id']: u['account_type'] for u in users}
    edges['type'] = map(
        lambda x: users_type.get(int(x), None), edges['poster_id'])
    edges['type'] = edges['type'].fillna('campaign fans')
    if csv:
        edges.to_excel(pre + '_edges_fans_type.xlsx', index=False)
    return edges


def get_reposts_fans_type(screen_names, begin_time='', end_time='', pre='temp', pool_num=None, edges_raw=0):
    #get_reposts_fans_type([u'观致汽车', u'潮-英朗'], '2013-10-31', '2014-01-01')
    posts = get_accounts_posts(
        screen_names, csv=1, begin_time=begin_time, end_time=end_time, pre=pre, pool_num=pool_num)
    posts = posts.ix[posts['reposts'] > 0]  # 只抓取有转发的
    leng = len(posts)
    mids = posts['post_mid'].tolist()
    # 抓取所有帖子
    edges = []
    results = []
    # 开多个进程一个进程一个转发
    pool = mul.Pool(pool_num)
    for i, mid in enumerate(mids):
        logging.info('----------crawling %s / %s post edges----------',
                     str(i + 1), str(leng))
        results.append(
            pool.apply_async(get_repost_edges, kwds=dict(post_id=mid, store_in_mongo=1, pool_num=pool_num)))
    pool.close()
    pool.join()
    for r in results:
        try:
            edges += r.get()
        except HTTPError as e:
            # 网络原因报错 存在用户不存在的情况，打一个删除标记
            logging.info('%s error occured', str(e))
            logging.info('----------edges error----------')
            continue
        except IndexError as e:
            logging.info('%s error occured', str(e))
            logging.info('----------edges error----------')
        except URLError as e:
            # 网络原因报错 存在用户不存在的情况，打一个删除标记
            logging.info('%s error occured', str(e))
            logging.info('----------edges error----------')
        except BadStatusLine as e:
            # 网络原因报错 存在用户不存在的情况，打一个删除标记
            logging.info('%s error occured', str(e))
            logging.info('----------edges error----------')
        except IncompleteRead as e:
            logging.info('%s error occured', str(e))
            logging.info('----------edges error----------')
    edges_df = pd.DataFrame(edges)
    # edges_df.to_excel(pre + '_posts_edges.xlsx', index=False)
    # 抓取所有转发
    logging.info('----------edges crawled----------')
    edges_type = get_user_type_from_edges(
        edges, store_in_mongo=1, csv=edges_raw, pre=pre, pool_num=pool_num)
    logging.info('----------completed generating excel----------')
    ctab = pd.crosstab(
        edges_type['original_weibo_url'], edges_type['type'], margins=True)
    posts_type = pd.merge(
        posts, ctab, how='left', left_on='post_url', right_index=True)
    # 处理mid长度为15
    edges_df['weibo_mid'] = edges_df[
        'weibo_mid'].apply(lambda x: '\'' + str(x))
    edges_df['original_mid'] = edges_df[
        'original_mid'].apply(lambda x: '\'' + str(x))
    edges_df['pid'] = edges_df['pid'].apply(lambda x: '\'' + str(x))
    # 处理mid长度为15
    posts_type.to_excel(pre + '_posts_with_type.xlsx', index=False)
    return posts_type


def generate_posts_comments(screen_names, begin_time, end_time, pre, pool_num):
    comments = get_users_comments(screen_names=screen_names, csv=1,
                                  begin_time=begin_time, end_time=end_time, pre=pre, store_in_mongo=1, pool_num=pool_num)
    comments_contents = pd.DataFrame(comments)['text'].tolist()
    generate_content_term_cloud(comments_contents, file_name=pre, csv=1)


def search_status(term, begin='', end='', page=1):
    # 时间是unix时间戳
    token = '2.00NTc2MUE1QzE5OEIzMkZEQzNBREU6'
    term_aru = urllib.urlencode({'q': term.encode('utf-8', 'ignore'), 'starttime': begin, 'endtime': end, 'page': page, 'access_token': token})
    url_pre = 'http://api.uniweibo.com/2/search/statuses.json?count=50&antispam=0&dup=0'
    url = url_pre + '&' + term_aru
    print url
    content = urllib2.urlopen(url).read()
    # 改为用urllib避免BadStatusLine
    # content = urllib.urlopen(url + encoded_args).read()
    return json.loads(content)

def get_search_results(term, begin='', end='', pages=20):
    search_results = []
    search_results_df = []
    for p in range(pages):
        results = search_status(term, begin=begin, end=end, page=p+1)
        if 'statuses' in results:
            search_results += results['statuses']
    for s in search_results:
        search_results_df.append({'created_at': s['created_at'], 'text':s['text'], 'mid':s['mid'], 'uid':s['user']['id'], 'user_name':s['user']['screen_name']})
    return search_results_df


def get_total_number(term):
    dt_begin = datetime.datetime(2013, 1, 1)
    numbers = []
    for i in range(365):
        begin = int(dt_begin.strftime("%s"))
        dt_end = dt_begin + datetime.timedelta(days=i + 1)
        end = int(dt_end.strftime("%s"))
        daily_number = get_searh_results(term, begin, end)['total_number']
        numbers.append(daily_number)
        # dt_begin = dt_end
        print i
    print term, sum(numbers)
    return numbers


def generate_post_comments_term_cloud(mids, file_name=u'comments_tags'):
    comments = []
    for mid in mids:
        comments += get_post_comments(mid, csv=0, store_in_mongo=0, max_id=0)
    comments_text = [c['text'] for c in comments]
    generate_content_term_cloud(
        comments_text, file_name=file_name, csv=1, top_n=1000)


def get_users_tags(uids, csv=0, file_name='temp', pool_num=None):
    tags = {}
    pool = mul.Pool(pool_num)
    for i, uid in enumerate(uids):
        try:
            logging.info('----------crawling %s: %s, pid is %s ----------', str(
                i + 1), str(uid), str(os.getpid()))
            tags_list = pool.apply(get_user_tag, kwds=dict(uid=uid))
            for i in tags_list:
                for j in i:
                    if j != "weight":
                        tags[i[j]] = tags.get(i[j], 0) + 1
        except HTTPError as e:
            logging.info('%s user del, %s', str(uid), str(e))
        except MaybeEncodingError as e:
            logging.info('%s user del, %s', str(uid), str(e))

    pool.close()
    pool.join()
    if csv:
        df = pd.DataFrame()
        df['tag'] = tags.keys()
        df['#'] = tags.values()
        df.to_excel(file_name + '_users_tags.xlsx', index=False)
    return tags


def get_followers_tags(screen_name, csv=0, file_name='temp', pool_num=None):
    uids = get_followers_ids(screen_name=screen_name)['ids']
    tags = get_users_tags(uids, csv=csv, file_name=file_name, pool_num=None)
    return tags


def get_followers_friends(screen_name, csv=0, top=500, pool_num=None):
    '''抓取粉丝的共同关注
    '''
    uids = get_followers_ids(screen_name=screen_name)['ids']
    logging.info('%s total followers is %s', str(
        screen_name.encode('utf-8', 'ignore')), str(len(uids)))
    co_friends = get_co_friends(uids, csv=csv, output_pre=screen_name, top=top)
    return co_friends


def get_co_friends(uids, csv=0, top=500, pool_num=None, output_pre='temp'):
    '''共同关注
    '''
    friends_ids_list = []
    pool = mul.Pool(pool_num)
    for i, f in enumerate(uids):
        logging.info('crawling %s / %s usrs', str(i + 1), str(len(uids)))
        try:
            friends_ids_list += pool.apply(
                get_friends_ids, kwds=dict(uid=f))['ids']
        except URLError as e:
            logging.info('error occured %s', str(e))
    pool.close()
    pool.join()
    logging.info('total user is %s', str(len(friends_ids_list)))
    co_friends_ids = collections.Counter(friends_ids_list).most_common(top)
    co_friends = []
    for i, u in enumerate(co_friends_ids):
        logging.info('crawling co_friends, %s / %s',
                     str(i), str(len(co_friends_ids)))
        user_info = get_user_show(u[0])
        user_info['fre'] = u[1]
        co_friends.append(user_info)
    if csv:
        co_friends = [{k: v for k, v in f.items() if type(v) not in [list, dict]}
                      for f in co_friends]
        df = pd.DataFrame(co_friends)
        df.to_excel(output_pre + '_co_friends.xlsx', index=False)
    return co_friends


def calculate_similar_documents_index(texts):
    word_bag = [get_term_list(t) for t in texts]
    dic = corpora.Dictionary(word_bag)
    corpus = [dic.doc2bow(wg) for wg in word_bag]
    tfidf = models.TfidfModel(corpus)
    corpus_tfidf = tfidf[corpus]
    index = similarities.MatrixSimilarity(corpus_tfidf)
    ids = []
    for n, i in enumerate(index):
        # if n == 10:
        #     print i
        #     break
        # print n
        # print i
        value_counts = np.bincount(i > 0.9)
        if len(value_counts) <= 1:
        # 全部相似度小余0.9
            ids.append(n)
        else:
            if value_counts[1] <= 1:
            # 相似度大于0.9的不超过2，自身+另外一个
                ids.append(n)
    return ids

# db.weibo_user.update({'is_deleted':True,'topics_%':{'$exists':False}},{'$unset':{'is_deleted':'','account_type':''}},multi=True)
# ---------------------------------------------------------------------------##
# -------------------------临时用来调试的代码----------------------------------##
# ---------------------------------------------------------------------------##
# posts = get_posts(screen_name=u'全球时尚最热门', page=1)

# searh_results_df = pd.read_csv('beetle_1000.csv')
# searh_results_df['tokens'] = searh_results_df['text'].apply(get_term_list)
# searh_results_df['tokens_set'] = searh_results_df['tokens'].apply(set)
# tokens = set(reduce(lambda x, y: x + y, searh_results_df['tokens']))
# for t in tokens:
# searh_results_df[t] = searh_results_df['tokens_set'].apply(lambda x: 1 if  t in x else 0)

# token_counts = searh_results_df.sum().reset_index()
# token_counts['%'] = token_counts[0] / len(searh_results_df)
# token_counts.to_csv('beetle_token_counts_search.csv', encoding='utf-8', index=False)


# exclude_ids = post_ids + otrvin_text['mid'].map(lambda x: x[1:]).tolist()

# term = u'欧太林'
# contents = get_search_results(term)
# df = DataFrame(contents)
# df.to_excel('otrvin_text.xlsx', index=False)
# df['text']
# generate_content_term_cloud(df['text'], file_name=u'content_tags', csv=1, top_n=1000)

