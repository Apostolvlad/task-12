import asyncio
import json
import os
import re
import traceback
from functools import reduce
from typing import Any

import aiohttp
from pymorphy2 import MorphAnalyzer

import api_wordstate
import api_xmlriver
import parsing
import service_file
import service_table
import service_webmasters
from api_redsale import *
from setting import LOCS

#api_redsale.Sections.load()
URL = {
    'makiyazh':'https://redsale.by/krasota/makiyazh',
    'manikur':'https://redsale.by/krasota/manikur',
    'narashivanie-resnic':'https://redsale.by/krasota/narashivanie-resnic',
    'remont-televizorov':'https://redsale.by/remont-tehniki/remont-televizorov',
    'sborka-mebeli':'https://redsale.by/mebel/sborka-mebeli',
    'plumber':'https://redsale.by/remont/plumber'
}#'https://redsale.by/krasota/makiyazh'# 'https://redsale.by/krasota/manikur'#'https://redsale.by/krasota/narashivanie-resnic'

URL = {
    #'electrician':'https://redsale.by/remont/electrician'
    'massazh':'https://redsale.by/krasota/massazh',
    'plumber':'https://redsale.by/remont/plumber',
    'sborka-mebeli':'https://redsale.by/mebel/sborka-mebeli'
}

URL = {
    'peretyazhka-mebeli':'https://redsale.by/mebel/peretyazhka-mebeli',
    #'logoped':'https://redsale.by/repetitory/logoped',
    #'animators':'https://redsale.by/artist/animators'
}


URL = service_file.loud_txt('urls')
IGNORE_WORDS = ('цены', 'цена', 'расценки', 'стоимость', 'сколько стоит', 'прайс', 'не дорого', 'дешёво')

BLACK_PREGLOGS = ['и', 'в', 'во', 'не', 'что', 'он', 'на', 'я', 'с', 'со', 'как', 'а', 'то', 'все', 'она', 'так', 'его', 'но', 'да', 'ты', 'к', 'у', 'же', 'вы', 'за', 'бы', 'по', 'только', 'ее', 'мне', 'было', 'вот', 'от', 'меня', 'еще', 'нет', 'о', 'из', 'ему', 'теперь', 'когда', 'даже', 'ну', 'вдруг', 'ли', 'если', 'уже', 'или', 'ни', 'быть', 'был', 'него', 'до', 'вас', 'нибудь', 'опять', 'уж', 'вам', 'ведь', 'там', 'потом', 'себя', 'ничего', 'ей', 'может', 'они', 'тут', 'где', 'есть', 'надо', 'ней', 'для', 'мы', 'тебя', 'их', 'чем', 'была', 'сам', 'чтоб', 'без', 'будто', 'чего', 'раз', 'тоже', 'себе', 'под', 'будет', 'ж', 'тогда', 'кто', 'этот', 'того', 'потому', 'этого', 'какой', 'совсем', 'ним', 'здесь', 'этом', 'один', 'почти', 'мой', 'тем', 'чтобы', 'нее', 'сейчас', 'были', 'куда', 'зачем', 'всех', 'никогда', 'можно', 'при', 'наконец', 'два', 'об', 'другой', 'хоть', 'после', 'над', 'больше', 'тот', 'через', 'эти', 'нас', 'про', 'всего', 'них', 'какая', 'много', 'разве', 'три', 'эту', 'моя', 'впрочем', 'хорошо', 'свою', 'этой', 'перед', 'иногда', 'лучше', 'чуть', 'том', 'нельзя', 'такой', 'им', 'более', 'всегда', 'конечно', 'всю', 'между']

# True - 9/10 false - 7/10
MODE_MARKER = False

PATH = ''
CLICKS = 1
IMPRESSIONS_MIN = 50
MODE_PARSING = True # True
MODE_GOOGLE = True # False


'''
9/10 только marker
show = 0 выделяем цветом #b6d7a8
не выводим те где меньше 2 анкоров. типа один = один
не выводим где есть слова цена и тд., маркерные оставляем

7/10 только не marker
show > 0 выделяем цветом #b6d7a8
не выводим где есть слова цена и тд.,
'''


COMPLIANCE_MIN = 9 if MODE_MARKER else 7  # совпадение 6/10

Sections.load()

with open('black-words.txt', encoding='UTF-8') as f: BLACK_WORDS = set(f.read().split('\n'))

if MODE_PARSING:
    morph = MorphAnalyzer()
else:
    morph = None

def save_json(base, t = ''):
    path = '' if PATH == '' else f'{PATH}\\'
    with open(f'{path}base{t}.json', "w", encoding='UTF-8') as f:
        f.write(json.dumps(base, indent=4, ensure_ascii=False))

def loud_json(name = ''):
    path = '' if PATH == '' else f'{PATH}\\'
    if not os.path.exists(f'{path}base{name}.json'): return None
    with open(f'{path}base{name}.json', encoding='UTF-8') as f:
        return json.load(f)

def query_to_key(query):
    query = correct_city(query)
    query = check_black_words(query)
    r = list()
    for word in query.split(' '):
        r.append(morph.normal_forms(word)[0])
    r.sort()
    return ' '.join(r)

def load_data_url(url):
    base = service_webmasters.get_data(url, 'equals')
    base = dict(map(lambda x: (query_to_key(x['keys'][0]), {'query':x['keys'][0], 'clicks':x['clicks'], 'impressions':x['impressions']}), base))
    save_json(base)

def correct_city(value):
    value = value.lower().replace('в минске', '').replace('минск', '')
    res = f'{value} Минск'.strip()
    res = re.sub('[^\w/.]', ' ', res)
    res = res.replace('  ', ' ')
    return res

def check_black_words(text):
    res = list()
    for word in text.split(' '):
        if word in BLACK_WORDS: continue
        res.append(word)
    return ' '.join(res)

def process_query():
    def f(x):
        x = list(x)
        if x[1]['clicks'] >= CLICKS or x[1]['impressions'] >= IMPRESSIONS_MIN:
            x[1]['query'] = check_black_words(correct_city(x[1]['query']))
            return x
        return (None, None)
    base = loud_json()
    base = dict(sorted(base.items(), key=lambda x:x[1]["clicks"]))
    base = dict(map(f, base.items()))
    if None in base: base.pop(None)

    save_json(base, '1')

def load_serp(url):
    url = url.replace('https://redsale.by/', '')
    result = list()
    get_name_childs(result, Sections.get_to_url(url))
    result = dict(map(lambda x: (query_to_key(x[0]), {'query':check_black_words(correct_city(x[0])), 'name':x[1], 'is_serp':True}), result))
    save_json(result, '_serp')
    return 
    result = dict()
    try:
        parsing.serp(result, url = url)
    except:
        traceback.print_exc()
    #b = dict(filter(lambda x: x[1][1] == 'inf', BASE_MARKERS_CRUMBS.items()))
    result = dict(map(lambda s: (query_to_key(s[0]), {'query':check_black_words(correct_city(s[0])), 'url':s[1], 'is_serp':True}), result.items()))
    #result2 = dict(map(lambda s: (query_to_key(s), {'query':check_black_words(correct_city(s)), 'url':None, 'is_serp':True}), b.keys()))
    #result2.update(result)
    save_json(result, '_serp')

def process_serp():
    base1 = loud_json('1')
    base_serp = loud_json('_serp')
    base_serp.update(base1)
    '''
    for key, data in base_serp.items():
        key = key
        base = base1.get(key)
        if base is None:
            base = dict()
            base1.update({key:base})
        base.update(data)
    '''
    save_json(base_serp, '2')

def revers_key(name1, name2):
    base1 = loud_json(name1)
    base2 = dict()
    for data in base1.values():
        query = data.pop('query')
        base2.update({query:data})
    save_json(base2, name2)

def process_google():
    base = loud_json('3')
    print('start process google')
    i = 0
    i_max = len(base.keys())
    for q, data in base.items():
        i += 1
        if i % 10:
            print('Осталось:', i, '/', i_max)
        data.update({'urls':api_xmlriver.get_urls(q, LOCS[0])})
    save_json(base, '4')


async def process_google2():
    async def add(work_queue):
        async with aiohttp.ClientSession() as session:
            while not work_queue.empty():
                q, data = await work_queue.get()
                data.update({'urls':await api_xmlriver.get_urls2(session, q, LOCS[0])})
                print(work_queue.qsize())
                work_queue.task_done()
        
    work_queue = asyncio.Queue()
    base = loud_json('3')
    print('start process google')
    for params in base.items():
        work_queue.put_nowait(params)

    await asyncio.gather(
        asyncio.create_task(add(work_queue)),
        asyncio.create_task(add(work_queue)),
        asyncio.create_task(add(work_queue)),
    )
    save_json(base, '4')

def process_wordstate():
    base = loud_json('4')
    print('start process wordstate')
    base_keys = list(base.keys()) # проблема с фразами в которых есть запятая.
    for i in range(0, len(base_keys), 100):
        phrases = list() # base_keys[i:i+100]
        max = i + 100
        if max > len(base_keys): max = len(base_keys)
        for ii in range(i, max):
            if len(base_keys[ii].split(' ')) > 7: continue
            phrases.append(base_keys[ii])
        res = api_wordstate.create_wordstat_report(phrases)
        id_ = res.get('data')
        if res is None:
            print(res)
        result = api_wordstate.get_wordstat_report(id_)
        for key, value in result:base[key].update({'Shows':value})
    save_json(base, '5')

def clustering():
    base = loud_json('5')
    for v in base.values():
        v["urls"] = set(v["urls"])
    base_c1 = dict()
    for key in list(base.keys()):
        data_select = base.pop(key)
        urls = data_select['urls']
        base_keys = list()
        base_c1.update({key:base_keys})
        for q, data2 in base.items():
            if len(urls.intersection(data2['urls'])) < COMPLIANCE_MIN: continue
            base_keys.append(q)
        base.update({key:data_select})
    save_json(base_c1, '_q1')
    base_c2 = dict()
    for main_query, data in base_c1.items():
        main_data = dict()
        for child_query in data:
            child_data = set(base_c1[child_query])
            main_data.update({child_query:list(child_data.intersection(data))})
        while len(main_data):
            _, max_ = max(main_data.items(), key = lambda value: len(value[1]))
            max_ = len(max_)
            key_min, min_ = min(main_data.items(), key = lambda value: len(value[1]))
            min_ = len(min_)
            if min_ >= max_: break
            main_data.pop(key_min)
            tuple(map(lambda x:x.remove(key_min) if key_min in x else None, main_data.values()))
        result = list()
        if len(main_data):
            main_data = tuple(main_data.items())[0]
            result.append(main_data[0])
            result.extend(main_data[1])
        base_c2.update({main_query:result})
    save_json(base_c2, '_q2')
    base_c3 = dict()
    #base = loud_json('5')
    base_query = list()
    for main_query, data_query in base_c2.items():
        data_query.append(main_query)
        data_query = set(data_query)
        data_query.difference_update(set(base_query))
        base_query.extend(data_query)
        if not len(data_query): continue
        res_query = main_query
        min_shows = 0
        min_impressions = 0
        for child_query in data_query:
            r = base[child_query]
            if r.get('is_serp'):
                res_query = child_query
                data_query.add('SERP')
                break
            if r.get("Shows", 0) > min_shows:
                min_shows = r["Shows"]
                res_query = child_query
            elif min_shows == 0:
                #print(r, min_impressions)
                if r["impressions"] > min_impressions:
                    min_impressions = r["impressions"]
                    res_query = child_query
        data_query.remove(res_query)
        
        base_c3.update({res_query:list(data_query)})
    save_json(base_c3, '_q3')
    return

def check_ignore_words(query):
    for word in IGNORE_WORDS:
        if query.find(word) != -1: return False
    return True

def generate_table(table:service_table.Table):
    def get_row(key:str = '', type:str = '', query:str = '', crumbs:str = '', Shows = '', clicks = '', impressions = '', marker = '', **kwarg):
        query_serp = base3.get(key)
        if key == crumbs: crumbs = ''
        query = query.replace(' Минск', '').capitalize()
        return [marker, type, key, query, crumbs, Shows, clicks, impressions]
    base = loud_json('5')
    base3 = loud_json('3')
    base_c3 = loud_json('_q3')
    result = list()
    result.append(['marker', 'type', 'key', 'query', 'crumbs', 'show', 'click', 'impressions'])
    if MODE_MARKER:
        check_marker = lambda x: x == ''  
        #check_shows = lambda x: type(x) is str or x == 0
    else: 
        check_marker = lambda x: x != ''
    check_shows = lambda x: type(x) is str or x == 0
    for key, base_query in base_c3.items():
        if not len(base_query):continue
        query_serp = base3.get(key)
        if query_serp.get('is_serp') is None:
            query_serp = ()
            type_, crumbs = '', ''
            marker = ''
        else:
            #query_serp = query_serp['url'][1]
            type_, crumbs = query_serp['name'], ''
            marker = 'marker'
        if check_marker(marker): continue
        result2 = list()
        if not check_ignore_words(key):
            base_query = sorted(base_query, key = lambda info: info[2])
            if base_query[0][2] == 0: base_query = sorted(base_query, key = lambda info: info[3])
            if base_query[0][3] == 0: base_query = sorted(base_query, key = lambda info: info[4])
            for query in base_query:
                if check_ignore_words(query):
                    key = query
                    break
            else:
                continue
        result2.append(get_row(key = key, type = type_, crumbs = crumbs, marker = marker, **base[key]))
        for query in base_query:
            if query == 'SERP': continue
            #if query in query_serp: continue
            if check_ignore_words(query):
                result2.append(get_row(query = query, **base[query]))
        if MODE_MARKER and len(result2) < 3: continue
        result.extend(result2)
        result.append(get_row())
    if len(result) < 2: return 
    table.clear_sheet(f'{PATH} 9/10' if MODE_MARKER else f'{PATH} 7/10')
    table.update_values(result)
    base_cells = list()
    for i_row, row in enumerate(result, 0):
        if check_shows(row[5]): continue
        base_cells.append(service_table.Cells(i_row, 5)) # 2 столбец query по 6 столбец impressions
    table.set_format_Cell2(base_cells)


async def main():
    global PATH, MODE_MARKER
    for url in URL:
        path = 'data\\' + url[url.find('redsale.by/') + 11:].replace('tag/', '').replace('/', '\\')
        print(path)
        PATH = path
        if not os.path.exists(path):
            os.makedirs(path)
        if MODE_PARSING:
            print('process info')
            load_data_url(url)
            process_query()
            load_serp(url)
            process_serp()
            revers_key('2', '3')
            revers_key('_serp', '_serp')
        if MODE_GOOGLE:
            print('parser google and wordstate...')
            await process_google2()
            process_wordstate()
    #if MODE_PARSING or MODE_GOOGLE: return 

    table = service_table.Table('1NTivk7d4PQ21N8bh44LJMaSQ_yuK4THBNGj3RvC2wCw')
    for url in URL:
        #path = url[url.rfind('/') + 1:] # url[url.find('redsale.by/') + 11:].replace('tag/', '').replace('/', '\\')
        PATH = 'data\\' + url[url.find('redsale.by/') + 11:].replace('tag/', '').replace('/', '\\')
        MODE_MARKER = False
        clustering()
        
        generate_table(table)
        MODE_MARKER = True
        clustering()
        generate_table(table)
        time.sleep(2)


    

if __name__ == '__main__':
    asyncio.run(main()) #main()
    