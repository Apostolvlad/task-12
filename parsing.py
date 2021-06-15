import requests
from bs4 import BeautifulSoup
import time
import re

def correct_city(value):
    value = value.lower().replace('в минске', '').replace('минск', '')
    res = f'{value} Минск'.strip()
    res = re.sub('[^\w/.]', ' ', res)
    res = res.replace('  ', ' ')
    return res

def get_base_query(soup):
    seo = soup.find(class_ = 'seo')
    if seo is None: return list()
    p = seo.find('p')
    if p is None: return list()
    return list(map(lambda x: correct_city(x.text.strip()), p.find_all('a')))

def serp(result, url):
    contents = requests.get('https://redsale.by/sitemap.xml').text
    soup = BeautifulSoup(contents, 'lxml')
    urls = list(map(lambda loc:loc.text, soup.find_all('loc')))
    for ignor_url in ('https://redsale.by', 'https://redsale.by/sections', 'https://redsale.by/about', 'https://redsale.by/partner'): 
        urls.remove(ignor_url)
    urls = list(filter(lambda x: x.startswith(url), urls)) 
    i_max = len(urls)
    for i, url in enumerate(urls):
        soup = BeautifulSoup(requests.get(url).text, 'lxml')
        key = soup.h1.text.replace('\n', '').strip()
        print(url)
        if key.find('Минск') == -1: continue
        base_query = get_base_query(soup)
        #if len(base_query) == 0:
        
        result.update({key:(url, base_query)})
        if not i % 10:
            print(f'Осталось: {i}/{i_max}')
        print(f'Осталось: {i}/{i_max}')
        time.sleep(0.5)

if __name__ == '__main__':
    serp()
    