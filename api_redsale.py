from sys import float_repr_style
import time
from typing import Dict, List, Any
import requests

import service_file

TOKEN = service_file.loud_txt('token')

if TOKEN is None or TOKEN == '':
    print('Ошибка, нет токена от redsale, вставьте токен в файл token.txt')
    service_file.save_txt([], 'token', '')
    exit(0)

def request_to_api(url:str, params:dict = {}):
    result =  requests.get(url, params = params)
    result = result.json()
    time.sleep(0.01)
    return result

def set_timer(func):
    def wrapper():
        func_name = func.__qualname__
        print('run', func_name)
        start_time = time.monotonic()
        result = func()
        end_time = time.monotonic()
        print('finish', func_name, ':', end_time - start_time)
        return result
    return wrapper

class City:
    def __init__(self, cityId: int, beautify: str, name: str, **kwarg:dict) -> None:
        self.id = cityId
        self.name = name
        self.beautify = beautify
    
    def __repr__(self) -> str: return f'{self.name} id = {self.id} en = {self.beautify}'

class Container:
    def __init__(self, containerId:int, metro:Any, district:Any, **kwarg:dict) -> None:
        self.id = containerId
        self.metro = metro
        self.district = district
        
    def __repr__(self) -> str: 
        m = f'|metro = {self.metro}' if self.metro else ''
        d = f'|district = {self.district}' if self.district else ''
        return f'id = {self.id}{m}{d}'
    
    @property
    def beautify(self):
        p = self.metro or self.district
        if p:return p["beautify"]
        return False

class Vacancy:
    def __init__(self, vacancyId:int, seo:dict = None, sectionHeader:str = '', **kwarg:dict) -> None:
        self.id = vacancyId
        if seo: 
            self.header = seo.get('sectionHeader', '')
        else:
            self.header = sectionHeader         

    def __repr__(self) -> str:
        return f'{self.id} {self.header}'

class Geo:
    def __init__(self, city:City) -> None:
        self._city:City = city
        self.vacancy:Vacancy = None
        self.containers:List[Container] = list()
    
    @property
    def id(self) -> int: return self._city.id

    @property
    def name(self) -> str: return self._city.name

    @property
    def beautify(self) -> str: return self._city.beautify
    
    def add_container(self, container:Container) -> None:
        self.containers.append(container)
    
    def set_vacancy(self, vacancy:Vacancy) -> None:
        self.vacancy = vacancy
    
    def __repr__(self) -> str:
        return f'{self.name}|{self.vacancy}|{self.containers}'

class Section:
    type = ''
    name = ''
    beautify = ''
    specialization = ''
    parent = None

    def __init__(self, sectionId:int, **kwarg:dict) -> None:
        self.id:int = sectionId
        self.base_geo:Dict[int, Geo] = dict()
        self.childs:List[Section] = list()
        self.update_params(kwarg)

    def update_params(self, kwarg:dict) -> None:
        if kwarg.get('type'): self.type = kwarg['type']
        if kwarg.get('name'): self.name = kwarg['name']
        if kwarg.get('beautify'): self.beautify = kwarg['beautify']
        if kwarg.get('specialization'): self.specialization = kwarg['specialization']
        if kwarg.get('parent'): 
            self.parent = kwarg['parent']
            self.parent.childs.append(self)

    def __repr__(self) -> str:
        par = f'|{self.parent}' if self.parent else ''
        return f'Id = {self.id} name = {self.name}{par} url = {self.url}'

    def get_containers(self) -> List[Container]:
        return self.containers
    
    @property
    def url(self) -> str:
        if self.parent:
            if self.type == 'tag_employee':
                return f'{self.parent.url}/tag/{self.beautify}'
            return f'{self.parent.url}/{self.beautify}'
        return self.beautify

    @property
    def parent_id(self) -> int:
        if self.parent: return self.parent.id
        return None
    
    def get_or_create_geo(self, city:City) -> Geo:
        geo = self.base_geo.get(city.id)
        if geo is None:
            geo = Geo(city)
            self.base_geo.update({geo.id:geo})
        return geo

    def add_container(self, city:City, container:Container) -> None:
        self.get_or_create_geo(city).add_container(container)
    
    def add_vacancy(self, city:City, vacancy:Vacancy) -> None:
        self.get_or_create_geo(city).set_vacancy(vacancy) 

class Sections:
    base:Dict[str, Section] = dict()
    base_urls:Dict[str, Section] = dict()
    white_ids = (24, 6672, 25, 7873, 30, 8890, 32, 9001, 33, 9133, 34,	9229, 36, 9738, 37)
	#(3040, 3042, 3045, 3048, 3049, 6871, 6872, 6873, None)
    @classmethod
    def get_or_create(cls, sectionId:int, parent_default:Any = None, **kwarg:dict) -> Section:
        #if not sectionId in cls.white_ids: return
        if sectionId is None: return
        section = cls.get(sectionId)
        if section is None: 
            section = Section(sectionId = sectionId, parent = cls.get_or_create(sectionId = kwarg.get('parentId', parent_default), parent_default = parent_default), **kwarg)
            cls.base.update({section.id:section})
        elif len(kwarg):
            kwarg.update({'parent':cls.get_or_create(sectionId = kwarg.get('parentId', parent_default))})
            section.update_params(kwarg)
        return section
    
    @classmethod
    def get(cls, id:int) -> Section: return cls.base.get(id)

    @classmethod
    def save(cls):
        with open('data.pickle', 'wb') as f:
            pickle.dump([cls.base, cls.base_urls], f)
    
    @classmethod
    def load(cls):
        with open('data.pickle', 'rb') as f:
            data_new = pickle.load(f)
            cls.base.update(data_new[0])
            cls.base_urls.update(data_new[1])

    @classmethod
    def get_to_url(cls, url:str):
        return cls.base_urls.get(url)

    @classmethod
    def distribution(cls):
        for section in cls.base.values():
            cls.base_urls.update({section.url:section})

@set_timer
def get_cities() -> List[City]:
    params = {'token':TOKEN}
    return [City(**data) for data in request_to_api('https://redsale.by/api/cities', params)] 

@set_timer
def get_root() -> None:
    params = {'token':TOKEN}
    for data in request_to_api('https://redsale.by/api/sections/roots', params):
        if data['sectionId'] in (24, 6672, 25, 7873, 30, 8890, 32, 9001, 33, 9133, 34, 9229, 36, 9738, 37):
            Sections.get_or_create(**data)

@set_timer
def process_root() -> None:
    get_root()
    params = {'token':TOKEN}
    root_ids = tuple(Sections.base.keys())
    for root_id in root_ids:
        b = request_to_api(f'https://redsale.by/api/sections/{root_id}/children', params)
        sec = [Sections.get_or_create(parent_default = root_id, **data) for data in b]
    Sections.distribution() # распределяем section по urls
    #print(Sections.get(356))
import pickle
@set_timer
def process_container() -> None:
    cities = get_cities()
    for city in cities:
        params = {'token':TOKEN, 'cityId': city.id}
        for data in request_to_api('https://redsale.by/api/vacancies/sections', params):
            section = Sections.get(data['sectionId'])
            if section is None: continue
            section.add_vacancy(city = city, vacancy = Vacancy(**data)) 

        for data in request_to_api('https://redsale.by/api/containers', params):
            section = Sections.get(data['sectionId'])
            if section is None: continue
            section.add_container(city = city, container = Container(**data))

def get_name_childs(result, child):
    result.append((child.name, child.type))
    for child in child.childs:
        result.append((child.name, child.type))
        get_name_childs(result, child)

@set_timer
def main():
    #Sections.load()
    if False:
        process_root()
        process_container()
        Sections.save()
        return 
    else:
        Sections.load()
        result = list()
        get_name_childs(result, Sections.get_to_url('mebel/peretyazhka-mebeli'))
        print(result)
        print('')

if __name__ == '__main__':
    main()
    
    