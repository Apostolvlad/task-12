import json
import os
import time

import requests

import service_file

TEST = True

# 157 - Минск
GEO = [157] 
TOKEN = 'AQAAAAAMgF1ZAAcdzwm7PJYIOEe3kePaA-goJvU' # https://oauth.yandex.ru/authorize?response_type=token&client_id=5ea6147df53b46189b22da46c3b10618 #ид приложения

if TEST:
    API_URL = 'https://api-sandbox.direct.yandex.ru/live/v4/json/' 
else:
    API_URL = 'https://api.direct.yandex.ru/v4/json/'

def request_to_api(query):
    temporary_error = 52

    payload = json.dumps(query, ensure_ascii=False).encode('utf-8')
    for attempt in range(10):
        try:
            response = requests.post(API_URL, payload)
            resp_json = response.json()
            if "error_code" in resp_json:
                if resp_json["error_code"] == temporary_error:
                    time.sleep(2)
                    return request_to_api(query)
            time.sleep(1)
            return resp_json
        except requests.exceptions.ConnectionError:
            print("Fail request to wordstat")
        print(f"Retry request to wordstat, attempt: {attempt}")
    raise Exception("Fail request to wordstat")

#ID: 5ea6147df53b46189b22da46c3b10618
#Password: 5a246d11e259442187c8cfde0144f973
#Callback URL: https://oauth.yandex.com/verification_code
# https://yandex.ru/dev/direct/doc/dg-v4/reference/CreateNewWordstatReport.html

# создание отчёта
def create_wordstat_report(phrases):
    query = {
        "method": "CreateNewForecast",
        "param": {
            "Phrases":phrases,
            "GeoID": GEO,
            "Currency": "RUB",
        },
        "token": TOKEN,
        "locale": "ru",
    }
    result = request_to_api(query)
    if result.get('error_str'):
        print(result)
        raise Exception('Error create_wordstat_report')
    return result

# получение отчёта
def get_wordstat_report(id_):
    report_in_progress = 74

    query = {
        "method": "GetForecast",
        "param": id_,
        "token": TOKEN,
        "locale": "ru",
    }

    resp_json = request_to_api(query)
    '''
    'Clicks':2
    'Phrase':'наращивание ресниц рассрочку Минск'
    'Shows':1
    'PremiumMin':73.3
    'CTR':100
    'Min':5.57
    'PremiumCTR':100
    'PremiumClicks':2
    'Currency':'RUB'
    'PremiumMax':73.3
    'FirstPlaceClicks':2
    'FirstPlaceCTR':100
    '''
    if "error_code" not in resp_json:
        delete_wordstat_report(id_)
        phrases = []
        for phrase in resp_json["data"]["Phrases"]:
            phrases.append((phrase["Phrase"], phrase["Shows"]))
        return phrases

    elif resp_json['error_code'] == report_in_progress:
        time.sleep(5)
        return get_wordstat_report(id_)
        
    else:
        delete_wordstat_report(id_)
        raise RuntimeError(resp_json)

# удаление отчета аналитики
def delete_wordstat_report(id_):
    query = {
        "method": "DeleteForecastReport",
        "param": id_,
        "token": TOKEN
    }
    response = request_to_api(query)
    if response["data"] == 1:  # 1 - отчет успешно удалён
        return
    else:
        print(response)
        raise RuntimeError(response)

def get_regions():
    result = request_to_api({"method": "GetRegions", "token":TOKEN})
    service_file.save_json(result, 'regions')
    return result

    #editXML('result.xml')
    
if __name__ == '__main__':
    get_regions() #create_wordstat_report()
    