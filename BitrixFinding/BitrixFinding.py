import re
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import time
import json
from loguru import logger

header={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
logger.add("parsingLog",encoding='utf-8', format="{time} {level} {message}")

jsonOfDomainsBitrix =[] # словарь c доменами и меткой о битрикс

start = time.time()

def funcJsonCompile(cookieDomain:set):

    # создание итогового файла

    try:
        with open('data_of_apps_sw.json', encoding="utf-8") as jsonToCompile:
            jsonDomains = json.load(jsonToCompile)

    except FileNotFoundError:
        logger.error("File data_of_apps_sw.json not such")


    with open('data_of_apps_sw_exit.json', 'w') as jsonExitFile:
        json.dump(BitrixMarking(jsonDomains,cookieDomain), jsonExitFile, indent=4)



def BitrixMarking(jsonDomains:dict,cookieDomain:set) -> dict :

    # Итоговое проставление меток Bitrix на основании парсинга троек + доменов из Cookie bitrix

    for domainFromParsingList in jsonOfDomainsBitrix:

        for domainDictionary in jsonDomains:

            if domainDictionary.get('Bitrix_EXT') is None:
                domainDictionary['Bitrix_EXT'] = ''

            if domainDictionary['Bitrix_EXT'] == 'present':
                continue

            if len(domainDictionary['ip_host_port_tuples']) <= 0:
                domainDictionary['Bitrix_EXT'] = 'unknown'
                continue

            for domain in domainDictionary['ip_host_port_tuples']:
                if str(domainFromParsingList['domain']) == str(domain['host']):
                    domainDictionary['Bitrix_EXT'] = domainFromParsingList['Bitrix_EXT']
                for domainFromCookie in cookieDomain:
                     if re.findall(f"{domainFromCookie}", domain['host']):
                        domainDictionary['Bitrix_EXT'] = 'present'
    return jsonDomains

def compileDomainForParsing() -> set:

    # Парсиег троек из Json

    domainsTriplets:list # Список троек из JSON
    domainSet = set() # Список троек из JSON прербразованный в set

    with open('data_of_apps_sw.json', 'r') as domainsFromJson:
        domainsTriplets = json.load(domainsFromJson)

    for domains in domainsTriplets:
        for domain in domains['ip_host_port_tuples']:
            domainSet.add(domain['host'])

    return domainSet


async def parsingDomain(domain:str) -> str:

    # Непосредственный парсинг

    counterLinc = 0 # счетчик линков Linc на bitrix
    counterSrc = 0 # счетчик ссылок Src на bitrix
    counterA = 0 # счетчик ссылок a на bitrix
    counterCookie = 0 # счетчик cookie
    counterCms = 0  # счетчик poweredCms
    domainFromCookie = 0 # домен из cookie bitrix`a
    url = f'https://{domain}' # url для отправки запроса

    timeout = aiohttp.ClientTimeout(total=200)
    conn = aiohttp.TCPConnector()

    async with aiohttp.ClientSession(connector=conn,trust_env = True,timeout=timeout) as session:

        try:
            response = await session.get(url=url, headers=header, ssl=False)
            bs = BeautifulSoup(await response.content.read(), 'lxml')


            for link in bs.find_all('link', href=re.compile('bitrix')):         # Считаем количество href, которые ведут на Bitrix
                counterLinc += 1

            for a in bs.find_all('a', href=re.compile('bitrix')):               # Считаем количество a , которые ведут на Bitrix
                counterA += 1

            for src in bs.find_all('script', src=re.compile('bitrix')):         # Считаем количество src , которые ведут на Bitrix
                counterSrc = counterSrc + 1

            if response.headers.get('X-Powered-Cms') is not None and re.match("Bitrix Site Manager",  response.headers['X-Powered-Cms']): # Считаем количество заголовков X-Powered-Cms равных Bitrix Site Manager
                counterCms += 1

            if response.headers.get('X-Bitrix-Ajax-Status') is not None:        # Считаем количество заголовков X-Bitrix-Ajax-Status
                counterCms += 1

            if response.cookies.get("BITRIX_SM_GUEST_ID") is not None or response.cookies.get("BITRIX_SM_LAST_VISIT") is not None:     # Просмотр наличия Cookie
                counterCookie += 1
                domainFromCookie = (response.cookies.get("BITRIX_SM_GUEST_ID").get("domain"))
            bitrixAnalitic(counterLinc, counterA, counterSrc, counterCms, domain, counterCookie,str(response.status))
        except Exception as r:

             domainBitrixInfo = {
                'domain': f'{domain}',
                'Bitrix_EXT': 'unknown'
               }
             jsonOfDomainsBitrix.append(domainBitrixInfo)
             logger.error(f"{domain,'unknown', r}")

    return domainFromCookie

def bitrixAnalitic(counterLinc,counterSrc,counterA,counterCms,domain,counterCookie,status):

    # Анализ наличия битрикса по ссылкам и куки

    if (counterLinc+counterSrc+counterA >=  1 or counterCms >= 1 or counterCookie >=1):
        domainBitrixInfo = {
            'domain': f'{domain}',
            'Bitrix_EXT': 'present'
        }
        jsonOfDomainsBitrix.append(domainBitrixInfo)
        logger.info(f"{domain, 'present' ,status}")
    else:
        domainBitrixInfo = {
            'domain': f'{domain}',
            'Bitrix_EXT': 'absent'
        }
        jsonOfDomainsBitrix.append(domainBitrixInfo)
        logger.info(f"{domain, 'absent', status}")

async def main(domains:set):
    tasks = []
    for url in domains:
        task = asyncio.create_task(parsingDomain(url.replace('\n','')))
        tasks.append(task)
    domainFromCookie = await asyncio.gather(*tasks)
    funcJsonCompile(set(domainFromCookie))  # запись в json


if __name__ == '__main__':
    result =  compileDomainForParsing() #парсинг доменов
    asyncio.run(main(result))


end = time.time() - start
print(end)