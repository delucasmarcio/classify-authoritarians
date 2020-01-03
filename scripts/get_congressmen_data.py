# -*- coding: utf-8 -*-
"""
Created on Mon May 13 17:06:54 2019

@author: Marcio
"""

## Importando libs
import re
import pandas as pd
from pandas.io.json import json_normalize

import json

import requests 
import asyncio
from aiohttp import ClientSession
from asyncio_throttle import Throttler

from functools import reduce

## Funções
def build_query(idLegislatura):
    # retornando url
    return 'https://dadosabertos.camara.leg.br/api/v2/deputados?idLegislatura=' + str(idLegislatura)

def get_all_pages(linksJson):
    # identificando primeira e última páginas
    first = linksJson[1]['href']
    last = linksJson[2]['href']
    # calculando número de páginas
    n_pages = re.search('pagina=(.+?)&itens',last).group(1)
    # extraindo url principal
    coreLink = first.split('&pagina=')[0] + '&pagina=' + '{}' + '&itens=' + first.split('&itens=')[1]
    # retornando lista de links
    return [coreLink.format(i) for i in range(2,int(n_pages))]

async def get_data(url, session):
    # definindo função de requisição do dados
    async def request_data(url, session):   
        async with session.get(url) as result:
            textR = await result.text() # XML >> txt
            return json.loads(textR) # txt >> JSON
    # limitando número de requisições por minuto
    async with Throttler(rate_limit=600, period=60):
        result = await request_data(url, session)
        # extraindo dados de deputados
        dataDep = result['dados']
        # verificando se todos os resultados foram coletados
        if len(result['links']) > 2:
            # iterando por links de novas páginas
            for link in get_all_pages(result['links']):
                newUrl = link['href']
                newResult = await request_data(newUrl, session)
                newDataDep = newResult['dados']
                # agregando dados em um único json
                dataDep += newDataDep
        
        return dataDep

def get_congressman(legislaturas):
    # definindo loop
    loop = asyncio.get_event_loop()
    # closure para construção de tasks
    async def inner(idLegislatura):
        async with ClientSession() as session:
            return await get_data(build_query(idLegislatura),session)
    # definindo lista de tasks
    tasks = [loop.create_task(inner(lt)) for lt in legislaturas]

    return loop.run_until_complete(asyncio.gather(*tasks))

# Extraindo dados de deputados
jsonDeputados = get_congressman(list(range(51,57)))

# Concatenando arquivos JSON
jsonDeputados = reduce(lambda a, b: a + b, jsonDeputados)

# JSON >> Pandas Dataframe
dfDeputados = json_normalize(jsonDeputados)
dfDeputados = dfDeputados.drop_duplicates()

# Exportando dados
path = '/home/marcio/Documentos/dissertacao/classify-authoritarian/data/'
dfDeputados.to_csv(path + 'dadosDeputados_2000ate2019.csv', sep='\t', encoding='utf-8', index = False)