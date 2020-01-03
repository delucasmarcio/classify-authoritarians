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

from time import sleep
from datetime import datetime
from dateutil.relativedelta import relativedelta

## Funções
def build_query(uri,idLegislatura):
    return uri + '/discursos?idLegislatura=' + str(idLegislatura)

mu = requests.get('https://dadosabertos.camara.leg.br/api/v2/deputados/74847/discursos?idLegislatura=54')
mu = json.loads(mu.text)
mu = mu['links']

def get_links(linksJson):
    dfLinks = json_normalize(linksJson)
    first_page = dfLinks.loc[dfLinks['rel'] == 'first']['href'].values[0]
    last_page = dfLinks.loc[dfLinks['rel'] == 'last']['href'].values[0]

    return (first_page,last_page)

def get_all_pages(linksJson):
    # identificando primeira e última páginas
    first = get_links(linksJson)[0]
    last = get_links(linksJson)[1]
    
    # calculando número de páginas
    n_pages = re.search('pagina=(.+?)&itens',last).group(1)
    # extraindo url principal
    coreLink = first.split('&pagina=')[0] + '&pagina=' + '{}' + '&itens=' + first.split('&itens=')[1]
    # retornando lista de links
    return [coreLink.format(i) for i in range(2,int(n_pages)+1)]

def are_multiple_pages(linksJson):
    try:
        first = get_links(linksJson)[0]
        last = get_links(linksJson)[1]

        if first == last:
            return False
        else:
            return True
    except:
        return False

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
        dataDis = result['dados']
        # verificando se todos os resultados foram coletados
        if are_multiple_pages(result['links']) == True:
            # iterando por links de novas páginas
            for newUrl in get_all_pages(result['links']):
                newResult = await request_data(newUrl, session)
                newDataDis = newResult['dados']
                # agregando dados em um único json
                dataDis += newDataDis
        
        return dataDis

def scrape_discourses(dep_tuples):
    # definindo loop
    loop = asyncio.get_event_loop()
    # closure para construção de tasks
    async def inner(uri,idLegislatura):
        async with ClientSession() as session:
            url = build_query(uri,idLegislatura)
            return await get_data(url,session)
    # definindo lista de tasks
    tasks = [inner(dep_tuple[0],dep_tuple[1]) for dep_tuple in dep_tuples]

    return loop.run_until_complete(asyncio.gather(*tasks))

def get_discourses(dep_tuples):
    init = 0
    end = init + 10
    jsonDiscursos = []
    n_queries = len(dep_tuples)

    while init < n_queries:
        # extraindo porção do banco de dados
        slice_tuple = dep_tuples[init:end]
        # extraindo discursos
        slice_discursos = scrape_discourses(slice_tuple)
        # concatenando discursos
        jsonDiscursos += slice_discursos
        # agregando
        init = end
        end += 10
        # visualizando progresso
        print(
            """
            #######################################
            #              PROGRESSO              #      
            #######################################
            #                                     #
                        {} % concluído          
            #                                     #
            #######################################
            """.format(
                round(end*100/n_queries,2)
                )
        )
    
    return jsonDiscursos

# Definindo path
path = '/home/marcio/Documentos/dissertacao/classify-authoritarian/data/'

# Carregando dados de deputados
dfDeputados = pd.read_csv(path + 'dadosDeputados_2000ate2019.csv', sep="\t", encoding='utf-8')

# Preprando inputs
dep_tuples = [(dfDeputados.iloc[i]['uri'],str(dfDeputados.iloc[i]['idLegislatura'])) for i in range(dfDeputados.shape[0])]

# Extraindo dados de deputados
jsonDiscursos = get_discourses(dep_tuples)

# Concatenando arquivos JSON
jsonDiscursosConcat = reduce(lambda a, b: a + b, jsonDiscursos)

# JSON >> Pandas Dataframe
dfDiscursos = json_normalize(jsonDiscursosConcat)
dfDiscursosNoDuplicates = dfDiscursos.drop_duplicates()

# Exportando dados
path = '/home/marcio/Documentos/dissertacao/classify-authoritarian/data/'
dfDiscursos.to_csv(path + 'dadosDiscursos_2000ate2019.csv', sep='\t', encoding='utf-8', index = False)
dfDiscursosNoDuplicates.to_csv(path + 'dadosDiscursos_2000ate2019_NoDuplicates.csv', sep='\t', encoding='utf-8', index = False)