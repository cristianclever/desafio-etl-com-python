import logging
import os
import sys
import pandas as pd
import json
from pymongo import MongoClient
import listagem_acoes_importer as ai
import re
import math

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(threadName)s [%(name)s.%(funcName)s] -  %(message)s",
)

def insert_news(news_list):
    client = MongoClient("mongodb://localhost:27017/")
    db = client.get_database("news_db")
    collection = db.get_collection("news")
    collection.insert_many(news_list)


def load_b3_ticker_info():


    client = MongoClient("mongodb://localhost:27017/")
    # Return the database object (e.g., 'my_local_database')
    db = client.get_database("news_db")

    try:
        collection = db.get_collection("listagem-de-acoes")
        ticker_list = list(collection.aggregate([
            {'$match': {'_id': re.compile(r"[^F]$")}},
            {'$lookup': {'from': 'b3-tickers', 'localField': 'issuingCompany', 'foreignField': 'issuingCompany',
                         'as': 'empresa', 'pipeline': [
                    {'$project': {'companyName': 1, 'tradingName': 1, '_id': 0, 'cnpj': 1}}
                ]
                         }
             },
            {
                '$unwind': '$empresa'
            },
            {
                '$group': {
                    '_id': '$issuingCompany',
                    'companyName': {
                        '$first': '$empresa.companyName'
                    },
                    'tradingName': {
                        '$first': '$empresa.tradingName'
                    },
                    'symbols': {
                        '$addToSet': '$_id'
                    }
                }
            }
        ]))

        client.close()
    except Exception as e:
        raise Exception("Unable to find the document due to the following error: ", e)
    return ticker_list

def load_news():
    return  pd.read_csv('../resources/Historico_de_materias.csv')



def simple_matches(title:str, content:str , tickerInfo):
    full_match = False
    simple_match = False

    if isinstance(title, str) and isinstance(content, str) :
        searchable_content =  title.upper() + " " + content.upper()

        #Primeiro realiza uma busca simplificada
        for mainKey in list([tickerInfo["_id"], tickerInfo["tradingName"]]):
            if mainKey in searchable_content:
                simple_match = True
                break

    return simple_match


def matches(title:str, content:str , tickerInfo):
    full_match = False
    simple_match = False

    if isinstance(title, str) and isinstance(content, str) :
        searchable_content =  title.upper() + " " + content.upper()

        #Primeiro realiza uma busca simplificada
        for mainKey in list([tickerInfo["_id"], tickerInfo["tradingName"]]):
            if mainKey in searchable_content:
                simple_match = True
                break

        #Se a busca simplificada teve exito, realiza uma busca mais  profunda
        if simple_match:
            for key in tickerInfo["symbols"]:
                if key in searchable_content:
                    full_match = True
                    break

    return full_match





if __name__ == "__main__":
    logging.info(      f"### Iniciando processamento do  id_ciclo_monitoracao:" )

    # Realiza a importacao de uma listagem de ativos. Os resultados são inseridos em uma collection mongodb
    logging.info(f"### Importando listagem de symbols")
    #ai.import_ticker_list()

    logging.info(f"### Obtem uma uniao de dados da collection: listagem-de-acoes")
    ticker_dict = load_b3_ticker_info()

    logging.info(f"### Obtem com publicações de Noticias")
    df_news = load_news()

    #Aplica limpeza dos dados
    df_news = df_news.drop('url_noticia', axis=1)
    filtered_news =  df_news[df_news['assunto'].isin(['economia', 'politica', 'tecnologia'])]


    news_list = list()

    #step 2 - Analisa o conteudo de cada noticia em 2  estagios


    for row in filtered_news.itertuples():
        dic = {
            "date": row.data,
            "url": row.url_noticia_curto,
            "title": row.titulo,
            "category": row.assunto,
            "content": row.conteudo_noticia,
            "tags" : list()
        }

        tags = list()

        for tickerInfo in ticker_dict:
            result = matches(row.titulo, row.conteudo_noticia, tickerInfo)
            if result:
                tags.extend(tickerInfo["symbols"])

        if len(tags) > 0:
            dic["tags"].extend(tags)
            news_list.append(dic)

    insert_news(news_list)


    del filtered_news
    del df_news

    #Pre verifica as noticias que contem um codigo de ativo valido na B3




    xx=0