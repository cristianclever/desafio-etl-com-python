import requests
import logging
import json
from pymongo import MongoClient



def import_ticker_list():


    client = MongoClient("mongodb://localhost:27017/")
    db = client.get_database("news_db")
    collection_listagem_de_acoes = db.get_collection("listagem-de-acoes")

    payload = {}
    headers = {
        'user-agent': 'PostmanRuntime/7.49.1'
    }


    logging.info(f"### Excluindo Dados da Collection: listagem-de-acoes")
    collection_listagem_de_acoes.delete_many({})
    set_ativos_importados = set()
    for i in range(1,73):
        url = f"https://www.genialinvestimentos.com.br/_next/data/iYgtzTsT7rOS8IQeHyFrc/onde-investir/renda-variavel/acoes/listagem-de-acoes/page/{i}.json"
        print(f"requesting page {url}")
        response = requests.request("GET", url, timeout=20, headers=headers, data=payload)
        if response.status_code == 200:
            list_tickers = list()
            json = response.json()
            tickers = json["pageProps"]["tickers"]
            for ticker in tickers:
                symbol = ticker["title"]

                if symbol not in set_ativos_importados:
                    set_ativos_importados.add(symbol)
                    list_tickers.append({
                        "issuingCompany":symbol[0:4],
                        "_id": symbol,
                        "post_type": ticker["post_type"],
                        "type": ticker["type"],
                    })
            if len(list_tickers) > 0:

                collection_listagem_de_acoes.insert_many(list_tickers)



