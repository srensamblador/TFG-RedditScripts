from psaw import PushshiftAPI
import argparse
from datetime import datetime
import json
from elasticsearch import Elasticsearch
from elastic_indexers import Indexer, NgramIndexer

api = PushshiftAPI()

def main(args):
    # Establece la conexión a Elastic
    global es
    es = Elasticsearch()

    # Crea el .json de backup donde se volcarán los post
    global filename
    filename = "dumps/" + args.query.replace(" " ,"") + "-Dump.json"
    with open(filename, "w") as f:
        f.write("{\"data\": [")

    query_API(args.query, args.scale, cache_size=3000)

    with open(filename, "a") as f:
        f.write("]}")

def query_API(query, scale, cache_size = 3000):
    '''
        Se utiliza psaw (https://github.com/dmarx/psaw) para consumir la API de Pushshift y extraer submissions. 
        A cada submission se le añaden tres campos: la frase con la que se obtuvo, la escala a la que pertenece la frase y
        un booleano para indicar que el post dio positivo en una escala.

        :param query: frase a consultar
        :param scale: escala a la que pertenece la frase
        :param cache_size: cada cuantos post se realiza una escritura, opcional. Por defecto, 10000
    '''

    gen = api.search_submissions(q=query)
    cache = []
    numIter = 0

    for c in gen:
        if numIter >= 4:
            break

        cache.append(c.d_)

        if len(cache) == cache_size:
            print(".", end="")
            
            dump_to_file(cache, numIter == 0)
            elastic_index(cache)

            cache = []
            numIter += 1

def dump_to_file(results, initial_dump):
    '''
        Vuelca una lista de submissions a un fichero .json
    '''

    if initial_dump:
        delimiter = ""
    else:
        delimiter=","
    
    with open(filename, "a") as f:
        f.write(delimiter + json.dumps(results).strip("[").strip("]"))

def elastic_index(results):
    indexers = [Indexer(es, "reddit-loneliness"), NgramIndexer(es, "reddit-loneliness-ngram")]
    for indexer in indexers:
        if not indexer.index_exists():
            print("Creado índice: " + indexer.index_name)
            indexer.create_index()
        
        indexer.index_documents(results)

def parse_args():
    parser = argparse.ArgumentParser(description="Script para la extracción de submissions de Reddit a través de pushshift.io")
    parser.add_argument("query", help="Frase usada como query")
    parser.add_argument("scale", help="Escala a la que pertenece la frase")
    args = parser.parse_args()
    return args

main(parse_args())