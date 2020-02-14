#!/usr/bin/env python
from elasticsearch import Elasticsearch
from elasticsearch_indexers import Indexer, NgramIndexer
import argparse
import json
from datetime import datetime
from psaw import PushshiftAPI

def main(args):
    global es
    es = Elasticsearch(args.elasticsearch)

    global api
    api = PushshiftAPI()


    pass

def query_api(query, before_timestamp, after_timestamp, num_posts, cache_size=3000):
    gen = api.search_submissions(q=query, before=before_timestamp, after=after_timestamp, limit=num_posts)
    cache = []    

    for c in gen:
        c.d_["query"] = ""
        c.d_["scale"] = "random baseline"
        c.d_["lonely"] = False
        cache.append(c.d_)

        if len(cache) == cache_size:
            
            dump_to_file(cache)
            elastic_index(cache)
            
            print(" *", datetime.fromtimestamp(cache[-1]["created_utc"]).strftime("%Y-%m-%d"))

            cache = []

            numIter += 1

def dump_to_file(results):
    """
        Vuelca una lista de submissions a un fichero .json. Los volcados se deben realizar de forma parcial 
        debido a limitaciones de memoria.
        Las escrituras son mucho más rápidas si se tratan como strings en vez de objetos JSON.

        Parámetros
        ----------
        results: list
            lista de documentos a volcar   
    """
    with open("randombaselineDump.json", "a") as f:
        for result in results:
            f.write(json.dumps(result) + "\n")

def elastic_index(results):
    """
    Indexa la lista de documentos pasados por parámetro. 
        Se crean dos índices si no existen, uno para unigramas y otro para bigramas. 
        A cada documento se le añadirá tres campos: la consulta y escala utilizadas para obtenerlo y 
        un booleano para marcarlos como positivos en soledad.

        Parámetros
        ----------
        results: list
            lista de documentos a indexar
    """
    indexers = [Indexer(es, "reddit-loneliness"), NgramIndexer(es, "reddit-loneliness-ngram")]
    for indexer in indexers:
        if not indexer.index_exists():
            print("Creado índice: " + indexer.index_name)
            indexer.create_index()
        
        indexer.index_documents(results)

def parse_args():
    """
        Procesamiento de los argumentos con los que se ejecutó el script
    """
    parser = argparse.ArgumentParser(description="Script para la extracción de submissions de Reddit a través de pushshift.io")
    parser.add_argument("-s", "--source", default="hourly_posts.csv", help="Fichero con la lista de post por intervarlo de tiempo.")
    parser.add_argument("-d", "--dump-dir", default="dumps", help="Directorio donde se volcarán los archivos .json de backup")
    parser.add_argument("-e", "--elasticsearch", default="http://localhost:9200", help="dirección del servidor Elasticsearch contra el que se indexará")
    return parser.parse_args()

if __name__ == "__main__":
    main(parse_args())