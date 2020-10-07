"""
    Script que crea un índice de usuarios de Reddit a partir de un dataset almacenado en un fichero

    Parámetros
    ----------
    * -i, --index: Nombre del índice de usuarios a crear.
    * -d, --data: Ruta del archivo con el dataset de usuarios.
    * -e, --elasticsearch: Dirección del servidor elastic contra el que indexar. Por defecto, http://localhost:9200
"""

import csv
import gzip
from elasticsearch import Elasticsearch
from src.elastic_utils.elastic_indexers import UserIndexer
import progressbar as pb
import argparse

__author__ = "Samuel Cifuentes García"

CHUNK_SIZE = 500000

def main(args):
    es = Elasticsearch(args.elasticsearch)
    # Se crea el índice si no existe
    indexer = UserIndexer(es, args.index)
    if not indexer.index_exists():
        print("Creando índice " + indexer.index_name)
        indexer.create_index()

    f = gzip.open(args.data, "rt")
    data = csv.reader(f)
    
    # La primera fila del .csv contiene las cabeceras
    headers = next(data)

    print("Indexando...")
    bar = pb.ProgressBar()
    cache = []
    for line in bar(data):
        cache.append(line)

        # Por limitaciones de memoria se indexa en trozos
        if len(cache) >= CHUNK_SIZE:
            indexer.index_documents(cache, headers)
            cache = []

def parse_args():
    """
        Procesamiento de los argumentos con los que se ejecutó el script
    """
    parser = argparse.ArgumentParser(description="Script para obtener los usuarios que postearon en un subreddit, obtener sus datos e indexarlos en un nuevo índice")
    parser.add_argument("-i", "--index", default="users-reddit", help="Nombre del índice de Elasticsearch en el que se indexaran los usuarios")
    parser.add_argument("-d", "--data", default="dataset-users/data.csv.gz", help="Ruta del archivo con el dataset de usuarios")
    parser.add_argument("-e", "--elasticsearch", default="http://localhost:9200", help="Dirección del servidor Elasticsearch")
    return parser.parse_args()

if __name__=="__main__":
    main(parse_args())