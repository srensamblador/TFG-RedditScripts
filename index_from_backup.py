from elasticsearch import Elasticsearch
from elastic_indexers import Indexer, NgramIndexer
import json
import os
import argparse

def main(args):
    # Carga los .json desde el directorio especificado
    json_files = [file for file in os.listdir(args.data_dir) if file.endswith(".json")]

    # Establecer conexión a Elastic
    es = Elasticsearch(args.elasticsearch)
    
    # Se crean los indexadores
    indexers = [Indexer(es, "reddit-loneliness"), NgramIndexer(es, "reddit-loneliness-ngram")]
    for indexer in indexers:
        if not indexer.index_exists():
            print("Creado índice: " + indexer.index_name)
            indexer.create_index()

    for filename in json_files:
        with open(args.data_dir + "/" + filename) as f:
            print("Procesando " + filename + "...")

            block_size = 8*1024*1024
            block = f.readlines(block_size)
            while block:
                index_block(block, indexers)
                block = f.readlines(block_size)
            
        print("Completado")


def index_block(block, indexers):
    data = []
    for line in block:
        post = json.loads(line)
        data.append(post)

    for indexer in indexers:
        indexer.index_documents(data)


def parse_args():
    """
        Procesamiento de los argumentos con los que se ejecutó el programa
    """
    parser = argparse.ArgumentParser(description="Script para cargar documentos desde ficheros .json e indexarlos en un servidor Elastic")
    parser.add_argument("-d", "--data-dir", default="dumps", help="Directorio donde se encuentran los .json a indexar")
    parser.add_argument("-e", "--elasticsearch", default="http://localhost:9200", help="Dirección del servidor Elasticsearch contra el que indexar")
    return parser.parse_args()

main(parse_args())
