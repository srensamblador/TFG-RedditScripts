#!/usr/bin/env python
"""
    Script para indexar documentos en Elastic a partir de ficheros .json de backup
    ------------------------------------------------------------------------------
    El script procesa los ficheros .json (incluido comprimidos en .gz) del directorio que se especifique, indexándolos con unigramas y bigramas en los
    índices reddit-loneliness y reddit-loneliness-ngram.

    Parámetros
    ----------
    * -d, --data-dir: directorio donde está almacenados los .json a indexar
    * -b, --block-size: tamaño de los bloques de líneas a procesar de cada vez, en Mb. Por defecto, 8.
    * -e, --elasticsearch: dirección del servidor Elasticsearch contra el que se indexará.
"""

from elasticsearch import Elasticsearch
from src.elastic_utils.elastic_indexers import Indexer, NgramIndexer
import json, gzip
import os
import argparse
import progressbar as pb

__author__="Samuel Cifuentes García"

def main(args):
    # Carga los .json desde el directorio especificado
    json_files = [file for file in os.listdir(args.data_dir) if file.endswith(".json") or file.endswith(".json.gz") or file.endswith(".ndjson")]

    # Establecer conexión a Elastic
    es = Elasticsearch(args.elasticsearch)
    
    # Se crean los indexadores
    # indexers = [Indexer(es, "reddit-loneliness"), NgramIndexer(es, "reddit-loneliness-ngram")]

    # Caso aplicar un filtro
    subreddits = []
    with open("subreddits_purgar.txt") as f:
        for sub in f:
            subreddits.append(sub.strip())
    subreddit_filter = {"subreddit": subreddits}

    indexers = [Indexer(es, "phase-b-purga", filter_criteria=subreddit_filter)]
    for indexer in indexers:
        if not indexer.index_exists():
            print("Creado índice: " + indexer.index_name)
            indexer.create_index()

    for filename in json_files:
        path = args.data_dir + "/" + filename
        # Se distingue entre .json y comprimidos en .gz
        if filename.endswith(".gz"):
            f = gzip.open(path)
        else:
            f = open(path)

        print("Procesando " + filename + "...")

        block_size = args.block_size*1024*1024 # Se procesará el fichero en bloques
        block = f.readlines(block_size)

        # Barra de progreso
        file_size = os.path.getsize(path)
        indexed_size = 0
        bar = pb.ProgressBar(max_value = file_size, widgets = [
            "- ", pb.Percentage(), " ", pb.Bar(), " ", pb.Timer(), " ", pb.AdaptiveETA()
        ])

        while block:
            index_block(block, indexers)
            block = f.readlines(block_size)

            indexed_size += block_size
            bar.update(min(indexed_size, file_size))

        bar.finish()
        
        f.close()        
        print("- Stats:")
        
        # Se imprimen las estadísticas del indexado
        for indexer in indexers:
            print("\t*%s - Indexed: %d, Errors:%d, Filtered:%d"%(indexer.index_name, indexer.stats["indexed"], indexer.stats["errors"], indexer.stats["filtered"]))

        print(filename + " completado")


def index_block(block, indexers):
    """
        Procesa un bloque de líneas de texto y delega en los indexers el indexado de los documentos.

        Parámetros
        ----------
        block: list of str
            Lista de documentos json, cargados como string desde fichero
        indexers: list of Indexer
            Lista de Indexers con los que se indexarán los documentos
    """
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
    parser.add_argument("-d", "--data-dir", help="Directorio donde se encuentran los .json a indexar", required=True)
    parser.add_argument("-b", "--block-size", type=int, default=8, help="Tamaño de los bloques de líneas a indexar en Mb")
    parser.add_argument("-e", "--elasticsearch", default="http://localhost:9200", help="Dirección del servidor Elasticsearch contra el que indexar")
    return parser.parse_args()

if __name__ == "__main__":
    main(parse_args())