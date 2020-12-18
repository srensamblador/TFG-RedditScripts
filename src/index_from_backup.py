#!/usr/bin/env python
"""
    Script para indexar documentos en Elastic a partir de ficheros .json de backup
    ------------------------------------------------------------------------------
    El script procesa los ficheros .json (incluido comprimidos en .gz) del directorio que se especifique, indexándolos con unigramas y bigramas en los
    índices reddit-loneliness y reddit-loneliness-ngram.

    Parámetros
    ----------
    * -i, --index: nombre del índice a crear.
    * -d, --data-dir: directorio donde está almacenados los .json a indexar.
    * -f, --filter: fichero JSON con un filtro de campos y valores a aplicar. Opcional.
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

    # Caso aplicar un filtro
    indexer_filter = None
    if args.filter:
        with open(args.filter) as f:
            try:
                indexer_filter = json.load(f)
            except:
                print("Error loading filter .json")
                exit(1)
    
    indexer = Indexer(es, args.index, filter_criteria=indexer_filter)
       
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
            index_block(block, indexer)
            block = f.readlines(block_size)

            indexed_size += block_size
            bar.update(min(indexed_size, file_size))

        bar.finish()
        
        f.close()        
        print("- Stats:")
        
        # Se imprimen las estadísticas del indexado
        print("\t*%s - Indexed: %d, Errors:%d, Filtered:%d"%(indexer.index_name, indexer.stats["indexed"], indexer.stats["errors"], indexer.stats["filtered"]))

        print(filename + " completado")


def index_block(block, indexer):
    """
        Procesa un bloque de líneas de texto y delega en los indexers el indexado de los documentos.

        Parámetros
        ----------
        block: list of str  
            \tLista de documentos json, cargados como string desde fichero
        indexer: Indexer  
            \tIndexador que procesará los documentos
    """
    data = []
    for line in block:
        post = json.loads(line)
        data.append(post)
    
    indexer.index_documents(data)


def parse_args():
    """
        Procesamiento de los argumentos con los que se ejecutó el programa
    """
    parser = argparse.ArgumentParser(description="Script para cargar documentos desde ficheros .json e indexarlos en un servidor Elastic")
    parser.add_argument("-i", "--index", help="Nombre del índice en el que indexar los posts", required=True)
    parser.add_argument("-d", "--data-dir", help="Directorio donde se encuentran los .json a indexar", required=True)
    parser.add_argument("-f", "--filter", help="Opcionalmente, se puede proporcionar un filtro a los indexadores. Será un .json con los campos y valores a filtrar.")
    parser.add_argument("-b", "--block-size", type=int, default=8, help="Tamaño de los bloques de líneas a indexar en Mb")
    parser.add_argument("-e", "--elasticsearch", default="http://localhost:9200", help="Dirección del servidor Elasticsearch contra el que indexar")
    return parser.parse_args()

if __name__ == "__main__":
    main(parse_args())