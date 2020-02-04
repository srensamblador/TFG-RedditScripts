#!/usr/bin/env python
"""
    Script para la extracción e indexado de posts de Reddit relevantes a items de escalas psicométricas
    ----------------------------------------------------------------------------------------------
    author: Samuel Cifuentes García

    Script encargado de emplear la API de Pushshift para extraer el histórico de posts de Reddit que sean relevantes
    para un item de una escala psicométrica de soledad. 
    Se vuelcan los documentos obtenidos en ficheros .json.
    Se indexan en Elasticsearch, previamente añadiendo campos para identificar la frase y escala con la que se obtuvieron los documentos,
    además de añadiendo un booleano para marcar los documentos como positivos en alguna escala de soledad.

    Para ejecutar el script se necesita un fichero de texto con frases y escalas separadas por líneas siguiendo el siguiente formato:
        Frase 1;Escala 1
        Frase 2;Escala 2
        Frase 3;Escala 3
        ...
    Por defecto se lee el fichero frases.txt, pero se puede especificar otro mediante el parámetro -q o --query-file

    También se puede especificar el directorio donde se volcarán los .json (-d o --dump-dir) así como la dirección del servidor Elasticsearch 
    (-e o --elasticsearch, por defecto localhost:9300)
"""

from psaw import PushshiftAPI
import argparse
from datetime import datetime
import json
import os
from elasticsearch import Elasticsearch
from elastic_indexers import Indexer, NgramIndexer

api = PushshiftAPI()

def main(args):
    # Establece la conexión a Elastic
    global es
    es = Elasticsearch(args.elasticsearch)

    # Se cargan las frases a procesar desde el fichero pasado por parámetro
    queries = load_queries(args.query_file)

    # Crea un directorio para los volcados si no existe
    if not os.path.exists(args.dump_dir):
        os.makedirs(args.dump_dir)

    for query, scale in queries.items():
        # Crea el .json de backup donde se volcarán los post
        global dump_filename
        dump_filename = args.dump_dir + "/" + query.replace(" " ,"") + "-Dump.json"
        with open(dump_filename, "w") as f:
            f.write('''
                {{
                    "query": {q},
                    "scale": {s}
                    "documents": [
                '''.format(q=query, s=scale))

        query_API(query, scale, cache_size=300)

        with open(dump_filename, "a") as f:
            f.write("]}")

        print("Consulta completada: ", query)

def load_queries(filename):
    """
        Carga las frases a consultar e indexar desde un fichero de texto pasado por parámetro.
        
        Parámetros
        ----------
        filename: str
            fichero de texto con las frases a utilizar en la ejecución del programa

    """
    queries = {}

    with open(filename) as f:
        for line in f:
            query = line.strip().split(";")
            queries[query[0]] = query[1]
    
    return queries

def query_API(query, scale, cache_size = 3000):
    '''
        Se utiliza psaw (https://github.com/dmarx/psaw) para consumir la API de Pushshift y extraer submissions. 
        A cada submission se le añaden tres campos: la frase con la que se obtuvo, la escala a la que pertenece la frase y
        un booleano para indicar que el post dio positivo en una escala.

        Parámetros
        ----------
        query: str
            Frase a consultar contra la API
        scale: str
            Escala a la que pertenece la frase
        cache_size: int
            Opcional. Cada cuantos documentos se realiza un volcado e indexado
    '''
    gen = api.search_submissions(q=query)
    cache = []
    numIter = 0

    for c in gen:
        if numIter >= 3:
            break
        
        cache.append(c.d_)

        if len(cache) == cache_size:
            
            dump_to_file(cache, numIter == 0)
            elastic_index(cache, query, scale)

            cache = []
            numIter += 1

def dump_to_file(results, initial_dump):
    '''
        Vuelca una lista de submissions a un fichero .json. Los volcados se deben realizar de forma parcial 
        debido a limitaciones de memoria.
        Las escrituras son mucho más rápidas si se tratan como strings en vez de objetos JSON.

        Parámetros
        ----------
        results: list
            lista de documentos a volcar
        initial_dump: boolean
            para indicar que delimitador se usa al volcar los documentos, de forma que se 
            construya un JSOn válido
e           
    '''
    if initial_dump:
        delimiter = ""
    else:
        delimiter=","
    
    with open(dump_filename, "a") as f:
        f.write(delimiter + json.dumps(results).strip("[").strip("]"))

def elastic_index(results, query, scale):
    """
        Indexa la lista de documentos pasados por parámetro. 
        Se crean dos índices si no existen, uno para unigramas y otro para bigramas. 
        A cada documento se le añadirá tres campos: la consulta y escala utilizadas para obtenerlo y 
        un booleano para marcarlos como positivos en soledad.

        Parámetros
        ----------
        results: list
            lista de documentos a indexar
        query: str
            frase utilizada para extraer los documentos
        scale: str
            escala psicométrica de la que proviene la frase
    """
    indexers = [Indexer(es, "reddit-loneliness", query, scale, lonely=True), NgramIndexer(es, "reddit-loneliness-ngram", query, scale, lonely=True)]
    for indexer in indexers:
        if not indexer.index_exists():
            print("Creado índice: " + indexer.index_name)
            indexer.create_index()
        
        indexer.index_documents(results)

def parse_args():
    """
        Parseo de los argumentos con los que se ejecutó el programa
    """
    parser = argparse.ArgumentParser(description="Script para la extracción de submissions de Reddit a través de pushshift.io")
    parser.add_argument("-q", "--query-file", default="frases.txt", help="Fichero con las frases a consultar.")
    parser.add_argument("-d", "--dump-dir", default="dumps", help="Directorio donde se volcarán los archivos .json de backup")
    parser.add_argument("-e", "--elasticsearch", default="http://localhost:9200", help="dirección del servidor Elasticsearch contra el que se indexará")
    args = parser.parse_args()
    return args

main(parse_args())