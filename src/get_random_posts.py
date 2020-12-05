#!/usr/bin/env python
"""
    Script que obtiene e indexa posts aleatorios de Reddit
    ------------------------------------------------------
    A partir de un fichero .csv que contiene intervalos de tiempo y un número de post para cada intervalo,
    el script utiliza la API de Pushshift para obtener tantos post aleatorios como se indiquen en cada intervalo,
    volcándolos en ficheros e indexándolos en Elastic.

    Parámetros
    ----------
    * -s, --source: fichero .csv con los intervalos de tiempo y el número de post en cada uno. Por defecto, `hourly_posts.csv`
    * -o, --output: fichero donde se volcarán los post. Por defecto, `randombaselineDump.json`
    * -e, --elasticsearch: dirección del servidor elastic contra el que indexar. Por defecto, http://localhost:9200
"""

from elasticsearch import Elasticsearch
from src.elastic_utils.elastic_indexers import Indexer, NgramIndexer
import argparse
import json
import os
from datetime import datetime
from psaw import PushshiftAPI
import csv
import progressbar as pb

__author__ = "Samuel Cifuentes García"

def main(args):
    global es
    es = Elasticsearch(args.elasticsearch)

    # Crea un directorio para los volcados si no existe
    global output_file
    output_file = args.output

    query_api(load_csv(args.source))


def load_csv(filename):
    """
        Procesa el fichero .csv con los intervalos de tiempo y número de posts por intervalo.
        El formato del fichero será el siguiente:  
            TimestampInicio;TimestampFinal;NumPosts
        
        Parámetros
        ----------
        filename: str
            Nombre del fichero .csv a leer
        
        Salida
        ------
        submissions_per_hour: list of tuples
            Lista de intervalos en forma de tupla (TimestampInicio, TimestampFinal, NumPosts)
    """
    submissions_per_hour = []
    with open(filename) as raw_file:
        csv_file = csv.reader(raw_file, delimiter=";")
        for line in csv_file:
            submissions_per_hour.append(line)
    return submissions_per_hour


def query_api(submissions_per_hour, cache_size=3000):
    """
        Para cada intervalo de tiempo se recuperan tantos post como hayan sido especificados. 
        Los post extraídos se indexan en Elastic y se vuelcan en .json a modo de backup. Estos post,
        obtenidos de forma aleatoria, se marcan con los siguientes campos para diferenciarlos de aquellos post
        que dieron positivo en alguna escala:
        * query: ""
        * scale: "random baseline"
        * lonely: False
        Se utiliza psaw (https://github.com/dmarx/psaw), un wrapper de la API de Pushshift para recuperar los post

        Parámetros
        ----------
        submissions_per_hour: list of tuple
            Lista de tuplas con intervalos de tiempo y número de post
        cache_size: int
            Opcional. Cada cuantos documentos se va a realizar un volcado e indexado
    """
    api = PushshiftAPI()

    cache = []
    # Barra de progreso para dar feedback
    bar = pb.ProgressBar(max_value=len(submissions_per_hour))
    for interval in bar(submissions_per_hour):
        gen = api.search_submissions(after=int(float(interval[0])), before=int(float(interval[1])), limit=int(interval[2]))
        for c in gen:
            # Establecemos estos campos para identificar los post como aleatorios
            c.d_["query"] = ""
            c.d_["scale"] = "random baseline"
            c.d_["lonely"] = False
            cache.append(c.d_)

            if len(cache) >= cache_size:                
                dump_to_file(cache)
                elastic_index(cache)

                cache = []

            dump_to_file(cache)
            elastic_index(cache)

def dump_to_file(results):
    """
        Vuelca una lista de submissions a un fichero .json. Los volcados se deben realizar de forma parcial 
        debido a limitaciones de memoria.
        Las escrituras son mucho más rápidas si se tratan como strings en vez de objetos JSON.

        Parámetros
        ----------
        results: list
            Lista de documentos a volcar   
    """
    with open(output_file, "a") as f:
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
            Lista de documentos a indexar
    """
    indexers = [Indexer(es, "subreddit-lonely"), NgramIndexer(es, "subreddit-lonely-ngram")]
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
    parser.add_argument("-o", "--output", default="randombaselineDump.ndjson", help="Fichero donde se volcarán los post")
    parser.add_argument("-e", "--elasticsearch", default="http://localhost:9200", help="dirección del servidor Elasticsearch contra el que se indexará")
    return parser.parse_args()

if __name__ == "__main__":
    main(parse_args())