#!/usr/bin/env python
"""
    Script para obtener el número de post por hora en el índice de Elasticsearch
    ----------------------------------------------------------------------------
    Sobre un índice de Elasticsearch especificado, se obtiene el número de posts en cada intervalo de una hora.  
    La lista de datos resultante se serializa en un fichero con el siguiente formato:

        TimestampInicial;TimestampFinal;NúmeroPosts
    
    Parámetros
    ----------
    * -e, --elasticsearch: dirección del servidor Elasticsearch. Por defecto, http://localhost:9200
    * -i, --index: nombre del índice sobre el que realizarán las consultas.
    * -o, --output: fichero donde se serializarán los resultados. Por defecto, "hourly_posts.csv"
"""

from elasticsearch import Elasticsearch
import argparse
from datetime import datetime, timedelta
import progressbar as pb
import math


__author__ = "Samuel Cifuentes García"


def main(args):
    # Conexión a Elastic
    global es
    es = Elasticsearch(args.elasticsearch)

    # Se obtienen las fechas límite
    dates = get_boundary_dates(args.index)

    print("Procesando documentos...")
    submissions_per_hour = []
    after_date = dates[1]

    # Barra de progreso
    bar = pb.ProgressBar(max_value=100, widgets=[
        "- ", pb.Percentage(), " ", pb.Bar(), " ", pb.Timer(), " ", pb.AdaptiveETA()
    ])
    while after_date > dates[0]:
        # Para cada intervalo de una hora se obtiene el número de posts
        before_date = after_date - timedelta(hours=1)

        # Actualizo la barra de progreso
        bar.update(math.ceil((dates[1].timestamp() - before_date.timestamp())
                             / (dates[1].timestamp() - dates[0].timestamp()) * 100))

        lonely_count = get_submission_count(
            before_date.timestamp(), after_date.timestamp(), args.index, True)
        random_count = get_submission_count(
            before_date.timestamp(), after_date.timestamp(), args.index, False)
        # Si un intervalo no tiene post no se incluye
        if lonely_count > 0:
            submissions_per_hour.append(
                (before_date.timestamp(), after_date.timestamp(), lonely_count, random_count))

        after_date = before_date
    bar.finish()

    print("Total documentos: ", sum([x[2] for x in submissions_per_hour]))

    # Guardamos los resultados en el archivo especificado
    print("Serializando en " + args.output + "...")
    write_to_csv(args.output, submissions_per_hour)

    print("Completado")


def get_boundary_dates(index):
    """
        Obtiene la fecha más reciente y la más antigua presente en el índice. 
        Para ello se emplean agregaciones de máximo y mínimo sobre el campo created_utc de los
        documentos.

        Parámetros
        ----------
        index: str
            nombre del índice sobre el que consultar

        Salida
        ------
        oldest_date: datetime
            fecha más antigua presente en el índice
        newest_date: datetime
            fecha más reciente presente en el ínndice

    """
    query = {
        "size": 0,
        "aggs": {
            "oldest_date": {
                "min": {
                    "field": "created_utc"
                }
            },
            "newest_date": {
                "max": {
                    "field": "created_utc"
                }
            }
        }
    }
    res = es.search(
        index=index,
        body=query
    )

    oldest_date = res["aggregations"]["oldest_date"]["value"]
    newest_date = res["aggregations"]["newest_date"]["value"]

    # Redondeamos los timestamp a horas exactas
    oldest_date = datetime.fromtimestamp(oldest_date)
    oldest_date = oldest_date.replace(minute=0, second=0, microsecond=0)

    newest_date = datetime.fromtimestamp(newest_date)
    newest_date = (newest_date + timedelta(hours=1)
                   ).replace(minute=0, second=0, microsecond=0)

    return oldest_date, newest_date


def get_submission_count(before_timestamp, after_timestamp, index, is_lonely):
    """
        Recupera el número de documentos en un índice en el intervalo de tiempo especificado.
        Se utiliza la API Count: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-count.html,
        ya que es más eficiente que contar el número de hits en una consulta Search

        Parámetros
        ----------
        before_timestamp: int
            timestamp de la fecha inicial del intervalo
        after_timestamp: int
            timestamp de la fecha final del intervalo
        index: str
            nombre del índice

        Salida
        ------
        count: int
            número de posts en el intervalo temporal
    """
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "created_utc": {
                                "gt": before_timestamp,
                                "lte": after_timestamp
                            }
                        }
                    },
                    {
                        "term": {
                            "lonely": is_lonely
                        }
                    }
                ]
            }
        }
    }

    res = es.count(
        index=index,
        body=query
    )
    return res["count"]


def write_to_csv(filename, submissions):
    """
        Vuelca una lista de post por intervalo de tiempo al fichero especificado. Cada elemento de la lista será
        una tupla de la forma:  
            (TimestampInicio,TimestampFinal,NumPosts)

        Parámetros
        ----------
        filename: str
            nombre del fichero  donde se serializarán los datos
        submissions: list of tuples
            lista de tuplas con los datos a serializar
    """
    with open(filename, "w") as f:
        for tuple in submissions:
            f.write(";".join([str(int(x)) for x in tuple]) + "\n")


def parse_args():
    """
        Procesamiento de los argumentos con los que se ejecutó el script
    """
    parser = argparse.ArgumentParser(
        description="Script para obtener el número de post por hora en Elastic")
    parser.add_argument("-e", "--elasticsearch", default="http://localhost:9200",
                        help="dirección del servidor Elasticsearch")
    parser.add_argument("-i", "--index", help="nombre del índice a procesar", required=True)
    parser.add_argument("-o", "--output", default="hourly_posts.csv",
                        help="Archivo donde se almacenarán los resultados")
    return parser.parse_args()


if __name__ == "__main__":
    main(parse_args())
