"""
    Script para obtener la fecha que contiene el 80% de documentos de un índice de Elasticsearch
    --------------------------------------------------------------------------------------------
    Parámetros
    ----------
    * index: Índice de Elasticsearch a tratar  
    * -e, --elasticsearch: Dirección del servidor Elasticsearch. Por defecto: http://localhost:9200  
"""

import argparse
from elasticsearch import Elasticsearch
from datetime import datetime as dt

__author__ = "Samuel Cifuentes García"

global total_docs
global es

def main(args):
    global total_docs, es
    es = Elasticsearch(timeout=10000)

    # Número total de documentos en el índice
    # res = es.count(index=args.index)
    # Si queremos calcular la fecha para un subconjunto del índice usamos una consulta
    res = es.count(index=args.index,
        body={
            "query":{
                "match":{
                    "lonely": True
                }
            }
        }
    )

    total_docs = res["count"]

    # Extraer el timestamp mínimo y máximo de la colección
    res = es.search(index=args.index,
        body = {
            "size":0,
            # La query se usa si queremos sacar la fecha del 80% de una de las muestras en vez de la totalidad del índice
            "query":{
                "match":{
                    "lonely": True
                }
            },
            "aggs":{
                "oldest_date":{
                    "min":{
                        "field": "created_utc"
                    }
                },
                "newest_date":{
                    "max": {
                        "field": "created_utc"
                    }
                }
            }
        }
    )
    oldest_date = int(res["aggregations"]["oldest_date"]["value"])
    newest_date = int(res["aggregations"]["newest_date"]["value"])

    str_oldest_date = dt.utcfromtimestamp(oldest_date).strftime("%d-%m-%Y %H:%M:%S")
    str_newest_date = dt.utcfromtimestamp(newest_date).strftime("%d-%m-%Y %H:%M:%S")
    print("Fecha más antigua: " + str_oldest_date)
    print("Fecha más reciente: " + str_newest_date)

    # Se obtiene la fecha correspondiente al 80% de documentos
    cutoff_date = recursive(oldest_date, newest_date, args.index, 0.0001)
    
    print(f"Fecha con el 80% de los documentos : {dt.utcfromtimestamp(cutoff_date).strftime('%d-%m-%Y')} - Timestamp: {cutoff_date}")


def recursive(old_date, new_date, index, tolerance):
    """
        Busca recursivamente la fecha que contenga el 80% de documentos del índice

        Parámetros
        ----------
        old_date: int  
            \tTimestamp de la fecha más antigua del intervalo a tratar
        new_date: int  
            \tTimestamp de la fecha más reciente del intervalo a tratar
        index: str  
            \tNombre del índice sobre el que trabajar
        tolerance: float  
            \tTolerancia de la condición de parada. Por ejemplo, para una tolerancia de 0.1 se admitirá cualquier fecha que contenga entre el 79% y el 81% de documentos

        Salida
        ------
        int  
            \tTimestamp de la fecha objetivo
    """
    # Se toma la fecha intermedia entre las dos dadas como límites del intervalo y se cuentan los documentos que hay en ésta
    next_date = (old_date + new_date)//2
    res = es.count(index=index,
        body={
            "query": {
                "bool":{
                    "must": [
                        # La cláusula match se usa si queremos restringir la búsqueda a un subset del índice
                        {"match":{
                            "lonely": True
                        }},
                        {"range":{
                            "created_utc":{
                                "lte": next_date
                            }
                        }}
                    ]
                }
            }
        }
    )
    doc_count = res["count"]

    ratio = abs(doc_count/total_docs)
    print(f"\t Fecha: {dt.utcfromtimestamp(next_date).strftime('%d-%m-%Y')}, Porcentaje del dataset: {ratio}")
    
    # Condición de parada 
    if abs(ratio - 0.8) <= tolerance:
        return next_date
    # Si no se satisface la condición de parada, se busca en un intervalo más cerrado, utilizando la fecha intermedia como nuevo límite    
    elif ratio < 0.8:
        return recursive(next_date, new_date, index, tolerance)
    else:
        return recursive(old_date, next_date, index, tolerance)
    

def parse_args():
    """
        Procesamiento de los argumentos con los que se ejecutó el script
    """
    parser = argparse.ArgumentParser(
        description="Script para obtener la fecha que contiene el 80% de documentos de un índice de Elasticsearch")
    parser.add_argument("index", help="Índice de Elasticsearch")
    parser.add_argument("-e", "--elasticsearch", default="http://localhost:9200", help="Dirección del servidor Elasticsearch")

    return parser.parse_args()


if __name__ == "__main__":
    main(parse_args())