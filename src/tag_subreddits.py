"""
    Script para marcar subreddits en función de un número de miembros
    -----------------------------------------------------------------
    
    A partir de una lista de subreddits separados por saltos de línea en un fichero de texto, 
    procesa cada subreddit anotándolo como True or False en función de sí superan el número de 
    miembros indicado por parámetro.

    Parámetros
    ----------
    -s, --source: fichero con la lista de subreddits a tratar
    -o, --output: fichero donde se almacenarán los resultados
    -n, --num-members: número de miembros que sirve de umbral para el marcado
"""
from psaw import PushshiftAPI
import argparse
import json
import os
from elasticsearch import Elasticsearch

__author__ = "Samuel Cifuentes García"

def main(args):
    pass

def parse_args():
    """
        Procesamiento de los argumentos con los que se ejecutó el script
    """
    parser = argparse.ArgumentParser(
        description="Marca una lista de subreddits en función de si superan un número de miembros")
    parser.add_argument("-s", "--source", default="subreddits.txt",
                        help="Lista de subreddits a marcar")
    parser.add_argument("-o", "--output", default="tagged_subreddits.csv",
                        help="Archivo donde se almacenará el resultado")
    parser.add_argument("-n", "--num-members", default=100, help="Número de miembros que sirve de umbral para el marcado")
    return parser.parse_args()

if __name__ == "__main__":
    main(parse_args())