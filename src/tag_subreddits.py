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
import argparse
import json
import os
from elasticsearch import Elasticsearch
import requests
import time

__author__ = "Samuel Cifuentes García"

def main(args):
    raw_subreddits = load_subreddits(args.source)

    tagged_subreddits = tag_subreddits(raw_subreddits, args.num_members)

    for t in tagged_subreddits:
        print(t)

def tag_subreddits(subreddits, threshold):
    tagged = []
    for subreddit in subreddits:
        r = requests.get("http://reddit.com/r/" + subreddit + "/about.json",
        headers={"User-agent": "subscriber-count 0.1"})
        sub_data = r.json()
        print(subreddit, r.status_code)
        subscribers = sub_data["data"]["subscribers"]
        tagged.append((subreddit, subscribers, subscribers > threshold))
        time.sleep(2)

    return tagged

def load_subreddits(filename):
    """
    Carga una lista de subreddits desde un fichero de texto. 
    El formato del fichero debe consistir en un fichero por línea.
    
    Parámetros
    ----------
    filename: str
        Nombre del fichero
    """
    subreddits = []
    with open(filename) as f:
        for line in f:
            subreddits.append(line.strip())
    return subreddits

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