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
import progressbar as pb
from src.utils import file_handler

__author__ = "Samuel Cifuentes García"

def main(args):
    raw_subreddits = file_handler.list_from_file(args.source)
    print("Marcando subreddits...")
    tagged_subreddits = tag_subreddits(raw_subreddits, args.num_members)
    print("Completado")
    file_handler.write_to_csv(args.output, tagged_subreddits, header=["Subreddit","Subscriptores","Etiqueta"])

def tag_subreddits(subreddits, threshold):
    """
        Etiqueta una lista de subreddits en función de su número de subscriptores.  
        Se devuelve una lista de tuplas, con una tupla por subreddit, donde se incluye el nombre,
        el número de subscriptores y un booleano indicando si supera el umbral establecido o no.  
        En caso de error al recuperar un subreddit (por ejemplo, el subreddit está baneado), se incluye el error
        y su código en vez del número de subscriptores y el booleano, para poder identificar a posteriori por qué ocurrió el
        error.

        Parámetros
        ----------
        subreddits: list of str
            Lista de subreddits a marcar
        threshold: int
            Número de miembros utilizado como umbral para marcar los subreddits.
    """
    tagged = []

    bar = pb.ProgressBar(max_value=len(subreddits)) # Para mostrar el progreso en ejecución
    for subreddit in bar(subreddits):
        r = requests.get("http://reddit.com/r/" + subreddit + "/about.json",
        headers={"User-agent": "subscriber.count:v0.1"}) # La API de Reddit limita fuertemente a agentes de usuario por defecto
        if r.status_code != 200: # Caso de error
            tagged.append((subreddit, "", "Error: " + str(r.status_code)))
        else:
            sub_data = r.json()
            subscribers = sub_data["data"]["subscribers"]
            if subscribers == None:
                subscribers = 0
            tagged.append((subreddit, subscribers, subscribers > threshold))
        time.sleep(2)

    return tagged

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