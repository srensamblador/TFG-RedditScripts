from psaw import PushshiftAPI
import argparse
from datetime import datetime as dt
from datetime import date
import json
import os
from elasticsearch import Elasticsearch
from src.elastic_utils.elastic_indexers import Indexer, NgramIndexer
import progressbar as pb

__author__ = "Samuel Cifuentes García"


def main(args):
    # Establecemos conexión a Elastic
    global es
    es = Elasticsearch(args.elasticsearch)

    # Inicializamos el cliente de la API
    global api
    api = PushshiftAPI()

    users = load_users(args.users)
    
    if not os.path.exists(args.dump_dir):
        os.makedirs(args.dump_dir)
    dump_filename = args.dump_dir + "/user_posts-Dump.ndjson"

    query_api(users, args.before, filename = dump_filename)


def load_users(path):
    users = {}
    with open(path) as f:
        headers = next(f).split(";")
        index_name = headers.index("Usuario")
        index_group = headers.index("Muestra")

        for line in f:
          data = line.split(";")
          users[data[index_name]] = data[index_group]
    return users  

def query_api(users, before_date, filename="dumps/user_posts-Dump.ndjson", cache_size=10000):
    # Barra de progreso
    widgets = [
            pb.Percentage(),
            " (", pb.SimpleProgress(), ") ",
            pb.Bar(), " ",
            pb.FormatLabel(""), " ",
            pb.Timer(), " ",
            pb.ETA(), " "
        ]
    bar = pb.ProgressBar(max_value=len(users), widgets=widgets)
    for user in bar(users):
        # Se muestra el usuario actual en la barra de progreso
        widgets[6] = pb.FormatLabel("User: " + user + " ")

        # Se extraen todos los posts del usuario
        gen = api.search_submissions(author=user, before=int(before_date.timestamp()))

        cache = []
        for c in gen:
            c.d_["lonely"] = users[user] == "lonely"        
            cache.append(c.d_)

            if len(cache) == cache_size:      
                dump_to_file(cache, filename)
                elastic_index(cache)
                
                cache = []

        dump_to_file(cache, filename)
        elastic_index(cache)

def dump_to_file(results, path):
    """
        Vuelca una lista de submissions a un fichero .json. Los volcados se deben realizar de forma parcial 
        debido a limitaciones de memoria.
        Las escrituras son mucho más rápidas si se tratan como strings en vez de objetos JSON.

        Parámetros
        ----------
        results: list
            lista de documentos a volcar   
    """
    with open(path, "a") as f:
        for result in results:
            f.write(json.dumps(result) + "\n")

def elastic_index(results):
    """
        TODO
        Parámetros
        ----------
        results: list
            lista de documentos a indexar
    """
    indexer = Indexer(es, "phase-b")
    if not indexer.index_exists():
        print("Creado índice: " + indexer.index_name)
        indexer.create_index()
        
    indexer.index_documents(results)

def parse_args():
    """
        Procesamiento de los argumentos con los que se ejecutó el script
    """
    parser = argparse.ArgumentParser(description="Script para la extracción de submissions de Reddit a partir de una lista de usuarios")
    parser.add_argument("-u", "--users", default="users_data.csv", help="Fichero con los usuarios")
    parser.add_argument("-d", "--dump-dir", default="dumps", help="Directorio donde se volcarán los archivos .json de backup")
    parser.add_argument("-e", "--elasticsearch", default="http://localhost:9200", help="Dirección del servidor Elasticsearch contra el que se indexará")
    parser.add_argument("-s", "--subreddit", default="lonely", help="Subreddit a excluir de los resultados")
    parser.add_argument("-b", "--before", default=dt.now(), type= lambda d: dt.strptime(d + " 23:59:59", '%Y-%m-%d %H:%M:%S'),
        help="timestamp desde el que se empezará a recuperar documentos hacia atrás en formato YYYY-mm-dd")
    return parser.parse_args()

if __name__ == "__main__":
    main(parse_args())