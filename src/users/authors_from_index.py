"""
    Script utilizado para obtener una lista de usuarios a partir de una consulta de un índice de documentos, 
    y que crea un nuevo índice, de usuarios, con datos relativos a estos, obtenidos de la API de PushShift.

    Parámetros
    ----------
    * -s, --source: Nombre del índice de documentos original del que se extraerá la lista de usuarios
    * -d, --destination: Nombre del índice destino que se creará. Contendrá datos de usuarios.
    * -e, --elasticsearch: Dirección del servidor elastic contra el que indexar. Por defecto, http://localhost:9200
    * -b, --before: Fecha límite utilizada para obtener el número de posts de cada usuario. No se contarán posts creados
    posteriormente a dicha fecha.

"""

from elasticsearch import Elasticsearch
import pickle
from psaw import PushshiftAPI
import requests
import datetime
import progressbar as pb
import time
import argparse
from src.elastic_utils.elastic_indexers import UserIndexer


__author__= "Samuel Cifuentes García"


def main(args):
    global es, api
    es = Elasticsearch(args.elasticsearch)
    api = PushshiftAPI()

    # Extraer lista de autores en el índice
    print("Obteniendo lista de autores...")
    authors = get_authors(args.source)
    
    # Obtener datos de los autores
    print("Obteniendo datos de los autores...")
    to_index = get_user_data(authors, args.before)

    # Indexar
    print("Indexando documentos...")
    indexer = UserIndexer(es, args.destination)
    if not indexer.index_exists():
        indexer.create_index()

    headers = ("id", "name", "created_utc", "updated_on", "comment_karma", "link_karma", "posts")
    indexer.index_documents(to_index, headers)

def get_authors(index):
    """
        Obtiene la lista de usuarios del índice de Elasticsearch indicado que han creado algún post en /r/lonely

        Parámetros
        ----------
        index: str  
            \tÍndice del que recuperar los usuarios

        Salida
        ------
        list  
            \tLista de autores resultantes
    """
    authors = []
    after = ""

    finished = False
    while not finished:
        res = es.search(index=index,
                body={
                    "size": 0,
                    "query":{
                        "match":{
                            "lonely": True
                        }
                    },
                    "aggs":{
                        "authors": {
                            "composite": {
                                "sources": [
                                {
                                    "author": {
                                        "terms": {
                                            "field": "author"
                                        }
                                    }
                                }
                                ],
                                "after": {
                                    "author": after
                                },
                                "size": 1000
                            }
                            }
                    }
                }
            )

        # Se utiliza https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-composite-aggregation.html para
        # poder paginar por los resultados de una agregación de Terms
        new_authors = [bucket["key"]["author"] for bucket in res["aggregations"]["authors"]["buckets"]]
        authors += new_authors

        # El after_key se utiliza para paginar, corresponde con el último resultado de cada página
        if "after_key" in res["aggregations"]["authors"]:
            after = res["aggregations"]["authors"]["after_key"]["author"]
        else:
            finished=True

    return authors

def get_user_data(authors, before_date):
    """
        Obtiene la fecha de creación, actualización, karma (posts y comentarios) y número de posts de una lista
        de usuarios

        Parámetros
        ----------
        authors: list  
            \tLista de nombres de usuario
        before_date: int  
            \tTimestamp de la fecha límite hasta la cuál se obtendrá el número de posts de cada usuario
        
        Salida
        ------
        list  
            Lista de usuarios junto con sus datos
    """
    users=[]
    
    step = 100 # Se piden 'step' usuarios simultáneamente
    id = 0
    # Barra de progreso
    bar = pb.ProgressBar(widgets=[pb.Percentage(), "   ", pb.Bar(), pb.Timer(), "   ", pb.ETA()])
    for i in bar(range(0,len(authors), step)):
        j = min(i+step, len(authors))

        # Llamada a la API para obtener todos los datos necesarios excepto el número de posts
        r = requests.get("http://api.pushshift.io/reddit/author/lookup?author="+ ",".join(authors[i:j]))
        if r.status_code != 200:
            print("Error", r.status_code)
        authors_data = r.json()["data"]

        # Se obtiene el número de posts de cada autor
        gen = api.search_submissions(author=authors[i:j], aggs="author", size=0, before=before_date)
        authors_posts = next(gen)["author"]

        for user in authors_data:
            # Busca el número de posts en la salida de la segunda consulta
            user["posts"] = [item["doc_count"] for item in authors_posts if item["key"] == user["author"]][0]
            
            users.append(
                (id, user["author"], user["created_utc"], user["updated_utc"],
                user["comment_karma"], user["link_karma"], user["posts"])
            )
            id +=1
        
        time.sleep(0.5)
    return users

def parse_args():
    """
        Procesamiento de los argumentos con los que se ejecutó el script
    """
    parser = argparse.ArgumentParser(description="Script para obtener los usuarios que postearon en un subreddit, obtener sus datos e indexarlos en un nuevo índice")
    parser.add_argument("-s", "--source", default="subreddit-lonely", help="Nombre del índice de Elasticsearch del que se recuperaran los usuarios")
    parser.add_argument("-d", "--destination", default="users-r-lonely", help="Nombre del índice de Elasticsearch en el que se indexaran los usuarios junto con sus datos")
    parser.add_argument("-e", "--elasticsearch", default="http://localhost:9200", help="dirección del servidor Elasticsearch")
    parser.add_argument("-b", "--before", default=datetime.date.today(), 
        type= lambda d: datetime.datetime.strptime(d, '%Y-%m-%d').date(), 
        help="Fecha límite para obtener el número de posts de los usuarios")
    return parser.parse_args()

if __name__== "__main__":
    main(parse_args())