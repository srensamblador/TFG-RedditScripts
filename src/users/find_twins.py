import argparse
from elasticsearch import Elasticsearch, helpers
from psaw import PushshiftAPI
import datetime
import progressbar as pb
import pprint as pp

def main(args):
    global es, api
    es = Elasticsearch(args.elasticsearch)
    
    # Extraer usuarios del primer índice
    print("Recuperando usuarios de /r/lonely...")
    users = get_users(args.source_users)

    # Buscar usuarios "gemelos"
    print("Obteniendo posibles gemelos...")
    bar = pb.ProgressBar()
    for username in bar(users):
        print("Procesando: " + username + "...")
        find_twins(users[username], args.user_index)
        print(username, len(users[username]["could_be_twins"]))


    # Filtrar por número de posts

    # Escoger el mejor
    pp.pprint(users)

    pass

def get_users(index):
    res = helpers.scan(es, index=index)
    
    users = {}
    bar = pb.ProgressBar()
    for user in bar(res):
        data = user["_source"]
        users[data["name"]] = {
            "created_utc": data["created_utc"],
            "comment_karma": data["comment_karma"],
            "link_karma": data["link_karma"],
            "posts": data["posts"],
            "could_be_twins": []
        }
    return users

def find_twins(user, index):
    MONTH_IN_SECONDS = 30*24*3600 
    bounds = {
        "created_old": user["created_utc"] - MONTH_IN_SECONDS,
        "created_new": user["created_utc"] + MONTH_IN_SECONDS,
        "comment_low": user["comment_karma"] - user["comment_karma"]*0.1,
        "comment_high": user["comment_karma"] + user["comment_karma"]*0.1,
        "link_low": user["link_karma"] - user["link_karma"]*0.1,
        "link_high": user["link_karma"] + user["link_karma"]*0.1
    }
    res = helpers.scan(es, index = index,
        query = {
            "query":{
                "bool":{
                    "must":[
                        {
                            "range":{
                                "created_utc":{
                                    "gte": bounds["created_old"],
                                    "lte": bounds["created_new"]
                                }
                            }
                        },
                        {
                            "range":{
                                "comment_karma":{
                                    "gte": bounds["comment_low"],
                                    "lte": bounds["comment_high"]
                                }
                            }
                        },
                        {
                            "range":{
                                "link_karma":{
                                    "gte": bounds["link_low"],
                                    "lte": bounds["link_high"]
                                }
                            }
                        }
                    ]
                }

            }
        }
    )

    for hit in res:
        user["could_be_twins"].append(hit["_source"]["name"])

def parse_args():
    """
        Procesamiento de los argumentos con los que se ejecutó el script
    """
    parser = argparse.ArgumentParser(description="Script para obtener usuarios 'gemelos'")
    parser.add_argument("-s", "--source-users", default="users-r-lonely", help="Nombre del índice de Elasticsearch del que se recuperaran los usuarios de los que se quieren encontrar gemelos")
    parser.add_argument("-u", "--user-index", default="users-reddit", help="Nombre del índice de Elasticsearch con los usuarios candidatos a ser gemelos")
    parser.add_argument("-e", "--elasticsearch", default="http://localhost:9200", help="dirección del servidor Elasticsearch")
    parser.add_argument("-b", "--before", default=datetime.date.today(), 
        type= lambda d: datetime.datetime.strptime(d, '%Y-%m-%d').date(), 
        help="Fecha límite para obtener el número de posts de los usuarios")
    return parser.parse_args()

if __name__=="__main__":
    main(parse_args())