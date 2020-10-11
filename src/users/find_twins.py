import argparse
from elasticsearch import Elasticsearch, helpers
from psaw import PushshiftAPI
from datetime import date
from datetime import datetime as dt
from datetime import timedelta
import progressbar as pb
import pprint as pp
import pickle

import requests


MAX_USERS = 1000

def main(args):
    global es, api
    es = Elasticsearch(args.elasticsearch)
    
    # Extraer usuarios del primer índice
    print("Recuperando usuarios de /r/lonely...")
    users = get_users(args.source_users)

    # Buscar usuarios "gemelos"
    print("Obteniendo posibles gemelos...")
    widgets = [
            pb.Percentage(),
            " (", pb.SimpleProgress(), ") ",
            pb.Bar(), " ",
            pb.FormatLabel(""), " ",
            pb.Timer(), " ",
            pb.ETA(), " "
        ]
    bar = pb.ProgressBar(max_value=len(users), widgets=widgets)
    i = 0
    for username in bar(users):
        if i > 100:
            break
        widgets[6] = pb.FormatLabel("User: " + username + " ")
        find_twins(users[username], args.user_index)
        i +=1
               
    with open("users_and_possible_twins.pickle", "wb") as f:
        pickle.dump(users, f)

    set_users = set()
    for user in users:
        for u in users[user]["possible_twins"]:
            set_users.add(u["name"])
    
    print(len(set_users))

    # Filtrar por número de posts

    # Escoger el mejor

    pass

def get_users(index):
    res = helpers.scan(es, index=index,
    query={
        "query":{
            "range":{
                "created_utc":{
                    "lt": 1539897781 # Fecha máxima en el índice de usuarios + 30 días
                }
            }
        }
    })
    
    users = {}
    bar = pb.ProgressBar()
    for user in bar(res):
        data = user["_source"]
        users[data["name"]] = {
            "created_utc": data["created_utc"],
            "comment_karma": data["comment_karma"],
            "link_karma": data["link_karma"],
            "posts": data["posts"],
            "possible_twins": []
        }
    return users

def find_twins(user, index):
    MONTH_IN_SECONDS = 30*24*3600 

    bounds = {
        "intervals": get_time_intervals(user["created_utc"]),
        "comment_low": user["comment_karma"] - user["comment_karma"]*0.1,
        "comment_high": user["comment_karma"] + user["comment_karma"]*0.1,
        "link_low": user["link_karma"] - user["link_karma"]*0.1,
        "link_high": user["link_karma"] + user["link_karma"]*0.1
    }

    num_hits = 0
    i = 0
    while num_hits < MAX_USERS and i < len(bounds["intervals"]):
        res = es.search(index = index,
            body = {
                "size": MAX_USERS,
                "query":{
                    "bool":{
                        "must":[
                            {
                                "range":{
                                    "created_utc":{
                                        "gte": bounds["intervals"][i]["date_old"],
                                        "lte": bounds["intervals"][i]["date_new"]
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
        num_hits = res["hits"]["total"]["value"]
        hits = res["hits"]["hits"]
        i+=1

    user["possible_twins"] = [hit["_source"] for hit in hits]
            
def get_time_intervals(timestamp):
    """
        Devuelve una lista de fechas en timestamp con los límites de los siguientes intervalos:
            * +/- 12 horas en el mismo día
            * +/- 24 horas
            * +/- 3.5 días en la misma semana
            * +/- 3 días
            * +/- 15 días en el mismo mes
            * +/- 30 días

        Parámetros
        ----------
        timestamp: int
            \t Timestamp de la fecha entorno a la cuál se calculan los intervalos

        Salida
        ------
        list
            \tLista con los timestamps correspondientes a los límites de los rangos
    """
    bounds = []
    
    date = dt.fromtimestamp(timestamp)

    # Intervalo 12 horas pero dentro del mismo día
    beginning_day = date.replace(hour=00, minute=00, second=00)
    end_day = date.replace(hour=23, minute=59, second=59)

    half_day_beginning = max(beginning_day, date - timedelta(hours=12)).timestamp()
    half_day_end = min(end_day, date + timedelta(hours=12)).timestamp()
    bounds.append({"date_old": half_day_beginning, "date_new": half_day_end })

    # Intervalo 24 horas
    bounds.append({"date_old": (date - timedelta(hours=24)).timestamp(),
        "date_new": (date + timedelta(hours=24)).timestamp()})

    # Intervalo 3.5 días pero en la misma semana
    ## Se calcula el primer día de la semana
    beginning_week = date - timedelta(days=date.weekday())
    beginning_week = beginning_week.replace(hour=00, minute=00, second=00)
    ## Se calcula el último
    end_week = beginning_week + timedelta(days=6)
    end_week = end_week.replace(hour=23, minute=59, second=59)
    ## Se calculan el rango de 3.5 días asegurándose de no salir de la semana
    half_week_beginning = max(beginning_week, date - timedelta(days=3.5)).timestamp()
    half_week_end = min(end_week, date + timedelta(days=3.5)).timestamp()
    bounds.append({"date_old": half_week_beginning, "date_new": half_week_end})

    # Intervalo 3 días
    bounds.append({"date_old": (date - timedelta(days=3)).timestamp(),
        "date_new": (date + timedelta(days=3)).timestamp()})

    # Intervalo 15 días pero dentro del mismo mes
    beginning_month = date.replace(day=1, hour=00, minute=00, second=00)
    ## Se saca el último día del mes
    next_month = date.replace(day=28) + timedelta(days=4)
    end_month = next_month - timedelta(days=next_month.day)
    end_month = end_month.replace(hour=23, minute=59, second=59)
    ## Se calcula el rango de 15 días sin salirse del mes
    half_month_beginning = max(beginning_month, date - timedelta(days=15)).timestamp()
    half_month_end = min(end_month, date + timedelta(days=15)).timestamp()
    bounds.append({"date_old": half_month_beginning, "date_new": half_month_end})

    # Intervalo 30 días
    bounds.append({"date_old": (date - timedelta(days=30)).timestamp(),
        "date_new": (date + timedelta(days=30)).timestamp()})
    

    return bounds

def parse_args():
    """
        Procesamiento de los argumentos con los que se ejecutó el script
    """
    parser = argparse.ArgumentParser(description="Script para obtener usuarios 'gemelos'")
    parser.add_argument("-s", "--source-users", default="users-r-lonely", help="Nombre del índice de Elasticsearch del que se recuperaran los usuarios de los que se quieren encontrar gemelos")
    parser.add_argument("-u", "--user-index", default="users-reddit", help="Nombre del índice de Elasticsearch con los usuarios candidatos a ser gemelos")
    parser.add_argument("-e", "--elasticsearch", default="http://localhost:9200", help="dirección del servidor Elasticsearch")
    parser.add_argument("-b", "--before", default=date.today(), 
        type= lambda d: dt.strptime(d, '%Y-%m-%d').date(), 
        help="Fecha límite para obtener el número de posts de los usuarios")
    return parser.parse_args()

if __name__=="__main__":
    main(parse_args())