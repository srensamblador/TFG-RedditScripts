from elasticsearch import Elasticsearch
import argparse
from datetime import datetime, timedelta

def main(args):
    global es
    es = Elasticsearch(args.elasticsearch)

    dates = get_boundary_dates()

    submissions_per_hour = []
    after_date = dates[1]
    while after_date > dates[0]:
        before_date = after_date - timedelta(hours=1) # Hora a hora
        
        curr_progress = (dates[1]-before_date)/(dates[1]-dates[0])*100
        if next(__progress(curr_progress)):
            print(curr_progress, "%")

        count = get_submission_count(before_date.timestamp(), after_date.timestamp())
        if count > 0:
            submissions_per_hour.append((before_date, after_date, count))

        after_date = before_date
    
    for tuple in submissions_per_hour:
        print(tuple)

def __progress(curr_progress):
    [(yield i) for i in range(0,100,10) if curr_progress > i]

def get_boundary_dates():
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
        index="reddit-loneliness",
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


def get_submission_count(before_timestamp, after_timestamp):
    query = {
        "query": {
            "range": {
                "created_utc": {
                    "gte": before_timestamp,
                    "lte": after_timestamp
                }
            }
        }
    }
    
    res = es.count(
        index="reddit-loneliness",
        body=query
    )
    return res["count"]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Script para obtener el número de post por hora en Elastic")
    parser.add_argument("-o", "--output", default="hourly_posts.csv",
                        help="Archivo donde se almacenarán los resultados")
    parser.add_argument("-e", "--elasticsearch", default="http://localhost:9200",
                        help="dirección del servidor Elasticsearch")
    return parser.parse_args()


if __name__ == "__main__":
    main(parse_args())
