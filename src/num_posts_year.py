from psaw import PushshiftAPI
import datetime as dt
import time
import json
import argparse
import progressbar as pb
import requests
from src.utils import file_handler

def main(args):
    subreddits = file_handler.list_from_file(args.source)
    posts_per_sub = get_num_posts(subreddits, int(args.year))
    file_handler.write_to_csv(args.output, posts_per_sub, header=["Subreddit","NumPosts"])

def get_num_posts(subreddits, year):
    posts_per_sub = []

    start_timestamp = dt.datetime(year=year, month=1, day=1).timestamp()
    end_timestamp = dt.datetime(year=year, month=12, day=31).timestamp()

    bar = pb.ProgressBar(max_value=len(subreddits))
    for subreddit in bar(subreddits):           
        r = requests.get("https://api.pushshift.io/reddit/search/submission/?subreddit="+ subreddit
         +"&metadata=true&size=0&before="+ str(int(end_timestamp)) +"&after=" + str(int(start_timestamp)))
        if r.status_code == 200:
            data = r.json()
            posts_per_sub.append((subreddit, data["metadata"]["total_results"]))
        time.sleep(2)
        
    return posts_per_sub


def parse_args():
    """
        Procesamiento de los argumentos con los que se ejecutó el script
    """
    parser = argparse.ArgumentParser(
        description="Script para obtenern el número de post por subreddit en un año especificado")        
    parser.add_argument("-y", "--year", help="Año para el que se extraerán los datos")
    parser.add_argument("-s", "--source", default="subreddits.txt",
                        help="Fichero con la lista de subreddits a procesar")
    parser.add_argument("-o", "--output", default="num_posts.csv",
                        help="Archivo donde se almacenarán los resultados")
    return parser.parse_args()

if __name__=="__main__":
    main(parse_args())

