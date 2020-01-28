from psaw import PushshiftAPI
import argparse
from datetime import datetime

api = PushshiftAPI()

def main(args):
    queryAPI(args.query, args.scale)

def queryAPI(query, scale):
    gen = api.search_submissions(q=query)
    cache = []

    for c in gen:
        submission = c.d_
        submission["query"] = query
        submission["scale"] = scale
        submission["lonely"] = True
        cache.append(submission)

        if len(cache) == 1000:
            print(".", end="")
            
            dumpToFile(cache)
            elasticIndex(cache)

            cache = []

def dumpToFile(results):
    pass

def elasticIndex(results):
    pass

def parse_args():
    parser = argparse.ArgumentParser(description="Script para la extracción de submissions de Reddit a través de pushshift.io")
    parser.add_argument("query", help="Frase usada como query")
    parser.add_argument("scale", help="Escala a la que pertenece la frase")
    args = parser.parse_args()
    return args

main(parse_args())