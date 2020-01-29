from psaw import PushshiftAPI
import argparse
from datetime import datetime
import json
import os.path
import time

api = PushshiftAPI()

def main(args):
    global filename
    filename = "dumps/" + args.query.replace(" " ,"") + "-Dump.json"

    with open(filename, "w") as f:
        f.write("{\"data\": [")

    query_API(args.query, args.scale)

    with open(filename, "a") as f:
        f.write("]}")

def query_API(query, scale):
    gen = api.search_submissions(q=query)
    cache = []
    numIter = 0

    for c in gen:
        if numIter >= 4:
            break

        submission = c.d_
        submission["query"] = query
        submission["scale"] = scale
        submission["lonely"] = True
        cache.append(submission)

        if len(cache) == 3000:
            print(".", end="")
            
            dump_to_file(cache, numIter == 0)
            elastic_index(cache)

            cache = []
            numIter += 1

def dump_to_file(results, initial_dump):
    start_time = time.time()

    if initial_dump:
        delimiter = ""
    else:
        delimiter=","
    
    with open(filename, "a") as f:
        f.write(delimiter + json.dumps(results).strip("[").strip("]"))

    print(time.time() - start_time)

def elastic_index(results):
    pass

def parse_args():
    parser = argparse.ArgumentParser(description="Script para la extracción de submissions de Reddit a través de pushshift.io")
    parser.add_argument("query", help="Frase usada como query")
    parser.add_argument("scale", help="Escala a la que pertenece la frase")
    args = parser.parse_args()
    return args

main(parse_args())