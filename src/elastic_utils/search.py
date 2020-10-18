"""
    Script que realiza una consulta contra un servidor de Elasticsearch y genera un .csv
    con los resultados
"""

import csv
from elasticsearch import Elasticsearch

def main():
    es = Elasticsearch(timeout=10000)
    
    print("Lanzando consulta...")
    res = es.search(index="phase-b",
                    body={
                        "size": 0,
                        "query": {
                            "term": {
                                "lonely": True
                            }
                        },
                        "aggs": {
                            "significant_subreddits": {
                                "significant_terms": {
                                    "field": "subreddit",
                                    "size": 1000,
                                    "gnd": {}
                                }
                            }
                        }
                    })

    buckets = res["aggregations"]["significant_subreddits"]["buckets"]
    lista = []
    for bucket in buckets:
        lista.append((bucket["key"], bucket["score"],
                    bucket["doc_count"], bucket["bg_count"]))
    
    print("Volcando resultados...")
    with open("phase-b-sign-subreddits.csv", "w", encoding="UTF-8") as f:
        f.write("Subreddit,GND,doc_count,bg_count\n")
        [f.write(",".join(map(str, t)) + "\n") for t in lista]

if __name__=="__main__":
    main()