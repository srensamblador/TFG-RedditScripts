import csv
from elasticsearch import Elasticsearch

es = Elasticsearch(timeout=10000)

res = es.search(index="reddit-loneliness",
                body={
                    "size": 0,
                    "query": {
                        "term": {
                            "lonely": True
                        }
                    },
                    "aggs": {
                        "significant_selftext": {
                            "significant_text": {
                                "field": "title",
                                "size": 1000,
                                "gnd": {}
                            }
                        }
                    }
                })

buckets = res["aggregations"]["significant_selftext"]["buckets"]
lista = []
for bucket in buckets:
    lista.append((bucket["key"], bucket["score"],
                  bucket["doc_count"], bucket["bg_count"]))

with open("significant_selftext.csv", "w", encoding="UTF-8") as f:
    f.write("Subreddit,GND,doc_count,bg_count\n")
    [f.write(",".join(map(str, t)) + "\n") for t in lista]
