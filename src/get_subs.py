import csv
from elasticsearch import Elasticsearch

es = Elasticsearch()

res = es.search(body = {
    "size": 0,
  "aggs": {
    "Lonely": {
      "filter": {
        "term": {
          "lonely": True
        }
      },
      "aggs": {
        "Subreddits": {
          "terms": {
            "field": "subreddit",
            "size": 100
          }
        }
      }
    }
  }
})

buckets = res["aggregations"]["Lonely"]["Subreddits"]["buckets"]
lista = []
for bucket in buckets:
    lista.append((bucket["key"], bucket["doc_count"]))

with open("docs_per_sub.csv", "w", encoding="UTF-8") as f:
    f.write("Subreddit;Número de documentos\n")
    [f.write(t[0] + ";" + str(t[1]) + "\n") for t in lista]
