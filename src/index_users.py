import csv
import gzip
from elasticsearch import Elasticsearch
from src.elastic_utils.elastic_indexers import UserIndexer
import os
import progressbar as pb
from psaw import PushshiftAPI

__author__ = "Samuel Cifuentes García"

CHUNK_SIZE = 500000

def main():
    es = Elasticsearch()
    indexer = UserIndexer(es, "reddit-users")
    if not indexer.index_exists():
        print("Creando índice " + indexer.index_name)
        indexer.create_index()

    global api
    api = PushshiftAPI()


    f = gzip.open("dataset-users/data.csv.gz", "rt")
    data = csv.reader(f)
    
    headers = next(data)

    print("Indexando...")
    bar = pb.ProgressBar()
    cache = []
    for line in bar(data):
        cache.append(line)

        if len(cache) >= CHUNK_SIZE:
            # authors = [line[headers.index("name")] for line in cache]
            # posts_per_author = query_api(authors)
            indexer.index_documents(cache)
            cache = []

def query_api(authors):
    gen = api.search_submissions(author=authors,aggs="author")

    aggs = next(gen)
    return aggs["author"]

if __name__=="__main__":
    main()