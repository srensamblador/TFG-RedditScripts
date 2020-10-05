from elasticsearch import Elasticsearch

__author__= "Samuel Cifuentes García"


def main():
    global es
    es = Elasticsearch()

    authors = get_authors()    
    print(len(authors))

def get_authors():
    authors = []
    after = ""

    finished = False
    while not finished:
        res = es.search(index="subreddit-lonely",
                body={
                    "size": 0,
                    "query":{
                        "match":{
                            "lonely": True
                        }
                    },
                    "aggs":{
                        "authors": {
                            "composite": {
                                "sources": [
                                {
                                    "author": {
                                        "terms": {
                                            "field": "author"
                                        }
                                    }
                                }
                                ],
                                "after": {
                                    "author": after
                                },
                                "size": 1000
                            }
                            }
                    }
                }
            )

        new_authors = [bucket["key"]["author"] for bucket in res["aggregations"]["authors"]["buckets"]]
        authors += new_authors

        if "after_key" in res["aggregations"]["authors"]:
            after = res["aggregations"]["authors"]["after_key"]["author"]
        else:
            finished=True

    return authors

if __name__== "__main__":
    main()