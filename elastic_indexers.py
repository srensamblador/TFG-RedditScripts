from elasticsearch import Elasticsearch
from elasticsearch import helpers


class Indexer:
    def __init__(self, connection, index_name, query, scale, lonely):
        self.es = connection
        self.index_name = index_name
        self.query = query
        self.scale = scale
        self.lonely = lonely

    def create_index(self):
        arguments = {
            "settings": {
                "index": {
                    "analysis": {
                        "analyzer": {
                            "default": {
                                "tokenizer": "standard",
                                "filter": ["lowercase", "filter_stopword"]
                            }
                        },
                        "filter": {
                            "filter_stopword": {
                                "type": "stop",
                                "stopwords": ["a", "about", "above", "after", "again", "against", "all", "also", "am", "an", "and", "another", "any", "are", "aren't", "as", "at", "back", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "even", "ever", "every", "few", "first", "five", "for", "four", "from", "further", "get", "go", "goes", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "high", "him", "himself", "his", "how", "how's", "however", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "just", "least", "less", "let's", "like", "long", "made", "make", "many", "me", "more", "most", "mustn't", "my", "myself", "never", "new", "no", "nor", "not", "now", "of", "off", "old", "on", "once", "one", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "put", "said", "same", "say", "says", "second", "see", "seen", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "since", "so", "some", "still", "such", "take", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "three", "through", "to", "too", "two", "under", "until", "up", "very", "was", "wasn't", "way", "we", "we'd", "we'll", "we're", "we've", "well", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "whether", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"]
                            }
                        }
                    }
                }
            }
        }
        self.es.indices.create(index=self.index_name, body=arguments)

        # Para poder utilizar agregaciones sobre los campos title y selftext es necesario activar fielddata
        arguments = {
            "properties": {
                "title": {
                    "type": "text",
                    "fielddata": "true",
                    "analyzer": "default"
                },
                "selftext": {
                    "type": "text",
                    "fielddata": "true",
                    "analyzer": "default"
                },
                "subreddit": {
                    "type": "text",
                    "fielddata": "true",
                    "analyzer": "default"
                },
                "query":{
                    "type": "keyword"
                },
                "scale":{
                    "type": "keyword"
                },
                "lonely":{
                    "type": "keyword"
                }
            }
        }

        self.es.indices.put_mapping(
            index=self.index_name, doc_type="post", body=arguments,  include_type_name=True)

    def index_documents(self, documents):
        toIndex = []

        for document in documents:
            ident = document["id"]

            document["_index"] = self.index_name
            document["_type"] = "post"
            # Genera una explosión de campos en otro caso
            document["media_metadata"] = None
            # Añadimos los siguientes campos
            document["query"] = self.query
            document["scale"] = self.scale
            document["lonely"] = self.lonely

            document["_id"] = ident

            toIndex.append(document)

        helpers.bulk(self.es, toIndex, chunk_size=len(
            toIndex), request_timeout=200)

    def index_exists(self):
        return self.es.indices.exists(index=self.index_name)


class NgramIndexer(Indexer):
    def create_index(self):
        arguments = {
            "settings": {
                "index": {
                    "analysis": {
                        "analyzer": {
                            "default": {
                                "tokenizer": "standard",
                                "filter": ["lowercase", "filter_stopwords"]
                            },
                            "analyzer_shingle": {
                                "tokenizer": "standard",
                                "filter": ["lowercase", "filter_stopwords", "filter_shingle"]
                            }
                        },
                        "filter": {
                            "filter_stopwords": {
                                "type": "stop",
                                "stopwords": ["a", "about", "above", "after", "again", "against", "all", "also", "am", "an", "and", "another", "any", "are", "aren't", "as", "at", "back", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "even", "ever", "every", "few", "first", "five", "for", "four", "from", "further", "get", "go", "goes", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "high", "him", "himself", "his", "how", "how's", "however", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "just", "least", "less", "let's", "like", "long", "made", "make", "many", "me", "more", "most", "mustn't", "my", "myself", "never", "new", "no", "nor", "not", "now", "of", "off", "old", "on", "once", "one", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "put", "said", "same", "say", "says", "second", "see", "seen", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "since", "so", "some", "still", "such", "take", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "three", "through", "to", "too", "two", "under", "until", "up", "very", "was", "wasn't", "way", "we", "we'd", "we'll", "we're", "we've", "well", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "whether", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"]
                            },
                            "filter_shingle": {
                                "type": "shingle",
                                "max_shingle_size": 2,
                                "min_shingle_size": 2,
                                "output_unigrams": "false"
                            }
                        }
                    }
                }
            }
        }
        self.es.indices.create(index=self.index_name, body=arguments)
        arguments = {
            "properties": {
                "title": {
                    "type": "text",
                    "fielddata": "true",
                    "analyzer": "analyzer_shingle"
                },
                "selftext": {
                    "type": "text",
                    "fielddata": "true",
                    "analyzer": "analyzer_shingle"
                },
                "subreddit": {
                    "type": "text",
                    "fielddata": "true",
                    "analyzer": "default"
                },
                "query":{
                    "type": "keyword"
                },
                "scale":{
                    "type": "keyword"
                },
                "lonely":{
                    "type": "keyword"
                }
            }
        }
        self.es.indices.put_mapping(
            index=self.index_name, doc_type="post", body=arguments,  include_type_name=True)
