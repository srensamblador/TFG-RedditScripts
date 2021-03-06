from elasticsearch import Elasticsearch
from elasticsearch import helpers
import progressbar as pb

__author__ = "Samuel Cifuentes García"


class Indexer:
    """
        Clase encargada de crear un índice en Elasticsearch e indexar documentos.

        Opcionalmente se le puede pasar un filtro en forma de diccionario con la siguiente
        estructura: 
        ```json
        {
            "campo1": ["valor1", "valor2", "valor3"],
            "campo2": ["valorA", "valorB", ...],
            ...
        }

        Aquellos documentos donde alguno de los campos coincida con uno de los valores incluidos 
        serán excluidos
        ```

        Atributos
        ---------
        es: Elasticsearch
            \tConexión a un servidor Elastic
        index_name: str
            \tNombre del índice con el que trabajará el indexer
        filter_criteria: dict  
            \tDiccionario con los filtros a aplicar. Opcional.
    """

    def __init__(self, connection, index_name, filter_criteria=None):
        self.es = connection
        self.index_name = index_name
        self.stats = {"indexed": 0, "errors": 0, "filtered": 0}
        self.filter = filter_criteria

    def create_index(self):
        """
            Crea un índice de unigramas.
            Se filtran palabras vacías.
            Se habilitan los campos título, selftext, subreddit así como los campos añadidos (query, scale y lonely) como fielddata/keyword
            para poder ser utilizados en agregaciones.
        """

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
            }}

        self.es.indices.create(index=self.index_name,
                               body=arguments)

        mappings = {
            "dynamic": False,
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
                    "type": "keyword"
                },
                "author": {
                    "type": "keyword"
                },
                "category": {
                    "type": "keyword"
                },
                "content_categories": {
                    "type": "keyword"
                },
                "created_utc": {
                    "type": "long"
                },
                "domain": {
                    "type": "keyword"
                },
                "gilded": {
                    "type": "long"
                },
                "num_comments": {
                    "type": "long"
                },
                "over_18": {
                    "type": "boolean"
                },
                "permalink": {
                    "type": "keyword"
                },
                "removal_reason": {
                    "type": "keyword"
                },
                "report_reasons": {
                    "type": "keyword"
                },
                "subreddit_id": {
                    "type": "keyword"
                },
                "subreddit_type": {
                    "type": "keyword"
                },
                "url": {
                    "type": "keyword"
                },
                "query": {
                    "type": "keyword"
                },
                "scale": {
                    "type": "keyword"
                },
                "lonely": {
                    "type": "keyword"
                }
            }
        }
        self.es.indices.put_mapping(
            index=self.index_name, doc_type="post", body=mappings,  include_type_name=True)

    def index_documents(self, documents):
        """
            Indexa una lista de posts de Reddit. Nos quedamos con los campos que nos interesan.
            Si el indexer se instanción con filtros, excluimos aquellos documentos afectados.
        """
        toIndex = []
        # Lista de campos que queremos conservar
        fields = ["author", "category", "content_categories", "created_utc", "domain", "downs", "gilded", "likes",
                  "name", "num_comments", "num_reports", "over_18", "permalink", "post_categories", "removal_reason", "report_reasons",
                  "retrieved_on", "score", "selftext", "selftext_html", "subreddit", "subreddit_id", "subreddit_type", "title",
                  "ups", "url", "user_reports", "query", "scale", "lonely"]

        for document in documents:
            # Si se instanció el indexer con filtros, los aplicamos aquí
            filter_flag = False # Indica si el post se debe excluir o no
            if self.filter:
                # El filtro es un diccionario con campos, donde para cada campo se incluye
                # la lista de valores a excluir.
                '''
                    Ejemplo de filtro. Aquellos posts donde algún valor de los campos indicados
                    encaje con los valores de las listas correspondientes serán excluidos.
                    self.filter = {
                        "subreddit": ["lonely", "loneliness", "ForeverAlone"],
                        "author": ["pepito", "manolito"]
                    }
                '''
                for field in self.filter:
                    if not filter_flag and document.get(field) in self.filter[field]:
                        filter_flag = True
                        self.stats["filtered"] += 1

            # Si no se excluye el post lo seguimos procesando            
            if not filter_flag:
                processed_post = {
                    "_index": self.index_name,
                    "_type": "post",
                    "_id": document.get("id")
                }

                for field in fields:
                    processed_post[field] = document.get(field)

                toIndex.append(processed_post)

        bulk_stats = helpers.bulk(self.es, toIndex, chunk_size=len(
            toIndex), request_timeout=200, raise_on_error=False)
        self.stats["indexed"] += bulk_stats[0]
        self.stats["errors"] += len(bulk_stats[1])

    def index_exists(self):
        """
            Indica si el índice ya está presente en el servidor Elastic
        """
        return self.es.indices.exists(index=self.index_name)


class NgramIndexer(Indexer):
    """
        Clase heredera de Indexer, implementa un índice de bigramas.
    """

    def create_index(self):
        """
            Crea un índice de n-gramas
            Mantiene el filtrado de palabras vacías
            Añade un analizador para tal fin
        """
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
            "dynamic": False,
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
                    "type": "keyword"
                },
                "author": {
                    "type": "keyword"
                },
                "category": {
                    "type": "keyword"
                },
                "content_categories": {
                    "type": "keyword"
                },
                "created_utc": {
                    "type": "long"
                },
                "domain": {
                    "type": "keyword"
                },
                "gilded": {
                    "type": "long"
                },
                "num_comments": {
                    "type": "long"
                },
                "over_18": {
                    "type": "boolean"
                },
                "permalink": {
                    "type": "keyword"
                },
                "removal_reason": {
                    "type": "keyword"
                },
                "report_reasons": {
                    "type": "keyword"
                },
                "subreddit_id": {
                    "type": "keyword"
                },
                "subreddit_type": {
                    "type": "keyword"
                },
                "url": {
                    "type": "keyword"
                },
                "query": {
                    "type": "keyword"
                },
                "scale": {
                    "type": "keyword"
                },
                "lonely": {
                    "type": "keyword"
                }
            }
        }
        self.es.indices.put_mapping(
            index=self.index_name, doc_type="post", body=arguments, include_type_name=True)

class UserIndexer(Indexer):
    """
        Clase heredera de Indexer que se especializará en índices de usuarios de Reddit
    """

    def create_index(self):
        """
            Crea un índice de usuarios, incluyendo mapeo de los campos necesarios
        """
        self.es.indices.create(index=self.index_name)
        mappings = {
            "dynamic": False,
            "properties": {
                "name": {
                    "type": "keyword"
                },
                "created_utc":{
                    "type":"long"
                },
                "updated_on":{
                    "type":"long"
                },
                "comment_karma":{
                    "type":"long"
                },
                "link_karma":{
                    "type":"long"
                },
                "posts":{
                    "type":"long"
                }
            }
        }
        self.es.indices.put_mapping(
            index=self.index_name, body=mappings)

    def index_documents(self, documents, fields):
        """
            Indexa documentos en el índice.

            Parámetros
            ----------
            documents: list
                \tLista de documentos a indexar
            fields: list
                \tLista con los campos a indexar
        """
        toIndex = []
        
        for document in documents:
            processed_user = {
                "_index": self.index_name,
                "_id": document[fields.index("id")]
            }
            for field in fields[1:]:
                processed_user[field] = document[fields.index(field)]
        
            toIndex.append(processed_user)
            
        bulk_stats = helpers.bulk(self.es, toIndex, chunk_size=len(
            toIndex), request_timeout=200, raise_on_error=False)
        self.stats["indexed"] += bulk_stats[0]
        self.stats["errors"] += len(bulk_stats[1])
