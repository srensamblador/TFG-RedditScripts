"""
    Script para la extracción e indexado de posts de Reddit a partir de una lista de usuarios
    -----------------------------------------------------------------------------------------

    Recupera el histórico de posts de una lista de usuarios y "gemelos" previos a la fecha especificada
    a través de la API de Pushshift.
    Los documentos se indexan en un servidor Elasticsearch. Los posts se marcan con un campo booleano
    en función de si pertenecen a usuarios o a sus "gemelos".
    Los posts se vuelcan en un fichero .ndjson además de ser indexados, para poseer un backup.

    El fichero de entrada es un .csv donde cada fila es un usuario. Este csv debe tener al menos los campos
    "Usuario" y "Muestra", para poder extraer el nombre de los usuarios y etiquetar sus posts de forma adecuada.

    Parámetros
    ----------
    * -u, --users: fichero .csv con la lista de usuarios
    * -d, --dump-dir: directorio donde se volcará el fichero .ndjson. Por defecto /dumps
    * -s, --subreddits: Fichero con los subreddit a excluir
    * -e, --elasticsearch: dirección del servidor Elasticsearch contra el que se indexará. Por defecto http://localhost:9200
    * -b, --before: fecha donde se comenzará a extraer posts hacia atrás en el tiempo. Por defecto, la fecha actual.

"""
from psaw import PushshiftAPI
import argparse
from datetime import datetime as dt
from datetime import date
import json
import os
from elasticsearch import Elasticsearch
from src.elastic_utils.elastic_indexers import Indexer, NgramIndexer
import progressbar as pb

__author__ = "Samuel Cifuentes García"


def main(args):
    # Establecemos conexión a Elastic
    global es
    es = Elasticsearch(args.elasticsearch)

    # Inicializamos el cliente de la API
    global api
    api = PushshiftAPI()

    print("Cargando usuarios...")
    users = load_users(args.users)

    if args.subreddits:
        print("Cargando subreddits a excluir...")
        subreddits = load_subreddits(args.subreddits)
    else:
        subreddits = ["lonely"]
    
    if not os.path.exists(args.dump_dir):
        os.makedirs(args.dump_dir)
    dump_filename = args.dump_dir + "/user_posts-Dump.ndjson"

    print("Obteniendo e indexando posts...")
    query_api(users, args.before, subreddits, filename = dump_filename)


def load_users(path):
    """
        Carga los usuarios desde un .csv y crea un diccionario con sus nombres de usuario
        y la muestra a la que pertenecen

        Parámetros
        ----------
        path: str  
            \tRuta del archivo .csv con los usuarios

        Salida
        ------
        dict  
            \tDiccionario con los nombres de usuario y sus respectivas muestras
    """
    users = {}
    with open(path) as f:
        headers = next(f).split(";")
        index_name = headers.index("Usuario")
        index_group = headers.index("Muestra") # Necesitamos este campo para marcar el usuario como control o lonely

        for line in f:
          data = line.split(";")
          users[data[index_name]] = data[index_group]
    return users  

def query_api(users, before_date, subreddits, filename="dumps/user_posts-Dump.ndjson", cache_size=10000):
    """
        Recupera los post de una lista de usuarios, los indexa en Elastic y vuelca a un fichero a modo
        de backup.  
        Se excluyen los posts que pertenezcan añ subreddit pasados por parámetro.

        Parámetros
        ----------
        users: dict  
            \tDiccionario con nombres de usuario y muestras a las que pertenecen  
        before_date: date  
            \tFecha límite tras la cuál no se extraerán más documentos  
        subreddit: str
            \tNombre del subreddit a excluir  
        filename: str  
            \tNombre del fichero donde se volcarán los documentos  
        cache_size: int  
            \tTamaño de los bloques de documentos que se indexarán de cada vez. Regular en función
            de la memoria disponible
    """
    # Inicializar el indexer
    subreddit_filter = {
        "subreddit": subreddits
    }

    indexer = Indexer(es, "phase-b", filter_criteria=subreddit_filter)
    if not indexer.index_exists():
        print("Creado índice: " + indexer.index_name)
        indexer.create_index()

    # Barra de progreso
    bar = pb.ProgressBar(max_value=pb.UnknownLength, widgets=[
        "- ", pb.AnimatedMarker(), " Docs processed: ", pb.Counter(), " ", pb.Timer()
    ])
    num_iter = 0
    # Ahorramos tiempo de máquina consultando a la API de 100 en 100 usuarios
    # Más no es viable porque superamos los límites de caracteres de la petición GET
    user_block = []
    for user in users:
        user_block.append(user)

        if len(user_block) == 100:
            # Se extraen todos los posts del usuario
            gen = api.search_submissions(author=user_block, before=int(before_date.timestamp()))

            # El tamaño de la caché dependerá de la memoria que tengamos
            cache = []
            for c in gen:
                # Usamos el campo muestra del .csv para marcar los posts como lonely o no
                c.d_["lonely"] = users[c.d_["author"]] == "lonely"        
                cache.append(c.d_)

                if len(cache) == cache_size:      
                    dump_to_file(cache, filename)
                    indexer.index_documents(cache)
                    
                    cache = []
                
                # Actualizar barra
                num_iter += 1
                bar.update(num_iter)                

            # Los restantes al salir del bucle
            dump_to_file(cache, filename)
            indexer.index_documents(cache)

            user_block = []
            
    print("\t*%s - Indexed: %d, Errors:%d, Filtered:%d"%(indexer.index_name, indexer.stats["indexed"], indexer.stats["errors"], indexer.stats["filtered"]))

def load_subreddits(path):
    """
        Carga una lista de subreddits de un archivo en disco.
        El archivo debe ser un fichero de texto con un subreddit en cada línea.

        Parámetros
        ----------
        path: str  
            \tRuta del fichero con los subreddits

        Salida
        ------
        list  
            \tLista de subreddits
    """
    subreddits = []
    with open(path) as f:
        for sub in f:
            subreddits.append(sub.strip())
    return subreddits


def dump_to_file(results, path):
    """
        Vuelca una lista de submissions a un fichero .ndjson. Los volcados se deben realizar de forma parcial 
        debido a limitaciones de memoria.
        Las escrituras son mucho más rápidas si se tratan como strings en vez de objetos JSON.

        Parámetros
        ----------
        results: list
            lista de documentos a volcar   
    """
    with open(path, "a") as f:
        for result in results:
            f.write(json.dumps(result) + "\n")

def parse_args():
    """
        Procesamiento de los argumentos con los que se ejecutó el script
    """
    parser = argparse.ArgumentParser(description="Script para la extracción de submissions de Reddit a partir de una lista de usuarios")
    parser.add_argument("-u", "--users", default="csv/users_data.csv", help="Fichero con los usuarios")
    parser.add_argument("-d", "--dump-dir", default="dumps", help="Directorio donde se volcará el archivo .ndjson de backup")
    parser.add_argument("-s", "--subreddits", help="Fichero con los subreddits a excluir")
    parser.add_argument("-e", "--elasticsearch", default="http://localhost:9200", help="Dirección del servidor Elasticsearch contra el que se indexará")
    parser.add_argument("-b", "--before", default=dt.now(), type= lambda d: dt.strptime(d + " 23:59:59", '%Y-%m-%d %H:%M:%S'),
        help="Fecha desde la que se empezará a recuperar documentos hacia atrás en formato YYYY-mm-dd")
    return parser.parse_args()

if __name__ == "__main__":
    main(parse_args())