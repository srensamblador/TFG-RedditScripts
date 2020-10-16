"""
    Script para encontrar los mejores "gemelos" de un conjunto de usuarios
    ----------------------------------------------------------------------
    A partir de los resultados de los scripts `find_possible_twins` y `posts_per_user` este script termina el proceso
    de selección de los "gemelos" más parecidos a los usuarios originales.
    En primer lugar aplica el criterio del número de posts (+/-10% de diferencia).
    Finalmente calcula el usuario más parecido al original de entre todos los candidatos, basándose en la fecha de creación,
    karma (posts y comentarios) y número de posts

    Parámetros
    ----------
    * -u, --users: Ruta del archivo .pickle con la salida del script `find_possible_twins`
    * -p, --posts: Ruta del .csv con posts por usuario obtenido de `posts_per_user`
    * -o, --output: Fichero .csv dónde se volcarán todos los datos de los usuarios y sus gemelos resultantes
    * --summary: Fichero .csv dónde se volcará la lista de usuarios con sus mejores "gemelos"
"""
import argparse
import pickle
import progressbar as pb
import numpy as np

__author__ = "Samuel Cifuentes García"

def main(args):
    print("Cargando usuarios...")
    with open(args.users, "rb") as f:
        users = pickle.load(f)
    
    print("Cargando números de post por posible candidato...")
    posts_per_twin = load_csv(args.posts)

    print("Filtrando candidatos en función del número de posts...")
    filter_twins(users, posts_per_twin)

    print("Seleccionando los mejores candidatos...")
    best_twins = find_best_twins(users)

    print("Volcando resultados...")
    dump_twin_summary(args.summary, best_twins)
    # dump_full_data(args.output, users, best_twins)

def filter_twins(users, posts_per_twin):
    """
        A partir de los usuarios y números de posts obtenidos en otros scripts, 
        elimina aquellos candidatos a "gemelo" que no cumplan el criterio de número de posts
        en un rango de +/-10% respecto al del usuario original

        Parámetros
        ----------
        users: dict  
            \tDiccionario con usuarios, sus datos y sus posibles candidatos
        posts_per_twin: dict  
            \tDiccionario de usuarios candidatos con su número de posts
    """
    bar = pb.ProgressBar()
    for user in bar(users):
        user_posts = users[user]["posts"]
        filtered_list = []

        for possible_twin in users[user]["possible_twins"]:
            name = possible_twin["name"]
            # Si el candidato no sale en la lista de posts por candidato es que no tiene posts
            # de acuerdo con el funcionamiento de la consulta en Pushshift
            twin_posts = 0
            if name in posts_per_twin:
                twin_posts = posts_per_twin[name]
            possible_twin["posts"] = twin_posts
            # Criterio: Número de posts +/- 10% del usuario original
            if twin_posts >= user_posts - user_posts * 0.1 and twin_posts <= user_posts + user_posts * 0.1:
                filtered_list.append(possible_twin)
        
        users[user]["possible_twins"] = filtered_list


def find_best_twins(users):
    """
        Obtiene el mejor "gemelo" para cada usuario de entrada. Para ello,
        tras previa normalización de las variables, se calcula la distancia euclídea entre
        los candidatos y el usuario, seleccionando el que tenga menor distancia. 
        Las variables utilizadas para calcular la distancia son la fecha de creación del usuario,
        su karma, tanto en posts como en comentarios y el número de posts del usuario.

        Parámetros
        ----------
        users: dict  
            \tDiccionario con los usuarios, sus datos y sus posibles gemelos
        
        Salida
        ------
        dict  
            \tDiccionario con los usuarios y para cada uno de ellos, su mejor gemelo y la distancia que los separa
    """
    best_twins = {}
    bar = pb.ProgressBar()
    for user in bar(users):
        possible_twins = users[user]["possible_twins"]
        data = users[user]

        # Primero hay que normalizar los valores, para ello utilizo la formula de la puntuación estándar: https://es.wikipedia.org/wiki/Unidad_tipificada
        raw_values = {
            "created_utc": [int(twin["created_utc"]) for twin in possible_twins],
            "comment_karma": [int(twin["comment_karma"]) for twin in possible_twins],
            "link_karma": [int(twin["link_karma"]) for twin in possible_twins],
            "posts": [int(twin["posts"]) for twin in possible_twins]
        }

        # Se añaden los datos del usuario a la población antes de calcular la media y desviación típica
        raw_values["created_utc"].append(int(data["created_utc"]))
        raw_values["comment_karma"].append(int(data["comment_karma"]))
        raw_values["link_karma"].append(int(data["link_karma"]))
        raw_values["posts"].append(int(data["posts"]))

        # Medias y desviaciones típicas
        stats = {
            "avg": {
                "created_utc": np.mean(raw_values["created_utc"]),
                "comment_karma": np.mean(raw_values["comment_karma"]),
                "link_karma": np.mean(raw_values["link_karma"]),
                "posts": np.mean(raw_values["posts"]),
            },
            "std":{            
                "created_utc": np.std(raw_values["created_utc"]),            
                "comment_karma": np.std(raw_values["comment_karma"]),            
                "link_karma": np.std(raw_values["link_karma"]),            
                "posts": np.std(raw_values["posts"])
            }
        }

        # Aquellos campos que tengan desviación típica los excluimos del cómputo de la distancia ya que implican
        # que en todos los sujetos valen lo mismo, y por lo tanto, no discriminan nada
        fields = [field for field in stats["std"] if stats["std"][field] > 0]

        # Preparamos el array del usuario
        user_array = np.zeros(len(fields))
        for i in range(len(fields)):
            # Aplicamos la fórmula de la puntuación estándar
            user_array[i] = (int(data[fields[i]])-stats["avg"][fields[i]])/stats["std"][fields[i]]
        
        best_distance = np.inf
        for possible_twin in possible_twins:
            # Estandarizamos
            twin_array = np.zeros(len(fields))
            for i in range(len(fields)):
                twin_array[i] = (int(possible_twin[fields[i]])-stats["avg"][fields[i]])/stats["std"][fields[i]]
            
            # Calculamos distancia euclídea
            distance = np.linalg.norm(twin_array - user_array, ord=2)

            if distance < best_distance:
                best_twins[user] = {
                    "name": possible_twin["name"],
                    "distance": distance
                    }
                best_distance = distance
            
    return best_twins     

def load_csv(path):
    """
        Carga los datos contenidos en el .csv con candidatos y sus números de post

        Parámetros
        ----------
        path: str  
            \tRuta del archivo .csv con los datos
        
        Salida
        ------
        dict  
            \tDiccionario donde para cada nombre de usuario se guarda su número de posts
    """
    twin_posts = {}
    with open(path) as f:
        next(f) # Saltar cabecera
        for line in f:
            user, posts = line.split(";")
            twin_posts[user] = int(posts)
    return twin_posts

def dump_twin_summary(path, twins):
    """
        Vuelca el resultado a un fichero .csv

        Parámetros
        ----------
        path: str  
            \tRuta del fichero de salida
        twins: dict  
            \tDiccionario con los usuarios, su mejor gemelo y la distancia que los separa
    """
    with open(path, "w", encoding="UTF-8") as f:
        f.write("Usuario;Gemelo;Distancia\n")
        for user in twins:
            f.write(";".join((str(user), str(twins[user]["name"]), str(twins[user]["distance"]),"\n")))

def dump_full_data(path, users, twins):
    with open(path, "w", encoding="UTF-8") as f:
        # Cabeceras
        f.write("Usuario;Fecha de creación;Valores de karma;Num. posts;Muestra;Usuario emparejado\n")
        for user in users:
            data= users[user]
            user_data = [
                user
            ]


def parse_args():
    """
        Procesamiento de los argumentos con los que se ejecutó el script
    """
    parser = argparse.ArgumentParser(description="Script que genera un csv con candidatos a 'gemelo' y su número de posts")
    parser.add_argument("-u", "--users", default="pickles/users_and_possible_twins.pickle", help="Ruta del archivo con el diccionario de " 
        "usuarios obtenido en el script find_possible_twins.py")
    parser.add_argument("-p", "--posts", default="posts_per_user.csv", help="Ruta del archivo con el número de posts por candidato")
    parser.add_argument("-o", "--output", default="users_data.csv", help="Archivo .csv donde se volcarán todos los datos de los usuarios y su gemelo")
    parser.add_argument("--summary", default="twins.csv", help="Archivo .csv donde se volcarán los usuarios y su mejor gemelo")
    return parser.parse_args()

if __name__ == "__main__":
    main(parse_args())