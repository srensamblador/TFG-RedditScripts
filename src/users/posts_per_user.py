"""
    Script para recuperar el número de posts de un conjunto de candidatos
    ---------------------------------------------------------------------
    A partir del diccionario de usuarios obtenidos tras la ejecución de `find_possible_twins`, este script genera
    un .csv con aquellos candidatos que hayan creado algún post antes de la fecha especificada y el número de posts 
    que han creado.

    Parámetros
    ----------
    * -s, --source: Ruta del archivo con el diccionario resultante de la ejecución de `find_possible_twins`
    * -o, --output: Fichero dónde se volcarán los resultados
    * -b, --before: Fecha límite antes de la cúal se obtendrá el número de posts de los usuarios
"""
import argparse
from psaw import PushshiftAPI
import pickle
import progressbar as pb
from datetime import datetime as dt
from datetime import time
from datetime import date
import csv

__author__ = "Samuel Cifuentes García"

def main(args):
    # Debido a las limitaciones de uso de la API este script es muy costoso 
    # en cuanto a tiempo de máquina. Es importante escoger un valor adecuado
    # de MAX_USERS en el script find_possible_twins para que no se vuelva inviable
    # la ejecución de este script
    api = PushshiftAPI()
    before_date = int(dt.combine(args.before, time(hour=23, minute=59, second=59)).timestamp())

    print("Cargando datos...")
    with open(args.source, "rb") as f:
        users = pickle.load(f)
   
    print("Generando lista de candidatos...")
    set_users = set()
    for user in users:
        for u in users[user]["possible_twins"]:
            set_users.add(u["name"])

    print("Consultando número de posts...")
    with open(args.output, "w") as f:
        f.writelines("Name;Posts\n")
        
        bar = pb.ProgressBar()
        block = []
        for user in bar(set_users):
            block.append(user)
            # Debemos consultarlos de 100 en 100
            if len(block) >= 100:
                gen = api.search_submissions(author=block, aggs="author", size=0, before=before_date)
                res = next(gen)
                for author in res["author"]:
                    f.write(";".join((author["key"], str(author["doc_count"]))) + "\n")
                block = []

        gen = api.search_submissions(author=block, aggs="author", size=0)
        res = next(gen)
        for author in res["author"]: 
            f.writelines(";".join((author["key"], str(author["doc_count"]))) + "\n")


def parse_args():
    """
        Procesamiento de los argumentos con los que se ejecutó el script
    """
    parser = argparse.ArgumentParser(description="Script que genera un csv con candidatos a 'gemelo' y su número de posts")
    parser.add_argument("-s", "--source", default="pickles/FaseB/users_and_possible_twins.pickle", help="Ruta del archivo con el diccionario de " 
        "usuarios obtenido en el script find_possible_twins.py")
    parser.add_argument("-o", "--output", default="posts_per_user.csv", help="Archivo donde se volcarán los resultados")
    parser.add_argument("-b", "--before", default=date.today(), 
        type= lambda d: dt.strptime(d, '%Y-%m-%d').date(), 
        help="Fecha límite para obtener el número de posts de los usuarios")
    return parser.parse_args()

if __name__ == "__main__":
    main(parse_args())