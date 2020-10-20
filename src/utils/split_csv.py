"""
    Script para dividir un archivo .csv en otros más pequeños
"""

import csv

__author__ = "Samuel Cifuentes García"

def main():
    with open("hourly_posts.csv") as file:
        csv_file = csv.reader(file, delimiter=";")
        cache = []
        num_docs = 0
        index = 0

        for line in csv_file:
            cache.append(line)
            num_docs += int(line[2])
            if num_docs > 250000:
                write_to_csv(cache, index)
                cache = []
                num_docs = 0
                index +=1

        write_to_csv(cache, index)

def write_to_csv(lines, index):
    """
        Vuelca una lista de líneas a un .csv

        Parámetros
        ----------
        lines: list  
            \tLíneas a volcar

        index: int
            \tÍndice con el que nombrar al archivo fragmento del original
    """
    with open("splitted_csv/hourly_posts_" + str(index) + ".csv", "w") as f:
                    for tuple in lines:
                        f.write(";".join([str(x) for x in tuple]) + "\n")



