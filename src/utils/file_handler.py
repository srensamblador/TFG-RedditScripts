"""
    Funciones básicas de manejo de ficheros que son utilizadas por otros scrips
"""

__author__: "Samuel Cifuentes García"

def write_to_csv(filename, data, header=None):
    """
        Serializa una lista de tuplas en formato .csv

        Parámetros
        ----------
        filename: str
            Nombre del archivo
        data: list of tuple
            Datos a serializar
        header: str
            Cabecera (opcional)
    """
    with open(filename, "w") as f:
        if header:
            f.write(",".join([x for x in header]) + "\n")
        for tuple in data:
            f.write(",".join([str(x) for x in tuple]) + "\n")

def list_from_file(filename):
    """
        Lee un fichero formado por elementos separados por saltos de línea y devuelve una lista

        Parámetros
        ----------
        filename: str
            Nombre del archivo
        
        Salida
        ------
        data: list
            Lista de elementos
    """
    data = []
    with open(filename) as f:
        for line in f:
            data.append(line.strip())
    return data