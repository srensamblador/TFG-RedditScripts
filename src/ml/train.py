"""
    Script para entrenar un clasificador de post de Reddit
    ------------------------------------------------------

    Parámetros
    ----------
    * training: fichero con el dataset de entrenamiento
    * testing: fichero con el dataset de test
    * -v, --vocab-size: Número de términos más frecuentes a incluir en el vocabulario. 
    * -s, --seed: semilla a utilizar para la generación de números pseudoaleatorios. Opcional.
    * --stem: aplica estemetización en el preprocesado de texto
    * --no-stem: no aplica estemetización en el preprocesado de texto
"""

import os
import json
import string
import numpy as np
import progressbar as pb
import re
import argparse
import pickle
import joblib
from pprint import pprint
from datetime import datetime as dt

from nltk.stem import PorterStemmer
from collections import Counter
from sklearn.naive_bayes import MultinomialNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.neural_network import MLPClassifier
from sklearn import svm
from scipy import sparse as sp

from src.model_stats import get_stats

__author__="Samuel Cifuentes García"

ps = PorterStemmer()
VOCAB_SIZE = 5000

def main(args):
    global VOCAB_SIZE
    VOCAB_SIZE = args.vocab_size

    if args.seed:
        np.random.seed(args.seed)
        
    num_training_docs = file_length(args.training)
    
    # Comentando o descomentando los bloques indicados podemos serializar los resultados parciales
    # del preprocesamiento previo al entrenamiento en disco, para ahorrarnos estos pasos en ejecuciones posteriores

    print("Generando el vocabulario...")
    """
    vocabulary = create_vocabulary(args.training, num_training_docs, args.stem)    
    # De esta forma, podemos serializar el vocabulario para no tener que recalcularlo cuando entrenemos otros modelos
    joblib.dump(vocabulary, "pickles/vocabulary.pickle")
    """
    # Si tenemos el vocabulario ya en disco, se cargaría así    
    vocabulary = joblib.load("pickles/vocabulary.pickle")    

    print("Obteniendo etiquetas...")
    """
    tags_training = get_tags(args.training, num_training_docs)
    # Serializar etiquetas
    joblib.dump(tags_training, "pickles/tags.pickle")    
    """    
    # Cargarlas desde disco
    tags_training = joblib.load("pickles/tags.pickle")
    
    print("Extrayendo características...")
    """
    matrix_training = extract_features(args.training, vocabulary, num_training_docs, args.stem)
    # Serializar
    sp.save_npz("pickles/features.npz", matrix_training)
    """
    # Cargar desde disco
    matrix_training = sp.load_npz("pickles/features.npz")
    

    print("Entrenando modelos...")
    # Metemos aquí los modelos que queramos entrenar en esta ejecución del script
    models = {
        # "LogisticRegression": LogisticRegression(verbose=True, random_state=args.seed),
        #"GiniTree": DecisionTreeClassifier(random_state=args.seed),
        #"IDFTree": DecisionTreeClassifier(criterion="entropy", random_state=args.seed),
        "Bayes": MultinomialNB()
    }

    ## Ejemplos
    # model = LogisticRegression(verbose=True)
    # model = MultinomialNB() # Red Bayesiana
    # model = DecisionTreeClassifier()
    # model = DecisionTreeClassifier(criterion="entropy")
    # model = MLPClassifier(hidden_layer_sizes=(150), random_state=args.seed, verbose=True)
    # model = svm.LinearSVC(verbose=True)
    # model = svm.SVC(cache_size=10000, verbose=True) # TODO Probar parámetro cache_size

    for model in models:
        print("Entrenando " + model + "...")
        models[model] = models[model].fit(matrix_training, tags_training)
        joblib.dump(models[model], "modelos/" + model.lower() + ".pickle") # Serializamos

    print("Evaluando modelos...")
    num_test_docs = file_length(args.test)

    print("Obteniendo etiquetas del conjunto de test...")   
    """ 
    tags_test = get_tags(args.test, num_test_docs)   
    # Serializar
    joblib.dump(tags_test, "pickles/tags_test.pickle")
    """
    # Alternativamente, cargar de disco
    tags_test = joblib.load("pickles/tags_test.pickle")


    print("Extrayendo características del conjunto de test...")   
    matrix_test = extract_features(args.test, vocabulary, num_test_docs, args.stem)
    # Serializar
    sp.save_npz("pickles/features_test.npz", matrix_test)    
    """
    # Cargar de disco
    matrix_test = joblib.load("pickles/features_test.npz)    
    """

    print("Generando predicciones para el conjunto de test...")
    for model in models:
        print("Modelo " + model + "...")
        results = models[model].predict(matrix_test)
        pprint(get_stats(results, tags_test))

def create_vocabulary(path, num_docs, stem):
    """
        Genera una lista de n términos más frecuentes en el dataset proporcionado. 
        Los términos de este vocabulario formarán las características de cada individuo del dataset.

        Parámetros
        ----------
        path: str  
            \tRuta del dataset a utilizar
        num_docs: int  
            \tNúmero de documentos del dataset
        stem: bool  
            \tIndica si se aplica o no estemetización en el preprocesado del texto

        Salida
        ------
        list  
            \tLista de n términos más frecuentes del dataset
    """
    word_list = []
    with open(path, encoding="UTF-8") as f:
        # Barra de progreso
        bar = pb.ProgressBar(max_value=num_docs)
        for line in bar(f):
            data = json.loads(line)["_source"]
            # Preprocesamos el texto hasta obtener una lista de tokens
            word_list += preprocess_text(data, stem)

    # Nos quedamos con los VOCAB_SIZE términos más frecuentes
    vocabulary = Counter(word_list)
    vocabulary = vocabulary.most_common(VOCAB_SIZE)

    vocabulary_list = []
    for item, _ in vocabulary:
        vocabulary_list.append(item)

    return vocabulary_list

def get_tags(path, num_docs):
    """
        Extrae los valores de la etiqueta a clasificar del dataset especificado

        Parámetros
        ----------
        path: str  
            \tRuta del dataset
        num_docs: int  
            \tNúmero de documentos en el dataset

        Salida
        ------
        ndarray  
            \tMatriz con las etiquetas del dataset especificado
    """

    # Tipo int16 por limitaciones de memoria
    matrix = np.zeros((num_docs), dtype=np.int16)
    doc_id = 0

    bar = pb.ProgressBar(max_value=num_docs)
    with open(path, encoding="UTF-8") as f:
        for line in bar(f):
            tag = json.loads(line)["_source"]["lonely"]
            matrix[doc_id] = int(tag)
            doc_id +=1
    return matrix

def extract_features(path, vocabulary, num_docs, stem):
    """
        Genera la matriz de características del dataset proporcionado.

        Se itera a través de los documentos del dataset, de los que se extraen todos los términos del título y el selftext, 
        tras previo preprocesamiento. Para cada término que esté a su vez en el vocabulario generado anteriormente, se calcula 
        su frecuencia (tf) en el documento.

        Por lo tanto, al final obtenemos una matriz donde cada fila es un documento y cada columna es el tf de uno de los términos del vocabulario.


        Parámetros
        ----------
        path: str  
            \tRuta del dataset
        vocabulary: list  
            \tLista de n términos más frecuentes en el dataset obtenida en create_vocabulary
        num_docs: int  
            \tNúmero de documentos en el dataset
        stem: bool  
            \tIndica si se aplica o no estemetización en el preprocesamiento del texto

        Salida
        ------
        ndarray  
            \tMatriz con el tf de cada palabra en cada documento       
        
    """

    """
        La matriz de características puede llegar a tener tamaños problemáticos debido a las limitaciones de hardware que pudiéramos tener.
        Por otra parte, la naturaleza de la matriz de características en nuestro problema implica que estamos trabajando con una matriz donde
        la mayor parte de los valores son 0s (cada término del vocabulario que no aparezca en un documento va tener una frecuencia 0)
        Una matriz donde la mayor parte de los valores son 0 es lo que se conoce como matriz dispersa, la cuál podemos almacenar en una estructura 
        de datos especializada, como lo es [csr_matrix](https://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.csr_matrix.html#scipy.sparse.csr_matrix) dentro del paquete `scipy`
        Utilizando esta estructura en vez de un array de numpy podemos llegar a ahorrar mucha memoria.
    """
    # Necesitamos llevar estos tres arrays para crear la matriz dispersa al acabar el procesado
    indices = [] # Índices de los términos
    values = [] # Las frecuencias
    indptr = [] # Punteros que indican donde empieza cada documento

    bar = pb.ProgressBar(max_value=num_docs)
    indptr.append(0)
    with open(path, encoding="UTF-8") as f:
        for line in bar(f):
            data = json.loads(line)["_source"]
            word_list = preprocess_text(data, stem)

            features = {}
            for word in word_list:
                if word in vocabulary:
                    vocab_index = vocabulary.index(word)
                    if word in features:
                        features[vocab_index] += 1
                    else:
                        features[vocab_index] = 1 
            
            indices.extend(features.keys())
            values.extend(features.values())
            indptr.append(len(indices))

    # Interesante jugar con el tipo numérico para ahorrar memoria. 
    indices = np.asarray(indices, dtype=np.int32)
    indptr = np.asarray(indptr, dtype=np.int32)
    values = np.asarray(values, dtype=np.int32)

    matrix = sp.csr_matrix((values, indices, indptr),
        shape=(len(indptr) -1, len(vocabulary)),
        dtype= np.int32)
    matrix.sort_indices()

    return matrix

def preprocess_text(data, stem):
    """
        Se extrae el título y el contenido de un post y se realiza un preprocesado del texto, eliminando
        puntuación, espacios en blanco y si se indica, aplicando un stemmer

        Parámetros
        ----------
        data: json  
            \tDocumento con el contenido de un post de Reddit
        stem: bool  
            \tIndica si se aplica o no estemetización al texto

        Salida
        ------
        list  
            \tLista de palabras del documento preprocesadas
    """
    # Se concatena título y selftext si es aplicable
    if "selftext" in data and data["selftext"]:
        text = data["title"] + " " + data["selftext"]
    else:
        text = data["title"]

    # Eliminar puntuación y espacios en blanco
    translator = str.maketrans("", "", string.punctuation)
    text = text.translate(translator)
    text = re.sub("\s+", " ", text).lower()

    words = text.split()

    # Estematizar
    if stem:
        words = [ps.stem(word) for word in words]

    return words


def file_length(path):
    """
        Cuenta el número de documentos en un fichero

        Parámetros
        ----------
        path: str
            \tRuta del documento

        Salida
        ------
        int
            \tNúmero de documentos en el fichero
    """
    with open(path, encoding="UTF-8") as f:
        for i, _ in enumerate(f):
            pass
        return i+1


def parse_args():
    """
        Procesamiento de los argumentos con los que se ejecutó el script
    """
    parser = argparse.ArgumentParser(
        description="Script para entrenar modelos a partir de colecciones de documentos de Reddit")
    parser.add_argument("-training", default="datasets/training.ndjson",
                        help="Ruta del dataset de entrenamiento")
    parser.add_argument("-test", default="datasets/test.ndjson",
                        help="Ruta del dataset de test")
    parser.add_argument("-v", "--vocab-size", default=5000, type=int, help="Número de términos a considerar en el vocabulario")
    parser.add_argument("-s", "--seed", type=int, help="Semilla a utilizar")
    parser.add_argument("--stem", dest="stem", action="store_true",
                        help="Aplica estemetización al genererar el vocabulario")
    parser.add_argument("--no-stem", dest="stem", action="store_false",
                        help="No usa estemitazación en la generaciónd el vocabulario")
    parser.set_defaults(stem=True)

    return parser.parse_args()


if __name__ == "__main__":
    main(parse_args())
