"""
    Script para obtener las métricas de rendimiento de un conjunto de modelos ya entrenados
    ---------------------------------------------------------------------------------------
    Permite generar un archivo .csv con varias métricas habituales para cada uno de los modelos 
    a evaluar

    Parámetros
    ----------
    * -s, --source: directorio con los modelos a evaluar
    * -t, --tags: .pickle con las etiquetas del conjunto de test extraídas en `src.train`
    * -f, --features: .npz con la matriz de características del conjunto de test
    * -o, --output: nombre del fichero .csv a generar como salida
    * -p, --plot: indica si se generarán las curvas ROC. En caso de ser así, se almacenarán en un .png con el mismo nombre que el .csv de las métricas
"""

import argparse
import os
import pickle
from sklearn.metrics import confusion_matrix, cohen_kappa_score, precision_score, recall_score, accuracy_score, f1_score, roc_auc_score, roc_curve
import progressbar as pb
import joblib
from scipy import sparse as sp
from matplotlib import pyplot as plt

__author__ = "Samuel Cifuentes García"

def main(args):
    print("Cargando modelos...")
    filenames = [file for file in os.listdir(args.source) if file.endswith(".pickle")]
    models = {}
    for filename in filenames:
        models[filename] = joblib.load(args.source + "/" + filename)

    print("Cargando datos del conjunto de test...")    
    tags_test = joblib.load(args.tags)
    matrix_test = sp.load_npz(args.features)

    print("Evaluando modelos...")    
    stats = {}

    if args.plot:
        # Al usasr un objeto de ejes podemos graficar todas las ROC sobre la misma figura
        _, axes = plt.subplots()
        axes.set_xlabel("False Positive Rate")
        axes.set_ylabel("True Positive Rate")

    bar = pb.ProgressBar()
    for name, model in bar(models.items()):
        results = model.predict(matrix_test)        
        stats[name] = get_stats(results, tags_test)

        if args.plot:
            # Curva ROC
            fpr, tpr, _ = roc_curve(tags_test, results)
            plt.plot(fpr, tpr, label=f"{name.replace('.pickle', '')} ({stats[name]['auc']:.2f})")

    print("Volcando resultados...")
    # Métricas numéricas
    with open(args.output, "w") as f:
        f.write("Model;Confusion Matrix;Kappa;Precision;Recall;Accuracy;F1_score;Micro_avg;Macro_avg;AUC\n") # Cabeceras
        for name, data in stats.items():
            matrix = data["confusion_matrix"]
            formatted_matrix = f"{matrix[0][0]} {matrix[0][1]}\\{matrix[1][0]} {matrix[1][1]}"
            line = ";".join((name, formatted_matrix, str(data["kappa"]), str(data["precision"]), str(data["recall"]),
                str(data["accuracy"]), str(data["f1_score"]), str(data["micro_avg"]), str(data["macro_avg"]), str(data["auc"]), "\n"))
            f.write(line)

    # Gráfica con las ROC
    plt.legend()
    plot_filename = "".join(args.output.split(".")[:-1]) + ".png"
    plt.savefig(plot_filename)

def get_stats(results, tags_test):
    """
        Proporciona métricas a partir de los resultados de predecir sobre el dataset de test.
        Las métricas empleadas son:  
        - Matriz de confusión  
        - Kappa  
        - Precision      
        - Recall  
        - Accuracy  
        - F1_score  
        - micro_avg   
        - macro_avg    
        - AUC

        Parámetros
        ----------
        results  
            \nResultado de predecir sobre el set de testing con el modelo
        tags_test  
            \nValores reales de la variable a predecir en el set de testing

        Salida
        ------
        dict  
            \nDiccionario con los valores de cada métrica
    """
    return {
            "confusion_matrix": confusion_matrix(tags_test, results, labels=[0,1]),
            "kappa": cohen_kappa_score(tags_test, results, labels=[0,1]),
            "precision": precision_score(tags_test, results, labels=[0,1]),
            "recall": recall_score(tags_test, results, labels=[0,1]),
            "accuracy": accuracy_score(tags_test, results),
            "f1_score": f1_score(tags_test, results, labels=[0,1]),
            "micro_avg": f1_score(tags_test, results, labels=[0,1], average="micro"),
            "macro_avg": f1_score(tags_test, results, labels=[0,1], average="macro"),
            "auc": roc_auc_score(tags_test, results, labels=[0,1])
    }
    

def parse_args():
    """
        Procesamiento de los argumentos con los que se ejecutó el script
    """
    parser = argparse.ArgumentParser(
        description="Script para obtener estadísticas de un conjunto de modelos ya entrenados")
    parser.add_argument("-s", "--source", default="modelos",
                        help="Directorio con los modelos entrenados")
    parser.add_argument("-t", "--tags", default="pickles/tags_test.pickle", help="Ruta de las etiquetas del conjunto de testing obtenidas durante el entrenamiento")
    parser.add_argument("-f", "--features", default="pickles/features_test.npz", help="Ruta de las características del conjunto de testing obtenidas durante el entrenamiento")
    parser.add_argument("-o", "--output", default="model_stats.csv", help=".csv donde se volcarán los resultados")
    parser.add_argument("-p", "--plot", action="store_true", help="Indica si se debe generar una gráfica con las curvas ROC. En caso afirmativo, se creará un .png con el mismo nombre que el .csv de las métricas")
    return parser.parse_args()


if __name__ == "__main__":
    main(parse_args())
