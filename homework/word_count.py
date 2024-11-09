# Este codigo simila como funciona el algoritmo de MapReduce en Hadoop. El codigo recibe un directorio de archivos de texto, realiza un preprocesamiento de las lineas de texto, 
# cuenta la cantidad de palabras en cada linea, 
# ordena las palabras y cuenta cuantas veces aparece cada palabra en el texto. Finalmente, el codigo almacena el resultado en un archivo de texto en un directorio de salida.

"""Taller evaluable"""

# pylint: disable=broad-exception-raised

import fileinput
import glob
import os.path
from itertools import groupby
from pprint import pprint
import string


#
# Escriba la función load_input que recive como parámetro un folder y retorna
# una lista de tuplas donde el primer elemento de cada tupla es el nombre del
# archivo y el segundo es una línea del archivo. La función convierte a tuplas
# todas las lineas de cada uno de los archivos. La función es genérica y debe
# leer todos los archivos de folder entregado como parámetro.
#
# Por ejemplo:
#   [
#     ('text0'.txt', 'Analytics is the discovery, inter ...'),
#     ('text0'.txt', 'in data. Especially valuable in ar...').
#     ...
#     ('text2.txt'. 'hypotheses.')
#   ]
#
def load_input(input_directory):
    """Funcion load_input"""
    files = glob.glob(f"{input_directory}/*.txt")
    # print()
    # pprint(files)
    # print()

    sequence = []
    # filesinput.input(files=files) recibe una lista de archivos, lee cada uno de los archivos y retorna una lista de lineas con las lineas de cada archivo
    with fileinput.input(files=files) as f:
        for line in f:
            # .filename() retorna el nombre del archivo actual y line.strip() retorna la linea actual sin espacios
            sequence.append((fileinput.filename(), line.strip())) 

    return sequence


#
# Escriba la función line_preprocessing que recibe una lista de tuplas de la
# función anterior y retorna una lista de tuplas (clave, valor). Esta función
# realiza el preprocesamiento de las líneas de texto,
#
def line_preprocessing(sequence):
    """
    Preprocess a list of tuples containing text lines.

    This function takes a list of tuples where each tuple consists of a key and a text line.
    It processes each text line by:
    1. Removing all punctuation.
    2. Converting all characters to lowercase.
    3. Stripping leading and trailing whitespace.

    Args:
        sequence (list of tuples): A list of tuples where each tuple contains a key and a text line.

    Returns:
        list of tuples: A list of tuples with the processed text lines.
    """
    # Initialize an empty list to store the processed tuples
    processed_sequence = []

    # Iterate over each tuple in the input list
    for key, value in sequence:
        # Remove punctuation from the text line
        value_no_punctuation = value.translate(str.maketrans("", "", string.punctuation))
        
        # Convert the text line to lowercase
        value_lowercase = value_no_punctuation.lower()
        
        # Strip leading and trailing whitespace from the text line
        value_stripped = value_lowercase.strip()
        
        # Append the processed tuple to the list
        processed_sequence.append((key, value_stripped))

    # Return the list of processed tuples
    return processed_sequence

    



#
# Escriba una función llamada maper que recibe una lista de tuplas de la
# función anterior y retorna una lista de tuplas (clave, valor). En este caso,
# la clave es cada palabra y el valor es 1, puesto que se está realizando un
# conteo.
#
#   [
#     ('Analytics', 1),
#     ('is', 1),
#     ...
#   ]
#
def mapper(sequence):
    """Mapper"""
    # return [(word, 1) for _ , text_line in sequence for word in text_line.split()]
# Initialize an empty list to store the word counts
    word_count_list = []

    # Iterate over each tuple in the input list
    for _, text_line in sequence:
        # Split the text line into words
        words = text_line.split()
        
        # Iterate over each word in the text line
        for word in words:
            # Append the word and count (1) as a tuple to the list
            word_count_list.append((word, 1))
    
    # Return the list of word counts
    return word_count_list


#
# Escriba la función shuffle_and_sort que recibe la lista de tuplas entregada
# por el mapper, y retorna una lista con el mismo contenido ordenado por la
# clave.
#
#   [
#     ('Analytics', 1),
#     ('Analytics', 1),
#     ...
#   ]
#
def shuffle_and_sort(sequence):
    """Shuffle and Sort"""
    return sorted(sequence, key=lambda x: x[0])
    


#
# Escriba la función reducer, la cual recibe el resultado de shuffle_and_sort y
# reduce los valores asociados a cada clave sumandolos. Como resultado, por
# ejemplo, la reducción indica cuantas veces aparece la palabra analytics en el
# texto.
#
def reducer(sequence):
    """Reducer"""
    # Initialize an empty dictionary to store the aggregated values
    dict_sequence = {}

    # Iterate over each tuple in the input list
    for key, value in sequence:
        # If the key is already in the dictionary, add the value to the existing value
        # If the key is not in the dictionary, initialize it with the value
        dict_sequence[key] = dict_sequence.get(key, 0) + value

    # Convert the dictionary items to a list of tuples and return it
    return list(dict_sequence.items())


#
# Escriba la función create_ouptput_directory que recibe un nombre de
# directorio y lo crea. Si el directorio existe, lo borra
#
def create_ouptput_directory(output_directory):
    """Create Output Directory"""
    # Check if the output directory exists
    if os.path.exists(output_directory):
        # Iterate over each file in the output directory
        for file in glob.glob(f"{output_directory}/*"):
            # Remove the file
            os.remove(file)
        # Remove the output directory itself
        os.rmdir(output_directory)
    
    # Create a new output directory
    os.mkdir(output_directory)


#
# Escriba la función save_output, la cual almacena en un archivo de texto
# llamado part-00000 el resultado del reducer. El archivo debe ser guardado en
# el directorio entregado como parámetro, y que se creo en el paso anterior.
# Adicionalmente, el archivo debe contener una tupla por línea, donde el primer
# elemento es la clave y el segundo el valor. Los elementos de la tupla están
# separados por un tabulador.
#
def save_output(output_directory, sequence):
    """Save Output"""
    with open(f"{output_directory}/part-00000", "w", encoding="utf-8") as file:
        for key, value in sequence:
            file.write(f"{key}\t{value}\n")


#
# La siguiente función crea un archivo llamado _SUCCESS en el directorio
# entregado como parámetro.
#
def create_marker(output_directory):
    """Create Marker"""
    with open(f"{output_directory}/_SUCCESS", "w", encoding="utf-8") as file:
        file.write("")


#
# Escriba la función job, la cual orquesta las funciones anteriores.
#
def run_job(input_directory, output_directory):
    """Job"""
    files = load_input(input_directory)
    files = line_preprocessing(files)
    files = mapper(files)
    files = shuffle_and_sort(files)
    files = reducer(files)
    create_ouptput_directory(output_directory)
    save_output(output_directory, files)
    create_marker(output_directory)
    print(files)


if __name__ == "__main__":
    run_job(
        "files/input",
        "files/output",
    )
