import os
import shutil
from halo import Halo
from typing import Dict, Tuple, List
from pathlib import Path
from threading import Thread
import boto3
import PIL
from PIL import Image
from resizeimage import resizeimage

s3Client = boto3.client('s3')

MAX_FILE_SIZE = 576000
DIRECTORY_SOURCE = '/var/www/html/pruebas/aws/s3-dev'
DIRECTORY_DST = '/var/www/html/pruebas/aws/test-processing'

def getPathFromSource(filename: str):
    return os.path.join(DIRECTORY_SOURCE, filename)

def getPathFromDestinity(filename: str):
    return os.path.join(DIRECTORY_DST, filename)

def pathIsFile(filePath: str):
    return os.path.isfile(filePath)

def imageRequireResize(filePath: str, reference:int = MAX_FILE_SIZE):
    return Path(filePath).stat().st_size > reference

def createFileName(filePrefix: str, fileName: str):
    return f"{filePrefix}-{fileName}" if filePrefix else fileName

def imageProcessor(fileName: str,  copysConfig: List[ Tuple[str, Tuple[int, int]] ]):
    """
    copysConfig: Es una lista de tuplas donde cada tupla esta compuesta por:
    tuple[0] = Prefijo del archivo
    tuple[1] = Iterable de dimensiones de la imagen

    """

    for tupleConfig in copysConfig:
        prefixFile = tupleConfig[0]
        dimensions = tupleConfig[1]

        sourcePath = getPathFromSource(fileName)
        finalFileName = createFileName(prefixFile, fileName)
        try:
            image = Image.open(sourcePath)
            if imageRequireResize(sourcePath):
                cover = resizeimage.resize_cover(
                    image, dimensions if dimensions[0] > 0 and dimensions[1] > 0 else image.size)
                cover.save(getPathFromDestinity(finalFileName), image.format)
            else:
                image.save(getPathFromDestinity(finalFileName), image.format)
        except PIL.UnidentifiedImageError:
            """
                Si se emite un error al momento de leer la imagen entonces
                puede que el archivo sea extremadamente pequeño. Para prevenir errores
                por corrupción de datos entonces simplemente se copia el archivo
                manualmente
            """
            with open(sourcePath, 'rb') as source:
                target = open(getPathFromDestinity(finalFileName), 'ab')
                # El operador := evita tener que hacer la operacion de lectura 
                # dos veces (una para consulta y otra para asignación)
                # docs: https://docs.python.org/3/whatsnew/3.8.html
                while (binary:= source.read(22000)):
                    target.write(binary)
                target.close()



def process(listdir: List[str]):
    
    for fileName in listdir:
        pathFileSource = getPathFromSource(fileName)
        if pathIsFile(pathFileSource):
            imageProcessor(
                fileName, 
                # MINIATURA
                [('thumb', (150, 150)), 
                # IMAGEN MEDIANA
                ('mid', (350, 350)),
                # IMAGEN ORIGINAL
                ('', (0,0))]
            )




def chunkList(elements: list, elementsByChunk: int = 3):
    """
    Separa una lista de archivos en porciones iguales
    el yield crea un generador por lo que el retorno sera 
    de manera iterativa
    """
    for i in range(0,len(elements), elementsByChunk):
        yield elements[i:i+elementsByChunk]


def main():
    lisDir = os.listdir(DIRECTORY_SOURCE)
    workersNum: int = 0

    with Halo(text='Procesando archivos ...'):
        for elements in chunkList(lisDir, 750):    
            workersNum += 1
            t = Thread(target=process, args=[elements] )
            t.start()
            t.join()

    print(f"Procesando imagenes con {workersNum} thread workers")
    


if __name__ == '__main__':
    main()