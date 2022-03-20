import threading
import boto3
import os
import sys
import py7zr


class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()
    


path = "C:/Users/rafae/igti/edc-mod1-desafio-rais/input_data/RAIS/2020/extract/"
s3_client = boto3.client("s3")


#def aws_upload (path,bucket,destino):
lista_arq = os.listdir(path)

#for file in lista_arq:
#    with py7zr.SevenZipFile(path+file, mode='r') as z:
#        z.extractall(path=path+'extract')
#        print(file)

# # Criar um cliente para interagir com o AWS S3
# s3_client = boto3.client('s3')
# path_e = path + 'extract'
bucket = "datalake-jeff-igti-edc-tf"
destino = "raw-data/rais/year=2020/"


for file in lista_arq:
    s3_client.upload_file(
                            path + file,
                            bucket,
                            destino + file,
                            Callback = ProgressPercentage(path + file))