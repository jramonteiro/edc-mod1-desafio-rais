import os
import sys
import py7zr
import boto3
path = "C:/Users/rafae/igti/edc-mod1-desafio-rais/input_data/RAIS/2020/"
s3 = "s3://datalake-jeff-igti-edc-tf/raw-data/rais/year=2020/extract/"
destino = "raw-data/rais/year=2020/"

#def aws_upload (path,bucket,destino):
#lista_arq = os.listdir(path)
#print(lista_arq)
#with py7zr.SevenZipFile('sample.7z', mode='r') as z:
#    z.extractall()

def get_matching_s3_objects(bucket, prefix="", suffix=""):
    """
    Generate objects in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    kwargs = {'Bucket': bucket}

    # We can pass the prefix directly to the S3 API.  If the user has passed
    # a tuple or list of prefixes, we go through them one by one.
    if isinstance(prefix, str):
        prefixes = (prefix, )
    else:
        prefixes = prefix

    for key_prefix in prefixes:
        kwargs["Prefix"] = key_prefix

        for page in paginator.paginate(**kwargs):
            try:
                contents = page["Contents"]
            except KeyError:
                break

            for obj in contents:
                key = obj["Key"]
                if key.endswith(suffix):
                    yield obj

def get_matching_s3_keys(bucket, prefix="", suffix=""):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    for obj in get_matching_s3_objects(bucket, prefix, suffix):
        yield obj["Key"]

x = get_matching_s3_keys("datalake-jeff-igti-edc-tf","raw-data/rais/year=2020")

for a in x:
    print(a)