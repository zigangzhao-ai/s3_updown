# !/usr/bin/python
# -*- coding: UTF-8- -*-

"""
使用boto3的resource方法去列举和下载，不需要借助生成器，直接可以获取到全部文件
"""
import boto3
import os
import time

# Client
access_key = ""
secret_key = ""
url = ""
bucket_name = ""
prefix = ""

s3_client = boto3.resource(service_name='s3', 
                            aws_access_key_id=access_key,
                            aws_secret_access_key=secret_key, 
                            endpoint_url=url)

bucket = s3_client.Bucket(name=bucket_name)
s3_client1 = boto3.client(service_name='s3', 
                            aws_access_key_id=access_key,
                            aws_secret_access_key=secret_key, 
                            endpoint_url=url)                   
i = 0
# print('----', len(bucket.objects.filter(Prefix=prefix)))
for obj in bucket.objects.filter(Prefix=prefix):
    i += 1
    print(i, '{0}:{1}'.format(bucket.name, obj.key))
    object_name = obj.key
    # print('++++', object_name)
    file_name = object_name
    folder_name = object_name[0:object_name.rfind('/')]
    if os.path.exists(folder_name) == False:
        os.makedirs(folder_name, mode=0o777)
    try:
        s3_client1.download_file(bucket_name, object_name, file_name)
        time.sleep(0.001)
    except:
        print('---download failed!')
    
