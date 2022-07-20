# !/usr/bin/python
# -*- coding: UTF-8- -*-
from boto3.session import Session
import boto3
import os
import re

def get_list_s3(s3_client, bucket_name, prefix):
    """
    循环列举出该目录下的所有文件
    args:
       bucket_name: 桶名称
       prefix: 要查询的文件夹
    returns: 
       该目录下所有文件列表
    """
    s3_result = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="")
    if 'Contents' not in s3_result:
        return []
    file_list = []
    for key in s3_result['Contents']:
        file_list.append(key['Key'])
    print(f"List count = {len(file_list)}")
    while s3_result['IsTruncated']:
        continuation_key = s3_result['NextContinuationToken']
        s3_result = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="", ContinuationToken=continuation_key)
        for key in s3_result['Contents']:
            file_list.append(key['Key'])
        print(f"List count = {len(file_list)}")
    return file_list


def get_all_s3_objects(s3_client, **base_kwargs):
    """
    循环读取文件下文件
    """
    continuation_token = None
    while True:
        list_kwargs = dict(MaxKeys=1000, **base_kwargs)
        if continuation_token:
            list_kwargs['ContinuationToken'] = continuation_token
        response = s3_client.list_objects_v2(**list_kwargs)
        print('----', response)
        yield from response.get('Contents', [])
        if not response.get('IsTruncated'):  # At the end of the list?
            break
        continuation_token = response.get('NextContinuationToken')

##total video- 23368
if __name__ == '__main__':

    # Client
    access_key = ""
    secret_key = ""
    url = ""
    bucket_name = ""
    prefix = "" #指定的bucket路径

    session = Session(access_key, secret_key)
    s3_client = session.client('s3', endpoint_url=url)

    # get_list_s3(s3_client, bucket_name, prefix)
    img_suffix = ['jpg', 'jpeg', 'png', 'tif']
    num = 0
    for obj in get_all_s3_objects(s3_client, Bucket=bucket_name, Prefix=prefix): #Delimiter='/'
        # print(obj)  
        object_name = obj['Key']
        # print('++++', object_name)
        file_name = object_name
        time_tag = file_name.split('/')[4][:4]
        tag = os.path.basename(file_name)
        print('++++', time_tag, tag)

        folder_name = object_name[0:object_name.rfind('/')]

        if os.path.exists(folder_name) == False:
            os.makedirs(folder_name, mode=0o777)
        if tag.endswith('.jpg') or tag.endswith('.jpeg') or tag.endswith('.png') or tag.endswith('.tif'):
            try:
                s3_client.download_file(bucket_name, object_name, file_name)
                num += 1
                print(num)
            except:
                print('---download failed!')
           
    