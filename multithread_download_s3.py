# !/usr/bin/python
# -*- coding: UTF-8- -*-

from boto3.session import Session
import boto3
import os
import threading, time
import re

def get_all_s3_objects(s3_client, **base_kwargs):
    continuation_token = None
    while True:
        list_kwargs = dict(MaxKeys=1000, **base_kwargs)
        if continuation_token:
            list_kwargs['ContinuationToken'] = continuation_token
        response = s3_client.list_objects_v2(**list_kwargs)
        yield from response.get('Contents', [])
        if not response.get('IsTruncated'): 
            break
        continuation_token = response.get('NextContinuationToken')

def download_data(s3_client, bucket_name, perfix, f_record, start, end):

    num = 0
    for idx, obj in enumerate(get_all_s3_objects(s3_client, Bucket=bucket_name, Perfix=perfix)):
        # print(idx, obj)
        if idx in list(range(start, end)): 
            object_name = obj['Key']
            # print(object_name)
            folder_name = object_name[0:object_name.rfind('/')]
            # print(folder_name)
            if os.path.exists(folder_name) == False:
                os.makedirs(folder_name, mode=0o777)
            file_name = object_name
            print(file_name)
            try:
                num += 1
                print(num)
                s3_client.download_file(bucket_name, object_name, file_name)
            except:
                f_record.write(object_name + '\n')


##total video-23368
if __name__ == '__main__':

    # Client
    access_key = ""
    secret_key = ""
    url = ""
    bucket_name = ""
    prefix = ""


    session = Session(access_key, secret_key)
    s3_client = session.client('s3', endpoint_url=url)
    f_record = open('error_list.txt', 'w') 

    start_time = time.time()
    t_list = []
    for i in range(0, 23368, 500):  #22368==len(file_list)
        start, end = i, i+500
        t = threading.Thread(target=download_data, args=(s3_client, bucket_name, prefix, f_record, start, end))
        t.start()
        t_list.append(t)
        # t.join()
    for t in t_list:
        t.join()

    end_time = time.time()
    interval = end_time - start_time
    print("interval_thread:", interval)

'''
    num = 0
    for obj in get_all_s3_objects(s3_client, Bucket=bucket_name):
        # print(obj)  
        object_name = obj['Key']
        # print(object_name)
        folder_name = object_name[0:object_name.rfind('/')]
        # print(folder_name)
        if os.path.exists(folder_name) == False:
            os.makedirs(folder_name, mode=0o777)
        file_name = object_name
        print(file_name)
        num += 1
    
        try:
            s3_client.download_file(bucket_name, object_name, file_name)
            print(num)
        except:
            f_record.write(object_name + '\n')
'''
      
       
