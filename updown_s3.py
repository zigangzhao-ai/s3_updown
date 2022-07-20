
import boto3
import re
import os
import math
import hashlib
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

class S3Bucket(object):
    """
    need download boto3 module
    """
    def __init__(self):
        self.access_key = S3_FILE_CONF.get("ACCESS_KEY")
        self.secret_key = S3_FILE_CONF.get("SECRET_KEY")
        self.bucket_name = S3_FILE_CONF.get("BUCKET_NAME")
        self.url = S3_FILE_CONF.get("ENDPOINT_URL")

        # 连接s3
        self.s3 = boto3.client(
            service_name='s3',
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            endpoint_url=self.url,
            )

    def upload_normal(self, path_prefix, file_upload):
        """
        ##小文件上传-上传本地文件到s3指定文件夹下
        """   
        GB = 1024 ** 3
        #default config
        config = TransferConfig(multipart_threshold=5*GB, max_concurrency=10, use_threads=True) #10默认，增加数值增加带宽
        file_name = os.path.basename(file_upload)
        object_name = os.path.join(path_prefix,file_name)
        print('-----begin to upload!----')
        try:
            self.s3.upload_file(file_upload, self.bucket_name, object_name, Config=config)
        except ClientError as e:
            print('error happend!' + str(e))
            return False
        print('upload done!')
        return True

    def upload_files(self, path_bucket, path_local):
        '''
        ##大文件上传
        args:
          path_bucket: bucket桶下的路径，文件上传dir
          path_local: 待上传文件的绝对路径
        '''
        # multipart upload
        chunk_size = 52428800
        source_size = os.stat(path_local).st_size
        print('source_size=', source_size)
        chunk_count = int(math.ceil(source_size/float(chunk_size)))
        mpu = self.s3.create_multipart_upload(Bucket=self.bucket_name, Key=path_bucket)
        part_info = {'Parts': []}
        with open(path_local, 'rb') as fp:
            for i in range(chunk_count):
                offset = chunk_size * i
                bytes = min(chunk_size, source_size-offset)
                data = fp.read(bytes)
                md5s = hashlib.md5(data)
                new_etag = '"%s"' % md5s.hexdigest()
                try:
                    self.s3.upload_part(Bucket=self.bucket_name,Key=path_bucket, PartNumber=i+1,
                                    UploadId=mpu['UploadId'],Body=data)
                except Exception as exc:
                    print("error occurred.", exc)
                    return False
                print('uploading %s %s'%(path_local, str(i/chunk_count)))
                parts={
                    'PartNumber': i+1,
                    'ETag':new_etag
                }
                part_info['Parts'].append(parts)
        print('%s uploaded!' % (path_local))
        self.s3.complete_multipart_upload(Bucket=self.bucket_name,Key=path_bucket,
                                          UploadId=mpu['UploadId'],
                                          MultipartUpload=part_info)
        print('%s uploaded success!' % (path_local))
        return True

    def download_file(self, object_name, path_local):
        """
        download the single file from s3 to local dir
        """
        GB = 1024**3
        config = TransferConfig(multipart_threshold=2*GB, max_concurrency=10, use_threads=True)
        suffix = object_name.split('.')[-1]
        if path_local[-len(suffix):] == suffix:
            file_name = path_local
            dir_name = os.path.dirname(file_name)
            if not os.path.exists(dir_name):
                os.mkdir(dir_name)
        else:
            if not os.path.exists(path_local):
                os.mkdir(path_local)
            file_name = os.path.join(path_local, os.path.basename(object_name))
        print(object_name, file_name)
        try:
            self.s3.download_file(self.bucket_name,object_name,file_name,Config= config)
        except Exception as exc:
            print('some wrong!')
            print("error occurred.", exc)
            return False
        print('downlaod ok', object_name)
        return True
    
    def download_files(self, path_prefix, path_local):
        """
        批量文件下载
        """
        GB = 1024**3
        config = TransferConfig(multipart_threshold=2*GB, max_concurrency=10, use_threads=True)
        list = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=path_prefix)['Contents']
        for key in list:
            name = os.path.basename(key['Key'])
            object_name = key['Key']
            print('-----', object_name, name)
            if not os.path.exists(path_local):
                os.makedirs(path_local)
            file_name = os.path.join(path_local, name)
            try:
                self.s3.download_file(self.bucket_name, object_name, file_name,Config= config)
            except Exception as exc:
                print("error occurred.", exc)
                return False
        return True

    def get_list_s3(self, obj_floder_path):
        """
        用来列举出该目录下的所有文件
        args:
            obj_floder_path: 要查询的文件夹路径
        returns: 
            该目录下所有文件列表
        """
        # 用来存放文件列表
        file_list = []
        response = self.client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=obj_floder_path,
            # Delimiter='',
            MaxKeys=1000,
           ) 
        for file in response['Contents']:
            s = str(file['Key'])
            p = re.compile(r'.*/(.*)(\..*)')
            if p.search(s):
                s1 = p.search(s).group(1)
                s2 = p.search(s).group(2)
                result = s1 + s2
                file_list.append(result)
        return file_list

if __name__ == '__main__':

    S3_FILE_CONF = {
        "ACCESS_KEY": "",
        "SECRET_KEY": "",
        "BUCKET_NAME": "",  
        "ENDPOINT_URL": "", 
        
        "DOWN_S3_DIR": "" ,   #需要下载的s3的文件目录",
        "DOWN_LOCAL_DIR":"",  #本地目录",

        "UPLOAD_S3_DIR": "", #需要上传的s3路径
        "UPLOAD_FILE_DIR": "",  #需要上传的本地文件
        
    }

    s3_buk = S3Bucket()

    """
    upload the file from local to s3
    """
    def main_upload():
        up_s3_dir = S3_FILE_CONF["UPLOAD_S3_DIR"]
        up_local_dir = S3_FILE_CONF["UPLOAD_FILE_DIR"]
        s3_buk.upload_files(up_s3_dir, up_local_dir)

    """
    download the model from s3 to local dir
    """
    def main_download():
        down_s3_dir = S3_FILE_CONF["DOWN_S3_DIR"]
        down_local_dir = S3_FILE_CONF["DOWN_FILE_DIR"]
        s3_buk.download_file(down_s3_dir, down_local_dir)

    main_download()
    ## test batch files download
    # s3_buk.download_files('data/upload-test', './test')