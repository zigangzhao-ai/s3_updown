# s3_updown
upload and download operations of python-s3

## start
```
pip install boto3


## Directory structure
```
├── updown_s3.py -- Upload, enumerate and download  data from s3 to local dir or from local to s3
├── batch_download_s3.py -- Batch enumerate data using generators an download
├── batch_download_s3_1.py -- Use boto3's resource method to list data
└── multithread_download_s3.py -- Download data in a multithreaded manner
