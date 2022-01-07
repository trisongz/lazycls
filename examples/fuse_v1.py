"""
Example to show how to mount multiple buckets

Tested to work on ubuntu 18.04
- uses gcsfuse/s3fuse in the backend.
"""

import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/path/to/adc.json'
os.environ['AWS_ACCESS_KEY_ID'] = '...'
os.environ['AWS_SECRET_ACCESS_KEY'] = '...'

from lazy.libz import Lib

bucket1 = 'gs://googlebucket1'
bucket2 = 'gs://googlebucket2'
bucket3 = 's3://awsbucket3'
mountpath1 = '/content/mount1'
mountpath2 = '/content/mount2'
mountpath3 = '/content/mount3'

print(Lib.run_fusemount(bucket1, mountpath1))
print(Lib.run_fusemount(bucket2, mountpath2))
print(Lib.run_fusemount(bucket3, mountpath3))