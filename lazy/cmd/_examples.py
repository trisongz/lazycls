"""
Examples of using the lazycls extension for cmd

Create a new Type[Cmd]
"""
from lazy.cmd import Cmd

aws = Cmd('aws')

buckets = aws('s3', 'ls').val

"""
>> Returns a List[str] of your buckets

['2021-12-22 13:16:17 bucket1',
'2021-11-21 16:07:00 bucket2',
'2021-12-05 21:38:56 bucket3']

"""

"""
Example usage of automatically generated Type[Cmd] based off of local executables
"""

from lazy.cmd import export

"""
Example usage of gsutil 
"""
from lazy.cmd.auto import gsutil

buckets = gsutil('ls').val

"""
>> Returns a List[str] of your buckets

['gs://bucket1',
 'gs://bucket2',
 'gs://bucket3',
 'gs://bucket4']
"""

bucket_paths = gsutil('ls', 'gs://bucket1').val

"""
>> Returns a List[str] of your bucket/path

['gs://bucket1/data/',
 'gs://bucket1/files/',
 'gs://bucket1/shared/',
 'gs://bucket1/public/']
"""


"""
Example usage of kubectl 
"""
from lazy.cmd.auto import kubectl

export(KUBECONFIG='/path/to/kubeconfig')
nodes = kubectl('get', 'nodes').as_table()

"""
>> Returns a List[Dict] of your Cluster Nodes

[{'NAME': '...ec2.internal',
  'STATUS': 'Ready',
  'ROLES': '<none>',
  'AGE': '3d15h',
  'VERSION': 'v1.21.5-eks-bc4871b'},
 {'NAME': '...ec2.internal',
  'STATUS': 'Ready',
  'ROLES': '<none>',
  'AGE': '10d',
  'VERSION': 'v1.21.5-eks-bc4871b'},
 {'NAME': '...ec2.internal',
  'STATUS': 'Ready',
  'ROLES': '<none>',
  'AGE': '3d8h',
  'VERSION': 'v1.21.5-eks-bc4871b'}]
"""

pods = kubectl('get', 'pods', '--namespace=ingress-nginx').as_table()

"""
>> Returns a List[Dict] of your Pods in namespace ingress-nginx

[{'NAME': 'ingress-nginx-controller-566d4c67fd-5dz7m',
  'READY': '1/1',
  'STATUS': 'Running',
  'RESTARTS': '0',
  'AGE': '2d23h'},
 {'NAME': 'ingress-nginx-controller-566d4c67fd-6k2jr',
  'READY': '1/1',
  'STATUS': 'Running',
  'RESTARTS': '0',
  'AGE': '10d'},
  ...
]
"""

