from . import reqz

from .reqz import *

""" 
Borrowed from

https://github.com/slingamn/mureq

Why?
In short: performance (memory consumption), security (resilience to supply-chain attacks), and simplicity.

Performance
python-requests is extremely memory-hungry, mainly due to large transitive dependencies like chardet that are not needed

mureq is a replacement for python-requests, intended to be vendored
in-tree by Linux systems software and other lightweight applications.

mureq is copyright 2021 by its contributors and is released under the
0BSD ("zero-clause BSD") license.

---
You can either use httpz or reqz which are identical

from lazy import httpz
from lazy.httpz import reqz

print(httpz.get('https://clients3.google.com/generate_204'))
print(reqz.get('https://clients3.google.com/generate_204'))

"""