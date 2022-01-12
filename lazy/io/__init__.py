from . import pathz
from . import pathz_v2
from . import cachez

#from .pathz import get_path, as_path, to_path, PathLike, register_pathlike_cls, PathzPath, PathzLike, get_lazydir
from .pathz_v2 import as_path, PathLike, PathzPath, PathzS3Path, PathzGSPath, PathzLike, get_lazydir, get_path


## fuze_v2 doesn't work as expected
## falling back to using others
#from . import fuze_v2

#from .fuze_v2 import GCSFuze, S3Fuze, MinioFuze, S3CompatFuze
#from .fuze_v2 import FuzeTypes, get_fuze, autofuze_mount, autofuze_unmount