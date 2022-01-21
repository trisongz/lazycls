from lazy.models import BaseCls, create_lazycls
from lazy.types import *

def convert_to_cls(resp: DictAny, module_name: str = 'lazy', base_key: str = 'api') -> List[Type[BaseCls]]:
    for key, vals in resp.items():
        mod_key = f'{base_key}{key}'
        if isinstance(vals, list): vals = [create_lazycls(mod_key, v, modulename=module_name) for v in vals]
        else: vals = create_lazycls(mod_key, vals, modulename=module_name)
        resp[key] = vals
    return resp


