# lazycls
 When writing data classes becomes too much work

---

## Motivation

This library is forked from an internal project that works with a _lot_ of dataclasses, (AWS API) and I got tired of writing data classes to work with and manipulate them. This library is a wrapper around the main `pydantic.create_model` function that recursively parses a `dict` object and transforms them into subclasses. So nested dict objects within dicts get transformed into their own dataclass.

---

## Quickstart

```bash
pip install --upgrade lazycls
```

```python
from lazycls import LazyCls, BaseLazy

data = {
    'x': ...,
    'y': ...
}

obj = LazyCls(
    name: str = 'CustomCls',
    data: Dict[str, Any] = data, 
    modulename: str = 'lazycls', # your module name
    basecls: Type[BaseModel] = BaseLazy # A custom Base Model class that is used to generate the model
    ) -> Type[BaseModel]:

"""
obj =   lazycls.CustomCls
        lazycls.CustomCls.x = ...
        lazycls.CustomCls.y = ...
"""

```

---
### Utilities

Some additional enhancements/utilities include:

- `set_modulename(name)` - set the default module name - useful when included in other libs

- `clear_lazy_models` - clears all the currently created lazy models. Memory management

- `classproperty` - allows for usage of `@classproperty` which isn't available for Python < 3.9

- `BaseCls` - A wrapper around `BaseModel` with:
    - `arbitrary_types_allowed = True`
    - `.get(name, default)` function to retain `dict`-like properties

- `BaseLazy` - Another wrapper around `BaseModel` with:
    - `arbitrary_types_allowed = True`
    - `extra = 'allow'`
    - `alias_generator = to_camelcase`
    - `orjson` serializer by default
    - `.get(name, default)` function to retain `dict`-like properties


