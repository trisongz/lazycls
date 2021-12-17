from typing import Optional, Any

class _classproperty(property):
    """
    The use this class as a decorator for your class property.
    Example:
        @classproperty
        def prop(cls):
            return "value"
    """

    def __get__(self, *args, **_) -> object:
        """
        This method gets called when a property value is requested.
        @return: The value of the property.
        """
        # apply the __get__ on the class, which is the second argument
        return super(_classproperty, self).__get__(args[1])

    def __set__(self, cls_or_instance, value: object) -> None:
        """
        This method gets called when a property value should be set.
        @param cls_or_instance: The class or instance of which the property should be changed.
        @param value: The new value.
        """
        # call this method only on the class, not the instance
        super(_classproperty, self).__set__(self.__get_class(cls_or_instance), value)

    def __delete__(self, cls_or_instance) -> None:
        """
        This method gets called when a property should be deleted.
        @param cls_or_instance: The class or instance of which the property should be deleted.
        """
        # call this method only on the class, not the instance
        super(_classproperty, self).__delete__(self.__get_class(cls_or_instance))

    def __get_class(self, cls_or_instance) -> type:
        """
        Get the class of an object if one is provided.
        @param cls_or_instance: Either an object or a class.
        @return: The class of the object or just the class again.
        """
        if isinstance(cls_or_instance, type): return cls_or_instance
        return type(cls_or_instance)

class _ClasspropertyMeta(type):
    """
    The class that uses the classproperty decorator must use this meta class if the setting and deleting of a class
    property should be supported.
    """
    def __setattr__(self, name: str, value: object) -> None:
        """
        Override of __setattr__ method to allow a classproperty.setter.
        @param name: The name of the attribute that should get a new value.
        @param value: The new value of the attribute.
        """
        cp_obj: Optional[_classproperty] = self.__get_classproperty_attr(name)
        if cp_obj: cp_obj.__set__(self, value)
        else: super(_ClasspropertyMeta, self).__setattr__(name, value)

    def __delattr__(self, name: str):
        """
        Override of __delattr__ method to allow a classproperty.deleter.
        @param str: The name of the attribute to delete.
        """
        cp_obj: Optional[_classproperty] = self.__get_classproperty_attr(name)
        if cp_obj: cp_obj.__delete__(self)
        else: super(_ClasspropertyMeta, self).__delattr__(name)

    def __get_classproperty_attr(self, name: str) -> Optional[_classproperty]:
        """
        Get a classproperty attribute from this class with the given name.
        @param name: The name of the attribute.
        @return: The classproperty object for the attribute. Or 'None' if it wasn't found.
        """
        # iterate through MRO list to get all attributes
        if (name in self.__dict__ and isinstance(self.__dict__[name], _classproperty)): return self.__dict__[name]
        else: return None

class cached_property:
    """
    Decorator that converts a method with a single self argument into a
    property cached on the instance.

    A cached property can be made out of an existing method:
    (e.g. ``url = cached_property(get_absolute_url)``).
    source: django
    """
    name = None

    @staticmethod
    def func(instance):
        raise TypeError(
            'Cannot use cached_property instance without calling '
            '__set_name__() on it.'
        )

    def __init__(self, func):
        self.real_func = func
        self.__doc__ = getattr(func, '__doc__')

    def __set_name__(self, owner, name):
        if self.name is None:
            self.name = name
            self.func = self.real_func
        elif name != self.name:
            raise TypeError(
                "Cannot assign the same cached_property to two different names "
                "(%r and %r)." % (self.name, name)
            )

    def __get__(self, instance, cls=None):
        """
        Call the function and put the return value in instance.__dict__ so that
        subsequent attribute access on the instance returns the cached value
        instead of calling cached_property.__get__().
        """
        if instance is None:
            return self
        res = instance.__dict__[self.name] = self.func(instance)
        return res


class classproperty_v2:
    """
    Decorator that converts a method with a single cls argument into a property
    that can be accessed directly from the class.
    Simpler than original. 
    Source: django 
    """
    def __init__(self, method=None):
        self.fget = method

    def __get__(self, instance, cls=None):
        return self.fget(cls)

    def getter(self, method):
        self.fget = method
        return self

class classproperty(_classproperty):
    pass

class ClasspropertyMeta(_ClasspropertyMeta):
    pass

__all__ = [
    "classproperty",
    "classproperty_v2",
    "ClasspropertyMeta",
    "cached_property"
]
