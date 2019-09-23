from collections import deque
from copy import copy
from typing import Type, Callable, Any

__all__ = ("BaseField", "ComplexBaseField", "EmbeddedBaseField")


class BaseField:
    def __init__(self, default=None, index: bool = False, valid: Callable[[Any], bool] = None):
        self._name = None
        self._default = default
        self._index = index
        self._valid = lambda value: True if valid is None else valid

    def __get__(self, instance, owner):
        return self if instance is None else instance._data.get(self._name, self._default_value)

    def __set__(self, instance, value):
        self._validate(value)
        if value is None or not self._valid(value):
            if self._name in instance._data:
                del instance._data[self._name]
        else:
            instance._data[self._name] = value

    @staticmethod
    def _validate(value):
        raise NotImplementedError

    @classmethod
    def _db_repr(cls, value):
        return value

    @property
    def _default_value(self):
        value = self._default() if callable(self._default) else self._default
        self._validate(value)
        return value


class ComplexBaseField(BaseField):
    def __init__(self, content_type: Type[BaseField], default=None, valid: Callable[[Any], bool] = None):
        if issubclass(content_type, BaseField):
            self._content_type = content_type
        else:
            raise TypeError

        super().__init__(default=default, index=False, valid=valid)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        else:
            value = instance._data.get(self._name)
            if value is None:
                value = instance._data[self._name] = copy(self._default_value)
            return value

    @classmethod
    def _db_repr(cls, value):
        if value is None or len(value) == 0:
            return None
        else:
            result = value.copy()
            result_len = len(result)
            for item in range(result_len):
                if issubclass(type(result[item]), BaseField):
                    result[item] = result[item]._db_repr(result[item])
            return result


class EmbeddedBaseFieldMeta(type):
    def __new__(mcs, name, bases, attrs):

        if name is not "EmbeddedBaseField":
            fields = {}

            for base in bases:
                if base is not EmbeddedBaseField and issubclass(base, EmbeddedBaseField):
                    fields.update(base._fields)

            for attr_name, attr in attrs.items():
                if isinstance(attr, BaseField):
                    if attr_name[0] == "_":
                        raise NameError
                    attr._name = attr_name
                    fields[attr_name] = attr
            attrs["_fields"] = fields

        return super().__new__(mcs, name, bases, attrs)


class EmbeddedBaseField(BaseField, metaclass=EmbeddedBaseFieldMeta):
    _fields = None

    def __init__(self, *args, default=None, valid: Callable[[Any], bool] = None, **kwargs):
        self.__set_data(*args, **kwargs)

        super().__init__(default=default, valid=valid)

    def __set__(self, instance, value):
        self._validate(value)

        if value is None or not self._valid(value):
            instance._data[self._name] = (self.__class__(default=self._default, valid=self._valid)
                                          if self._default_value is None else self._default_value)
        else:
            tmp = instance._data[self._name] = self.__class__(default=self._default, valid=self._valid)
            if issubclass(type(value), self.__class__):
                tmp._data = value._data

            elif issubclass(type(value), dict):
                tmp.__set_data(**value)

    def __set_data(self, *args, **kwargs) -> None:
        self._data = {}

        args = deque(args)

        for name, field in self._fields.items():
            value = kwargs.get(name, None) if len(args) == 0 else args.popleft()
            setattr(self, name, value)

    @classmethod
    def _validate(cls, value):
        if value is not None:
            value_type = type(value)
            if not issubclass(value_type, cls) and not issubclass(value_type, dict):
                raise TypeError(value)

    @classmethod
    def _db_repr(cls, instance):
        dictionary = {}
        for name, field in cls._fields.items():
            value = field._db_repr(instance._data.get(name))
            if value is not None:
                dictionary[name] = value

        return None if len(dictionary) == 0 else dictionary
