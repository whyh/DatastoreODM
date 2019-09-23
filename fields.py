from datetime import datetime
from typing import Type, Callable, Any

from . import custom
from .basefields import BaseField, ComplexBaseField


class IntegerField(BaseField):
    def __init__(self, default=None, index: bool = False, valid: Callable[[Any], bool] = None):
        super().__init__(default=default, index=index, valid=valid)

    @staticmethod
    def _validate(value):
        if value is not None and type(value) is not int:
            raise TypeError(value)


class DatetimeField(BaseField):
    def __init__(self, default=None, index: bool = False, valid: Callable[[Any], bool] = None):
        super().__init__(default=default, index=index, valid=valid)

    @staticmethod
    def _validate(value):
        if value is not None and type(value) is not datetime:
            raise TypeError(value)


class StringField(BaseField):
    def __init__(self, default=None, index: bool = False, valid: Callable[[Any], bool] = None):
        super().__init__(default=default, index=index, valid=valid)

    @staticmethod
    def _validate(value):
        if value is not None and type(value) is not str:
            raise TypeError


class BooleanField(BaseField):
    def __init__(self, default=None, index: bool = False, valid: Callable[[Any], bool] = None):
        super().__init__(default=default, index=index, valid=valid)

    @staticmethod
    def _validate(value):
        if value is not None and type(value) is not bool:
            raise TypeError


class FloatField(BaseField):
    def __init__(self, default=None, index: bool = False, valid: Callable[[Any], bool] = None):
        super().__init__(default=default, index=index, valid=valid)

    @staticmethod
    def _validate(value):
        if value is not None:
            type_value = type(value)
            if type_value is not int and type_value is not float:
                raise TypeError


class ListField(ComplexBaseField):
    def __init__(self, content_type: Type[BaseField], default=None, valid: Callable[[Any], bool] = None):
        super().__init__(content_type, default=default, valid=valid)
        self._default = custom.List(self._content_type) if default is None else default

    def __set__(self, instance, value):
        if type(value) is list:
            value = custom.List(self._content_type, value)
        super().__set__(instance, value)

    @staticmethod
    def _validate(value):
        if value is not None and type(value) is not custom.List:
            raise TypeError()


class DictionaryField(ComplexBaseField):
    def __init__(self, content_type: Type[BaseField], default=None, valid: Callable[[Any], bool] = None):
        super().__init__(content_type, default=default, valid=valid)
        self._default = custom.Dictionary(self._content_type) if default is None else default

    def __set__(self, instance, value):
        if type(value) is dict:
            value = custom.Dictionary(self._content_type, value)
        super().__set__(instance, value)

    @staticmethod
    def _validate(value):
        if value is not None and type(value) is not custom.Dictionary:
            raise TypeError
