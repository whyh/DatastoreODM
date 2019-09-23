from typing import Type

from .basefields import BaseField, EmbeddedBaseField

__all__ = ("List", "Dictionary")


class List(list):
    def __init__(self, content_type: Type[BaseField], *args, **kwargs):
        if not issubclass(content_type, BaseField):
            raise TypeError

        self._content_type = content_type

        super().__init__(*args, **kwargs)

        for value in range(len(self)):
            self._content_type._validate(self[value])
            if issubclass(self._content_type, EmbeddedBaseField) and issubclass(type(self[value]), dict):
                self[value] = self._content_type(**self[value])

    def __setitem__(self, key, value):
        self._content_type._validate(value)

        if issubclass(self._content_type, EmbeddedBaseField) and issubclass(type(value), dict):
            value = self._content_type(**value)

        super().__setitem__(key, value)

    def append(self, item):
        self._content_type._validate(item)

        if issubclass(self._content_type, EmbeddedBaseField) and issubclass(type(item), dict):
            item = self._content_type(**item)

        return super().append(item)


class Dictionary(dict):
    def __init__(self, content_type: Type[BaseField], *args, **kwargs):
        if not issubclass(content_type, BaseField):
            raise TypeError

        self._content_type = content_type

        super().__init__(*args, **kwargs)

        for key, value in self.items():
            self.valid_item(key, value)
            if issubclass(self._content_type, EmbeddedBaseField) and issubclass(type(value), dict):
                self[key] = self._content_type(**value)

    def __setitem__(self, key, value):
        self.valid_item(key, value)

        if issubclass(self._content_type, EmbeddedBaseField) and issubclass(type(value), dict):
            value = self._content_type(**value)

        super().__setitem__(key, value)

    def valid_item(self, key, value) -> None:
        if type(key) is not str:
            raise TypeError

        self._content_type._validate(value)
