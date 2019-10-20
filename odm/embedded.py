from __future__ import annotations

from copy import copy
from abc import ABCMeta
from typing import Any, Callable, Optional, Type, Dict, Tuple, Set

from .basefield import Field


class EmbeddedMeta(ABCMeta):
    def __new__(mcs, class_name: str, bases: Tuple[type], attrs: dict, **kwargs) -> type:
        if class_name is not "Embedded":
            fields = {}
            noindex = set()

            for base in bases:
                if issubclass(base, Embedded) and base._fields is not NotImplemented:
                    fields.update(base._fields)
                    noindex |= base._noindex

            for name, attr in attrs.items():
                if isinstance(attr, Field):
                    if not attr._index:
                        noindex.add(name)
                    attr._update_meta(name=name)
                    fields[name] = attr

            attrs["_fields"] = fields
            attrs["_noindex"] = noindex

        return super().__new__(mcs, class_name, bases, attrs, **kwargs)


class Embedded(Field, metaclass=EmbeddedMeta):
    _dsType = "entityValue"

    _fields: Dict[str, Field] = NotImplemented
    _noindex: Set[str] = NotImplemented

    def __init__(self, default: Any = None, required: bool = False, index: bool = False, alter: Optional[Callable[[Any], Any]] = None,
                 **values: Any) -> None:

        self._pyType = self.__class__
        self._data = {}
        for name, field in self._fields.items():
            value = values.get(name)
            if field._required and value is None:
                raise ValueError("Required value is not present or is equal to None")
            setattr(self, name, value)

        super().__init__(default=default, required=required, index=index, alter=alter)

    def __eq__(self, other: Embedded) -> bool:
        return True if super().__eq__(other) and self._fields == other._fields else False

    def _update_meta(self, name: Optional[str] = None, **meta: Any) -> None:
        for field in self._fields.values():
            field._update_meta(**meta)
        super()._update_meta(name=name, **meta)

    def _assignable(self, value: Any, value_type: Type) -> bool:
        return True if value == self else False

    def _assign(self, value: Embedded) -> Embedded:
        value = copy(value)
        value._update_meta(**self._meta)
        return value

    def _convert(self, value: Any, value_type: Type) -> Embedded:
        if issubclass(value_type, Embedded):
            value = self._from_entity(value._to_entity(value))
        elif issubclass(value_type, dict):
            value = self._pyType(**value)
        else:
            value = super()._convert(value, value_type)
        value._update_meta(**self._meta)
        return value

    def _to_entity(self, value: Optional[Embedded]) -> Optional[dict]:
        if value is None:
            return super()._to_entity(value)

        properties = {}
        for name, field in self._fields.items():
            property = value._data.get(name)
            if property is not None:
                properties[name] = field._to_entity(property)

        if properties:
            return {"entityValue": {"properties": properties}}

    def _from_entity(self, entity: dict) -> Optional[Embedded]:
        return None if "nullValue" in entity else self.__class__(**entity["entityValue"]["properties"])
