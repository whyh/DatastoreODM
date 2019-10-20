from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import Type, Callable, Any, Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from .embedded import Embedded
    from .kind import Kind


class Field(metaclass=ABCMeta):
    _pyType: Type = None
    _dsType: str = None
    _storeNone: bool = None

    def __init__(self, default: Any = None, required: bool = False, index: bool = False, alter: Optional[Callable[[Any], Any]] = None) -> None:
        self._meta = {}
        self._index = index
        self._required = required
        self._alter = lambda value: value if alter is None else alter
        self._default = self._mold(default() if callable(default) else default)
        if self._storeNone is not None:
            self._update_meta(store_none=self._storeNone)

    def __get__(self, instance: Union[Embedded, Kind], owner: Union[Type[Embedded], Type[Kind]]):
        if instance is None:
            return self
        value = instance._data.get(self._name)
        return self._assign(self._default) if value is None else value

    def __set__(self, instance: Union[Embedded, Kind], value: Any) -> None:
        instance._data[self._name] = self._mold(value)

    def __eq__(self, other: Field) -> bool:
        return True if (self.__class__ == other.__class__ and
                        self._index == other._index and
                        self._required == other._required and
                        self._alter == other._alter and
                        self._default == other._default) else False

    @property
    def _name(self) -> str:
        return self._meta.get("name", "")

    @property
    def _store_none(self) -> bool:
        return self._meta.get("store_none", self._storeNone)

    def _update_meta(self, name: Optional[str] = None, **meta: Any) -> None:
        if name is not None:
            self._meta["name"] = name
        self._meta.update(meta)

    def _mold(self, value: Any) -> _pyType:
        value = self._alter(value)
        if value is None:
            return

        value_type = type(value)
        try:
            if value_type is dict:
                return self._from_entity(value)

            if self._assignable(value, value_type):
                return self._assign(value)

            return self._convert(value, value_type)
        except Exception as error:
            raise ValueError(f"Incompatible value {value}\n"
                             f"Conversion error: {error}")

    def _assignable(self, value: Any, value_type: Type) -> bool:
        return True if issubclass(value_type, self._pyType) else False

    def _assign(self, value: _pyType) -> _pyType:
        return value

    def _convert(self, value: Any, __________: Type) -> _pyType:
        return self._pyType(value)

    @abstractmethod
    def _to_entity(self, _____: Optional[_pyType]) -> Optional[dict]:
        if self._store_none:
            return {"nullValue": None}

    @abstractmethod
    def _from_entity(self, entity: dict) -> Optional[_pyType]:
        return None if "nullValue" in entity else entity.get(self._dsType)
