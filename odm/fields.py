from __future__ import annotations

from copy import copy
from typing import Type, Callable, Any, Optional

from ..datatypes import Array, Key, Location
from .basefield import Field


class IntegerField(Field):
    _pyType = int
    _dsType = "integerValue"

    def _to_entity(self, value: Optional[_pyType]) -> Optional[dict]:
        return super()._to_entity(value) if value is None else {self._dsType: str(value)}

    def _from_entity(self, entity: dict) -> Optional[_pyType]:
        return None if "nullValue" in entity else self._pyType(entity.get(self._dsType))


class TimestampField(Field):
    _pyType = int
    _dsType = "timestampValue"

    def _to_entity(self, value: Optional[_pyType]) -> Optional[dict]:
        return super()._to_entity(value) if value is None else {self._dsType: str(value)}

    # def _from_entity(self, entity: dict) -> Optional[_pyType]:
    #     return None if "nullValue" in entity else NotImplemented


class StringField(Field):
    _pyType = str
    _dsType = "stringValue"

    def _to_entity(self, value: Optional[_pyType]) -> Optional[dict]:
        return super()._to_entity(value) if value is None else {self._dsType: value}

    def _from_entity(self, entity: dict) -> Optional[_pyType]:
        return super()._from_entity(entity)


class BooleanField(Field):
    _pyType = bool
    _dsType = "booleanValue"

    def _to_entity(self, value: Optional[_pyType]) -> Optional[dict]:
        return super()._to_entity(value) if value is None else {self._dsType: value}

    def _from_entity(self, entity: dict) -> Optional[_pyType]:
        return super()._from_entity(entity)


class DoubleField(Field):
    _pyType = float
    _dsType = "doubleValue"

    def _to_entity(self, value: Optional[_pyType]) -> Optional[dict]:
        return super()._to_entity(value) if value is None else {self._dsType: value}

    def _from_entity(self, entity: dict) -> Optional[_pyType]:
        return None if "nullValue" in entity else self._pyType(entity.get(self._dsType))


class ArrayField(Field):
    _pyType = Array
    _dsType = "arrayValue"

    def __init__(self, content: Field, default: Any = None, required: bool = False, index: bool = False,
                 alter: Optional[Callable[[Any], Any]] = None) -> None:

        if not issubclass(type(content), Field):
            raise TypeError(f"{self.__name__}`s parameter \"content\" should be instance of Field\n")
        self._content = content

        super().__init__(default=default, required=required, index=index, alter=alter)

    def __eq__(self, other: ArrayField) -> bool:
        return True if super().__eq__(other) and self._content == other._content else False

    def _update_meta(self, name: Optional[str] = None, **meta: Any) -> None:
        self._content._update_meta(**meta)
        super()._update_meta(name=name, **meta)

    def _assignable(self, value: Any, value_type: Type) -> bool:
        return True if super()._assignable(value, value_type) and value._content == self._content else False

    def _assign(self, value: _pyType) -> _pyType:
        return copy(value)

    def _convert(self, value: Any, value_type: Type) -> _pyType:
        return self._pyType(self._content, value)

    def _to_entity(self, value: Optional[_pyType]) -> Optional[dict]:
        if value is None:
            return super()._to_entity(value)
        values = [self._content._to_entity(item) for item in value]
        return {self._dsType: {"values": values}} if values else super()._to_entity(value)

    def _from_entity(self, entity: dict) -> Optional[_pyType]:
        if "nullValue" in entity:
            return None
        values = entity.get(self._dsType).get("values")
        return self._pyType(self._content, [self._content._from_entity(value) for value in values]) if values else None


class KeyField(Field):
    _pyType = Key
    _dsType = "keyValue"

    def _to_entity(self, value: Optional[_pyType]) -> Optional[dict]:
        if value is None:
            return super()._to_entity(value)

        partition = {"projectId": value.project, "namespaceId": value.namespace}
        path = [{"kind": value.kind, value.id_type: value.id}]
        return {self._dsType: {"partitionId": partition, "path": path}}

    def _from_entity(self, entity: dict) -> Optional[_pyType]:
        if "nullValue" in entity:
            return None
        value = entity.get(self._dsType)
        partition = value.get("partitionId")
        path = value.get("path")[0]
        return self._pyType(project=partition.get("projectId"), namespace=partition.get("namespaceId"), id=path.get("id"), name=path.get("name"), kind=path.get("kind"))


class GeoPointField(Field):
    _pyType = Location
    _dsType = "geoPointValue"

    def _to_entity(self, value: Optional[_pyType]) -> Optional[dict]:
        return super()._to_entity(value) if value is None else {self._dsType: {"latitude": value.latitude, "longitude": value.longitude}}

    def _from_entity(self, entity: dict) -> Optional[_pyType]:
        if "nullValue" in entity:
            return None
        return self._pyType(latitude=entity.get("latitude"), longitude=entity.get("longitude"))


class BlobField(Field):
    ...


class LStringField(Field):
    ...
