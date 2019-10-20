from __future__ import annotations

from abc import ABCMeta
from copy import copy
from typing import Optional, Any, TYPE_CHECKING, Dict, Type, Set, Tuple

from datastore.datatypes import Key
from .basefield import Field

if TYPE_CHECKING:
    from ..client import Client


class KindMeta(ABCMeta):
    def __new__(mcs, class_name: str, bases: Tuple[type], attrs: dict, **kwargs: Any) -> Type[Kind]:
        if class_name is not "Kind":
            fields = {}
            noindex = set()

            for base in bases:
                if issubclass(base, Kind) and base._fields is not NotImplemented:
                    fields.update(copy(base._fields))
                    noindex |= base._noindex

            for name, attr in attrs.items():
                if isinstance(attr, Field):
                    if not attr._index:
                        noindex.add(name)
                    attr._update_meta(name=name)
                    fields[name] = attr

            store_none = attrs.get("_storeNone")
            if store_none is not None:
                for name, field in fields.items():
                    field._update_meta(store_none=store_none)

            attrs["_fields"] = fields
            attrs["_kind"] = attrs.get("_kind", class_name.lower())
            attrs["_noindex"] = noindex
        return super().__new__(mcs, class_name, bases, attrs, **kwargs)


class Kind(metaclass=KindMeta):
    _kind: str = None
    _storeNone: bool = None

    ds: Client = NotImplemented
    key: Key = NotImplemented
    _fields: Dict[str, Field] = NotImplemented
    _noindex: Set[str] = NotImplemented
    _v: Optional[str] = NotImplemented
    _backup_v: Optional[str] = NotImplemented
    _backup_key: Optional[Key] = NotImplemented

    def __init__(self, client: Client, namespace: str = None, id: Optional[int] = None, name: Optional[str] = None, prealocate: bool = False,
                 reserve: bool = False, **values: Any) -> None:

        if bool(id is not None) + bool(name is not None) + prealocate + reserve > 1:
            raise ValueError("Parameters \"id\", \"name\", \"reserve_id\", \"allocate_id\" are mutually exclusive")

        self._data = {}
        self._v = self._backup_v = self._backup_key = None
        self.ds = client

        self.key = Key(project=self.ds.project_id, kind=self._kind, namespace=namespace, id=id, name=name)
        if prealocate:
            self.preallocate()
        elif reserve:
            self.reserve()

        for name, field in self._fields.items():
            value = values.get(name)
            if field._required and value is None:
                raise ValueError("Required value is not present or is equal to None")
            setattr(self, name, value)

    def _backup(self) -> None:
        self._v = self._backup_v
        self.key._backup()

    def _rollback(self) -> None:
        if self._backup_v is not None:
            self._v = self._backup_v
            self.key._rollback()

    def _clear_backup(self) -> None:
        self._backup_v = None
        self.key._clear_backup()

    def _to_entity(self) -> dict:
        values = {}
        for name, field in self._fields.items():
            value = field._to_entity(self._data.get(name))
            if value is not None:
                values[name] = value
        if values:
            return {"key": self.key._entity, "properties": values}

    @classmethod
    def _from_entity(cls, entity: dict) -> Kind:
        entity = entity.get("entity")
        key = Key._from_entity(entity.get("key"))
        return cls(**{"namespace": key.namespace, key.id_type: key.id}, **entity.get("properties"))

    async def fetch(self):
        """
        Uses "lookup" API Call
        https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/lookup
        :return:
        """
        return self.lookup(key=self.key)

    async def update(self):
        """
        Uses "commit" API Call with mutation/operation "update"
        https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/commit#Mutation
        :return:
        """
        pass

    async def save(self):
        """
        Uses "commit" API Call with mutation/operation "upsert"
        https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/commit#Mutation
        :return:
        """
        pass

    async def insert(self):
        """
        Uses "commit" API Call with mutation/operation "insert"
        https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/commit#Mutation
        :return:
        """
        pass

    async def lookup(self, id: Optional[int] = None, name: Optional[str] = None, key: Optional[Key] = None):
        """
        Uses "lookup" API Call
        https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/lookup
        :param id:
        :param name:
        :param key:
        :return:
        """
        return self.ds.lookup(key=key)

    async def delete(self):
        """
        Uses "commit" API Call with mutation/operation "delete"
        https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/commit#Mutation
        :return:
        """
        pass

    async def reserve(self):
        """
        Uses "reserveIds" API Call
        https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/reserveIds
        :return:
        """
        pass

    async def preallocate(self):
        """
        Uses "allocateIds" API Call
        https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/allocateIds
        :return:
        """
        pass
