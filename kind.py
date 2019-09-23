import re
from collections import deque
from typing import Optional, Union, Tuple

from google.cloud.datastore import Entity, Transaction

from . import db
from .basefields import BaseField

__all__ = ("Kind",)

reserved_fields = ("id", "key", "entity", "from_entity", "save", "unlock", "find", "find_or_create", "find_where",
                   "delete")


def valid_kind(kind: Optional[str]) -> bool:
    return True if kind and re.fullmatch(r"[a-z]+", kind) else False


class LockField(BaseField):
    def __init__(self, default=False):
        super().__init__(default=default)

    @staticmethod
    def _validate(value):
        if value is not None and type(value) is not bool:
            raise TypeError


class KindMeta(type):
    def __new__(mcs, name, bases, attrs):
        kind = attrs.pop("_kind", None)
        if not valid_kind(kind):
            kind = name.lower()
            if not valid_kind(kind):
                raise ValueError

        if name is not "Kind":
            attrs_keys = list(attrs.keys())
            for name in reserved_fields:
                if name in attrs_keys:
                    raise NameError

            fields = {}
            exclude_from_index = []

            for base in bases:
                if base is not Kind and issubclass(base, Kind):
                    fields.update(base._fields)
                    exclude_from_index.extend(base._exclude_from_index)

            for attr_name, attr in attrs.items():
                if isinstance(attr, BaseField):
                    if attr_name[0] == "_":
                        raise NameError
                    else:
                        if not attr._index:
                            exclude_from_index.append(attr_name)

                        attr._name = attr_name
                        fields[attr_name] = attr

            if len(fields) == 0:
                raise AttributeError

            if attrs.get("_p_lock", False):
                attr = LockField()
                attr_name = "_locked"
                attr._name = attr_name
                fields[attr_name] = attr
                exclude_from_index.append(attr_name)
                attrs[attr_name] = attr

            attrs["_fields"] = fields
            attrs["_exclude_from_index"] = tuple(exclude_from_index)
            attrs["_kind"] = kind
            attrs["_partial_key"] = db.client.key(kind)

        return super().__new__(mcs, name, bases, attrs)


class Kind(metaclass=KindMeta):
    _fields = None
    _exclude_from_index = None
    _kind = None
    _partial_key = None
    _p_lock = False

    def __init__(self, *args, **kwargs):
        args = deque(args)
        self._data = {}

        self.id = kwargs.pop("id", db.client.allocate_ids(self._partial_key, 1)[0].id)
        self.key = self._partial_key.completed_key(self.id)

        for name, field in self._fields.items():
            value = kwargs.get(name, None) if len(args) == 0 else args.popleft()
            setattr(self, name, value)

    @property
    def entity(self):
        return self.__to_entity()

    def __to_entity(self) -> Optional[Entity]:
        entity = Entity(self.key, self._exclude_from_index)
        for name, field in self._fields.items():
            value = field._db_repr(self._data.get(name))
            if value is not None:
                entity[name] = value
        return None if len(entity) == 0 else entity

    @classmethod
    def from_entity(cls, entity: Entity) -> "Kind":
        return cls(id=entity.id, **entity)

    def save(self):
        entity = self.entity
        if entity is not None:
            db.client.put(entity)

    def unlock(self):
        return self.save()

    @classmethod
    def __acquire_lock(cls, entity: Entity, batch: Transaction) -> "Kind":
        kind = cls.from_entity(entity)
        if cls._p_lock and kind._locked:
            raise LookupError
        else:
            if cls._p_lock:
                kind._locked = True
                batch.put(kind.entity)
                kind._locked = None
            return kind

    @classmethod
    def find(cls, id: Optional[int]) -> Optional["Kind"]:
        @db.transaction(retry=True)
        def find_unlocked(cls: "Kind", id: int, batch: Transaction) -> Optional["Kind"]:
            entity = db.client.get(db.client.key(cls._kind, id))

            if entity is not None:
                return cls.__acquire_lock(entity, batch)

        if id is not None:
            return find_unlocked(cls, id)

    @classmethod
    def find_or_create(cls, id: Optional[int], *args, **kwargs) -> "Kind":
        kind = cls.find(id)
        return cls(*args, **kwargs) if kind is None else kind

    @classmethod
    def find_where(cls, _quantity: Optional[int] = None, **kwargs) -> Optional[Union[Tuple["Kind"], "Kind"]]:
        @db.transaction(retry=True)
        def fetch_unlocked(batch: Transaction) -> Optional[Union[Tuple["Kind"], "Kind"]]:
            fetched = tuple(cls.__acquire_lock(entity, batch) for entity in query.fetch(limit=_quantity))
            if _quantity == 1:
                try:
                    return fetched[0]
                except IndexError:
                    return
            return fetched

        query = db.client.query(kind=cls._kind)
        for name, value in kwargs.items():
            if name in cls._fields:
                query.add_filter(name, "=", value)
            else:
                raise NameError

        return fetch_unlocked()

    def delete(self):
        db.client.delete(self.key)
