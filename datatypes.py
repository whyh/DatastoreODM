from __future__ import annotations

from typing import Any, Iterable, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .odm.basefield import Field


class Key:
    def __init__(self, project: str, kind: str, namespace: Optional[str] = None, id: Optional[int] = None, name: Optional[str] = None) -> None:
        self.kind = kind
        self.project = project
        self.namespace = namespace
        self.partial = True
        self._backup_id = None
        self._backup_id_type = None

        try:
            self._complete(id, name)
        except ValueError:
            self.id = None
            self.id_type = None

    def _backup(self) -> None:
        self._backup_id = self.id
        self._backup_id_type = self.id_type

    def _rollback(self) -> None:
        if self._backup_id is not None:
            self.id = self._backup_id
        if self._backup_id_type is not None:
            self.id_type = self._backup_id_type

    def _clear_backup(self) -> None:
        self._backup_id = None
        self._backup_id_type = None

    @property
    def _entity(self) -> dict:
        partition = {"projectId": self.project, "namespaceId": self.namespace}
        path = [{"kind": self.kind}]
        if self.id_type is not None:
            path[0].update({self.id_type: self.id})
        return {"partitionId": partition, "path": path}

    @classmethod
    def _from_entity(cls, entity: dict) -> Key:
        partition = entity.get("partitionId")
        path = entity.get("path")[0]
        return cls(project=partition.get("projectId"), namespace=partition.get("namespaceId"), id=path.get("id"), name=path.get("name"),
                   kind=path.get("kind"))

    def _complete(self, id: Optional[int] = None, name: Optional[str] = None) -> None:
        if not self.partial:
            raise TypeError("Key is not partial. Only partial key can be completed")
        if not (id is None or name is None) or (id is None and name is None):
            raise ValueError("Either \"id\" or \"name\" should be specified to complete a Key")

        if id is not None:
            self.id = id
            self.id_type = "id"
        else:
            self.id = name
            self.id_type = "name"

    def _uncomplete(self) -> None:
        self.id = None
        self.id_type = None
        self.partial = True


class Array(list):
    def __init__(self, content: Field, iterable: Iterable) -> None:
        self._content = content
        super().__init__(self._content._mold(value) for value in iterable)

    def __setitem__(self, key: int, value: Any) -> None:
        super().__setitem__(key, self._content._mold(value))

    def append(self, value: Any) -> None:
        super().append(self._content._mold(value))

    def extend(self, iterable: Iterable) -> None:
        super().extend(self._content._mold(value) for value in iterable)

    def insert(self, index: int, value: Any) -> None:
        super().insert(index, self._content._mold(value))


class Location:
    def __init__(self, latitude: float, longitude: float) -> None:
        self.latitude = latitude
        self.longitude = longitude
