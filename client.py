from __future__ import annotations

import functools
from typing import Optional, Union, Tuple, Iterable, Set, Dict, Type
from urllib.parse import urlencode
from time import time
from os import getenv
import asyncio
import json

from aiohttp import ClientSession, TCPConnector
import jwt

from .datatypes import Key
from .odm.kind import Kind
from .errors import TransactionFailed

REQUEST_URL = "https://datastore.googleapis.com/v1/projects/{project_id}:{method}"

TOKEN_LIFETIME_S = 3600
TOKEN_RENEW_BEFOREHAND_S = 100


class Client:
    Batch: Type[Batch] = NotImplemented
    Transaction: Type[Transaction] = NotImplemented
    _session: ClientSession = NotImplemented
    __update_token_loop: asyncio.Task = NotImplemented

    def __init__(self, credentials: Optional[str] = None) -> None:
        """
        :param credentials: path to the service account credentials
        If not present will be attempted to receive with env GOOGLE_APPLICATION_CREDENTIALS
        This env will be automatically set if you use AppEngine or emulator of it
        """
        with open(getenv("GOOGLE_APPLICATION_CREDENTIALS") if credentials is None else credentials) as file:
            credentials = json.load(file)
            self.project_id = credentials.pop("project_id")
            self.__token_uri = credentials.get("token_uri")
            self.__private_key = credentials.get("private_key")
            self.__client_email = credentials.get("client_email")

        url = REQUEST_URL.format(project_id=self.project_id, method="{method}")
        self.__allocate_ids_url = url.format(method="allocateIds")
        self.__commit_url = url.format(method="commit")
        self.__lookup_url = url.format(method="lookup")
        self.__reserve_ids_url = url.format(method="reserveIds")
        self.__run_query_url = url.format(method="runQuery")

        self.connected = False
        self.Batch = type("Batch", (Batch,), {"__ds": self})
        self.Transaction = type("Transaction", (Transaction,),
                                {"__ds": self,
                                 "__rollback_url": url.format(method="rollback"),
                                 "__begin_transaction_url": url.format(method="beginTransaction")})

    async def _execute(self, transaction: Optional[str] = None, update: Optional[Iterable[Kind, ...]] = None,
                       save: Optional[Iterable[Kind, ...]] = None, insert: Optional[Iterable[Kind, ...]] = None,
                       delete: Optional[Iterable[Key, ...]] = None) -> Set[Union[Kind, Key]]:

        def normalize(*ops: Optional[Iterable[Union[Kind, Key], ...]]) -> Iterable[Union[Kind, Key], ...]:
            return (() if op is None else op for op in ops)

        def mutations(**ops: Iterable[Union[Kind, Key, ...]]) -> Tuple[Dict[str: dict], ...]:
            return tuple(mutation for group in (({method: op._to_entity(), "baseVersion": op._v} for op in operands)
                                                for method, operands in ops.items()) for mutation in group)

        update, save, insert, delete = normalize(update, save, insert, delete)
        data = {"mode": "NON_TRANSACTIONAL" if transaction is None else "TRANSACTIONAL",
                "mutations": mutations(update=update, upsert=save, insert=insert, delete=delete)}

        conflict = set()
        async with self._session.post(self.__commit_url, json=data) as response:
            print(await response.json())
            if response.status is 200:
                response = (await response.json())["mutationResults"]
                for ops in update, save, insert, delete:
                    for op in ops:
                        mutation = response.pop(0)

                        if transaction is not None:
                            op._backup()

                        if mutation.get("conflictDetected"):
                            conflict.add(op)

                        op._v = mutation["version"]
                        key = mutation.get("key")
                        if key is not None:
                            op.key = Key._from_entity(key)

                return conflict

    async def _lookup(self) -> :

    async def connect(self) -> None:
        async def update_token_loop() -> None:
            async with ClientSession(headers={"content-type": "application/x-www-form-urlencoded"}) as session:
                while True:
                    now = int(time())
                    payload = {"aud": self.__token_uri,
                               "iss": self.__client_email,
                               "scope": "https://www.googleapis.com/auth/datastore",
                               "iat": now,
                               "exp": now + TOKEN_LIFETIME_S}
                    data = {"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                            "assertion": jwt.encode(payload, self.__private_key, algorithm="RS256")}

                    async with session.post(self.__token_uri, data=urlencode(data)) as response:
                        if response.status is 200:
                            response = await response.json()

                            self.connected = True
                            self._session._default_headers.update(Authorization="Bearer " + response.get("access_token"))

                            timeout = response.get("expires_in")
                            if timeout > TOKEN_RENEW_BEFOREHAND_S:
                                await asyncio.sleep(timeout - TOKEN_RENEW_BEFOREHAND_S)

        self._session = ClientSession(connector=TCPConnector(limit=0), headers={"Content-Type": "application/json"})
        self.__update_token_loop = asyncio.create_task(update_token_loop())
        while not self.connected:
            await asyncio.sleep(0.01)

    async def disconnect(self) -> None:
        if not self.connected:
            raise ConnectionError("Client is not connected")

        self.__update_token_loop.cancel()
        self.connected = False
        await self._session.close()
        self.__update_token_loop = self._session = NotImplemented

    async def preallocate(self, *partial_keys: Key) -> Optional[Tuple[Key]]:
        async with self._session.post(self.__allocate_ids_url, json={"keys": partial_keys}) as response:
            if response.status is 200:
                return (await response.json()).get("keys")

    async def reserve(self, *keys: Key) -> bool:
        async with self._session.post(self.__reserve_ids_url, json={"keys": keys}) as response:
            return True if response.status is 200 else False

    async def lookup_multiple(self, *keys: Union[Key, Kind], eventual: bool = False, transaction: Optional[str] = None,
                              generator: bool = False) -> Optional[Union[Kind, Tuple[Kind]]]:
        entities = []
        data_tpl = {"readOptions": {"readConsistency": "EVENTUAL" if eventual else "STRONG"} if transaction is None
                    else {"transaction": transaction}}

        while True:
            async with self._session.post(self.__lookup_url, json=data_tpl.update(keys=keys)) as response:
                if response.status is 200:
                    response = await response.json()
                    print(response)
                    entities.extend(response.get("found", []))
                    keys = response.get("deferred", [])

                    if generator:
                        while entities:
                            yield Kind._from_entity(entities.pop(0))
                    if not keys:
                        if entities:
                            yield (Kind._from_entity(entity) for entity in entities)
                        return

    def transaction(self, read_only: bool = False, retry_max: Optional[int] = 0, retry_timeout: Optional[float] = None):
        def wrap_wrap(function):
            @functools.wraps(function)
            async def wrap(*args, **kwargs):
                retry_of = None
                while True:
                    try:
                        async with self.Transaction(read_only=read_only, retry=retry_of) as trans:
                            return await function(trans, *args, **kwargs)
                    except TransactionFailed as error:
                        if retry_max is None or retry_max > 0:
                            retry_of = error.id
                            if retry_timeout is not None:
                                await asyncio.sleep(retry_timeout)
                        else:
                            raise error

            return wrap

        return wrap_wrap

    async def run_query(self, data):
        results = []
        start_cursor = None
        while True:
            async with aiohttp.ClientSession() as session:

                if start_cursor is not None:
                    data['query']['startCursor'] = start_cursor

                async with session.post(
                        self.__run_query_url,
                        data=json.dumps(data),
                        headers=await self._get_headers()) as resp:

                    content = await resp.json()

                    if resp.status == 200:

                        entity_results = \
                            content['batch'].get('entityResults', [])

                        results.extend(entity_results)

                        more_results = content['batch']['moreResults']

                        if more_results in (
                                'NO_MORE_RESULTS',
                                'MORE_RESULTS_AFTER_LIMIT',
                                'MORE_RESULTS_AFTER_CURSOR'):
                            break

                        if more_results == 'NOT_FINISHED':
                            start_cursor = content['batch']['endCursor']
                            continue

                        raise ValueError(
                            'Unexpected value for "moreResults": {}'
                                .format(more_results))

                    raise ValueError(
                        'Error while query the datastore: {} ({})'
                            .format(
                            content.get('error', 'unknown'),
                            resp.status
                        )
                    )

        return results



class Batch:
    __ds: Client = NotImplemented

    def __init__(self) -> None:
        self.__insert, self.__update, self.__save, self.__delete = set(), set(), set(), set()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.__ds._execute(update=self.__update, save=self.__save, insert=self.__insert, delete=self.__delete)

    def insert(self, entity: Kind) -> None:
        self.__insert.add(entity)

    def update(self, entity: Kind) -> None:
        self.__update.add(entity)

    def save(self, entity: Kind) -> None:
        self.__save.add(entity)

    def delete(self, entity: Union[Kind, Key]) -> None:
        self.__delete.add(entity)


class Transaction(Batch):
    __ds: Client = NotImplemented
    __begin_transaction_url: str = NotImplemented
    __rollback_url: str = NotImplemented
    __id: str = NotImplemented
    __conflict: Set[Union[Kind, Key], ...] = NotImplemented

    def __init__(self, read_only: bool = False, retry: Optional[str] = None):
        self.__read_only = read_only
        self.__retry = retry
        super().__init__()

    async def __aenter__(self):
        options = {"readOnly" if self.__read_only else "readWrite": {"previousTransaction": self.__retry}}
        async with self.__session.post(self.__begin_transaction_url, json={"transactionOptions": options}) as response:
            if response.status is not 200:
                raise Exception
            self.__id = (await response.json())["transaction"]

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if not self.__read_only:
            self.__conflict = await self.__ds._execute(transaction=self.__id, update=self.__update, save=self.__save,
                                                       insert=self.__insert, delete=self.__delete)
            if self.__conflict:
                async with self.__session.post(self.__rollback_url, json={"transaction": self.__id}) as response:
                    if response is not 200:
                        raise Exception

                for entity in self.__conflict:
                    entity._rollback()
                    entity._clear_backup()

                raise TransactionFailed(self.__id, "Transaction failed")
            else:
                for entity in self.__conflict:
                    entity._clear_backup()

    @property
    def __session(self):
        return self.__ds._session

    async def lookup(self, *entities: Union[Kind, Key]) -> Optional[Tuple[Kind, ...]]:
        return ...