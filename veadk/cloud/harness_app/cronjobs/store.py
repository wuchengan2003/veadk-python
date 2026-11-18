# Copyright (c) 2025 Beijing Volcano Engine Technology Co., Ltd. and/or its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""TOS JSON objects with atomic creation and ETag conditional replacement"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Callable


class Conflict(Exception):
    pass


class TosStore:
    def __init__(self, bucket: str, prefix: str, client_factory: Callable):
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.client_factory = client_factory
        self._slots = asyncio.Semaphore(32)

    async def _call(self, callback, *args):
        async with self._slots:
            return await asyncio.to_thread(callback, *args)

    def key(self, key: str) -> str:
        return f"{self.prefix}/{key}"

    async def get(self, key: str) -> tuple[dict, str] | None:
        return await self._call(self._get, key)

    def _get(self, key):
        try:
            response = self.client_factory().get_object(
                bucket=self.bucket, key=self.key(key)
            )
        except Exception as error:
            if getattr(error, "status_code", None) == 404:
                return None
            raise
        content = response.read()
        if len(content) > 4 * 1024 * 1024:
            raise ValueError("Cronjob object exceeds the storage limit")
        etag = str(response.etag).strip('"')
        if not etag:
            raise ValueError("TOS response has no ETag")
        return json.loads(content), etag

    async def put(self, key: str, value: dict, etag: str | None = None):
        return await self._call(self._put, key, value, etag)

    def _put(self, key, value, etag):
        content = json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode()
        if len(content) > 4 * 1024 * 1024:
            raise ValueError("Cronjob object exceeds the storage limit")
        try:
            response = self.client_factory().put_object(
                bucket=self.bucket,
                key=self.key(key),
                content=content,
                content_type="application/json",
                if_match=etag,
                forbid_overwrite=True if etag is None else None,
            )
            return str(response.etag).strip('"')
        except Exception as error:
            if getattr(error, "status_code", None) in {409, 412}:
                raise Conflict() from error
            raise

    async def list(
        self, prefix: str, *, cursor: str = "", limit: int = 1000
    ) -> tuple[list[str], str]:
        return await self._call(self._list, prefix, cursor, limit)

    def _list(self, prefix, cursor, limit):
        response = self.client_factory().list_objects_type2(
            bucket=self.bucket,
            prefix=self.key(prefix),
            max_keys=limit,
            continuation_token=cursor,
        )
        keys = [
            item.key[len(self.prefix) + 1 :]
            for item in (response.contents or [])
            if item.key.endswith(".json")
        ]
        return keys, (
            response.next_continuation_token or ""
        ) if response.is_truncated else ""

    async def probe(self):
        """Fail startup if list/read/write or conditional-write semantics are missing"""
        from uuid import uuid4

        key = f"probes/{uuid4().hex}.json"
        first = {"probe": 1}
        await self.put(key, first)
        try:
            try:
                await self.put(key, first)
            except Conflict:
                pass
            else:
                raise RuntimeError("TOS did not enforce atomic create")
            _, etag = await self.get(key)
            await self.put(key, {"probe": 2}, etag)
            try:
                await self.put(key, first, etag)
            except Conflict:
                pass
            else:
                raise RuntimeError("TOS did not enforce ETag conditions")
            await self.list("jobs/", limit=1)
        finally:
            await self._call(
                lambda: self.client_factory().delete_object(
                    bucket=self.bucket, key=self.key(key)
                )
            )


def store_from_env() -> TosStore:
    from ..tos_client import build_client_factory, tos_endpoint

    provider = os.getenv("AGENTKIT_CLOUD_PROVIDER", "volcengine")
    region = os.environ["HARNESS_CRONJOB_TOS_REGION"]
    endpoint = os.getenv("HARNESS_CRONJOB_TOS_ENDPOINT") or tos_endpoint(
        region, provider
    )
    client_factory = build_client_factory(
        region=region, endpoint=endpoint, provider=provider
    )
    return TosStore(
        os.environ["HARNESS_CRONJOB_TOS_BUCKET"],
        os.environ["HARNESS_CRONJOB_TOS_PREFIX"],
        client_factory,
    )
