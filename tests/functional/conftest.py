import asyncio
import json
from dataclasses import dataclass

import aiohttp
import aioredis
import pytest
from elasticsearch import AsyncElasticsearch
from multidict import CIMultiDictProxy

from settings import Settings

settings = Settings()


@pytest.fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope='session')
async def redis_flush():
    redis = await aioredis.create_redis_pool((settings.redis_host, settings.redis_port), minsize=10, maxsize=20)
    await redis.flushall()
    yield
    redis.close()


@pytest.fixture(scope='session')
async def es_client():
    client = AsyncElasticsearch(hosts=settings.es_url)

    await client.indices.create('movies', body=json.load(open('/schemas/es.movies.schema.json')))
    await client.indices.create('genres', body=json.load(open('/schemas/es.persons.schema.json')))
    await client.indices.create('persons', body=json.load(open('/schemas/es.persons.schema.json')))

    yield client
    await client.indices.delete('movies')
    await client.indices.delete('genres')
    await client.indices.delete('persons')
    await client.close()


@pytest.fixture(scope='session')
async def session():
    session = aiohttp.ClientSession()
    yield session
    await session.close()
