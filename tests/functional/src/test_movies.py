import asyncio
import json

import aiohttp
import aioredis
import pytest

from dataclasses import dataclass

from aioredis import Redis
from multidict import CIMultiDictProxy
from elasticsearch import AsyncElasticsearch

from db.models import Film
from settings import Settings

settings = Settings()


@dataclass
class HTTPResponse:
    body: dict
    headers: CIMultiDictProxy[str]
    status: int

@pytest.fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope='session')
async def redis_flush():
    redis = await aioredis.create_redis_pool((settings.REDIS_HOST, settings.REDIS_PORT), minsize=10, maxsize=20)
    await redis.flushall()
    yield

@pytest.fixture(scope='session')
async def es_client():
    client = AsyncElasticsearch(hosts=settings.es_url)
    try:
        await client.indices.delete('movies')
        await client.indices.delete('genres')
        await client.indices.delete('persons')
    except:
        pass

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


async def make_get_request(session, method: str, params: dict = None) -> HTTPResponse:
    params = params or {}
    url = settings.api_host + '/v1' + method  # в боевых системах старайтесь так не делать!
    async with session.get(url, params=params) as response:
        return HTTPResponse(
            body=await response.json(),
            headers=response.headers,
            status=response.status,
        )

@pytest.mark.asyncio
async def test_search_detailed(session: aiohttp.ClientSession, es_client: AsyncElasticsearch):
    # Заполнение данных для теста
    films = [{
        'id': '08293f6d-90e4-467d-b2c3-ee85b5b0d326',
        'title': 'Night Owl',
        'actors_names': [],
        'writers_names': [],
        'directors_names': [],
        'genres_names': [],
        'actors': [],
        'writers': [],
        'directors': [],
        'genres': [],
    }, {
        'id': 'd3a8967d-9601-4b96-a43c-6edfc0dd372b',
        'title': 'Star was Born',
        'actors_names': [],
        'writers_names': [],
        'directors_names': [],
        'genres_names': [],
        'actors': [],
        'writers': [],
        'directors': [],
        'genres': [],
    }]

    film_models = []
    for film in films:
        model = Film.parse_obj(film)
        film_models.append(model)
        await es_client.index('movies', body=model.dict())

    # Выполнение запроса
    response = await make_get_request(session, '/film/', {'query': 'Star'})
    # Проверка результата
    assert response.status == 200
    assert len(response.body) == 1
    assert response.body['id'] == 'd3a8967d-9601-4b96-a43c-6edfc0dd372b'
    print(response.body)
    # assert response.body == expected
