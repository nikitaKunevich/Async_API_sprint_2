import asyncio
import json
from typing import List

import aiohttp
import aioredis
import pytest

from dataclasses import dataclass

from aioredis import Redis
from multidict import CIMultiDictProxy
from elasticsearch import AsyncElasticsearch

from db.models import Film, Person
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
    await client.indices.create('genres', body=json.load(open('/schemas/es.genres.schema.json')))
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


@pytest.fixture
async def persons(es_client: AsyncElasticsearch):
    persons = [
        {
            "id": "0040371d-f875-4d42-ab17-ffaf3cacfb91",
            "full_name": "Chris Cooper",
            "roles": [
                "actor"
            ],
            "film_ids": [
                "93d538fe-1328-4b4c-a327-f61a80f25a3c"
            ]
        },
        {
            "id": "0040371d-f875-4d42-ab17-ffaf3cacfb92",
            "full_name": "June Laverick2",
            "roles": [
                "actor"
            ],
            "film_ids": [
                "93d538fe-1328-4b4c-a327-f61a80f25a3c",
                "93d538fe-1328-4b4c-a327-f61a80f20000"
            ]
        },
        {
            "id": "00573d04-34ba-4b52-808c-49c428af704d",
            "full_name": "June Laverick",
            "roles": [
                "actor"
            ],
            "film_ids": []
        }
    ]

    await asyncio.gather(
        *[es_client.index('persons', body=Person.parse_obj(person).dict(), id=person['id'], refresh='wait_for') for
          person in persons])
    return persons


@pytest.fixture
async def person_movies(es_client: AsyncElasticsearch):
    films = [
        {
            'id': '93d538fe-1328-4b4c-a327-f61a80f25a3c',
            'title': 'Test Movie',
            'actors_names': [],
            'writers_names': [],
            'directors_names': [],
            'genres_names': [],
            'actors': [],
            'writers': [],
            'directors': [],
            'genres': [],
        },
        {
            'id': '93d538fe-1328-4b4c-a327-f61a80f20000',
            'title': 'Dummy Movie',
            'actors_names': [],
            'writers_names': [],
            'directors_names': [],
            'genres_names': [],
            'actors': [],
            'writers': [],
            'directors': [],
            'genres': [],
        },
    ]
    await asyncio.gather(
        *[es_client.index('movies', body=Film.parse_obj(film).dict(), id=film['id'], refresh='wait_for') for film in
          films])
    return films


@pytest.mark.asyncio
async def test_person_search_single_match(session: aiohttp.ClientSession, es_client: AsyncElasticsearch,
                                          persons: List):
    # Выполнение запроса
    response = await make_get_request(session, '/person', {'query': 'Chris'})
    # Проверка результата
    assert response.status == 200
    assert len(response.body) == 1
    assert response.body[0]['uuid'] == '0040371d-f875-4d42-ab17-ffaf3cacfb91'
    assert response.body[0]['full_name'] == 'Chris Cooper'
    assert response.body[0]['roles'] == ["actor"]
    assert response.body[0]['film_ids'] == ["93d538fe-1328-4b4c-a327-f61a80f25a3c"]
    # assert response.body == expected


@pytest.mark.asyncio
async def test_person_search_multiple_match(session: aiohttp.ClientSession, es_client: AsyncElasticsearch,
                                            persons: List):
    # Выполнение запроса
    response = await make_get_request(session, '/person', {'query': 'June'})
    # Проверка результата
    assert response.status == 200
    assert len(response.body) == 2
    expected_ids = [persons[1]['id'], persons[2]['id']]
    assert response.body[0]['uuid'] in expected_ids
    assert response.body[1]['uuid'] in expected_ids
    # assert response.body == expected


@pytest.mark.asyncio
async def test_person_search_not_found(session: aiohttp.ClientSession, es_client: AsyncElasticsearch, persons: List):
    # Выполнение запроса
    response = await make_get_request(session, '/person', {'query': 'Something'})
    # Проверка результата
    assert response.status == 404
    # assert response.body == expected


@pytest.mark.asyncio
async def test_person_detail(session: aiohttp.ClientSession, es_client: AsyncElasticsearch, persons: List):
    looked_person_id = persons[0]['id']
    person_detail_endpoint = f'/person/{looked_person_id}'
    response = await make_get_request(session, person_detail_endpoint)

    assert response.status == 200
    assert response.body['uuid'] == '0040371d-f875-4d42-ab17-ffaf3cacfb91'
    assert response.body['full_name'] == 'Chris Cooper'
    assert response.body['roles'] == ["actor"]
    assert response.body['film_ids'] == ["93d538fe-1328-4b4c-a327-f61a80f25a3c"]
    # assert response.body == expected


@pytest.mark.asyncio
async def test_person_detail_not_found(session: aiohttp.ClientSession, es_client: AsyncElasticsearch, persons: List):
    person_detail_endpoint = '/person/1'
    response = await make_get_request(session, person_detail_endpoint)

    assert response.status == 404


@pytest.mark.asyncio
async def test_person_films_single_match(session: aiohttp.ClientSession, es_client: AsyncElasticsearch,
                                         person_movies: List, persons: List):
    looked_person_id = persons[0]['id']
    person_film_endpoint = f'/person/{looked_person_id}/film'
    response = await make_get_request(session, person_film_endpoint)

    assert response.status == 200
    assert len(response.body) == 1
    assert response.body[0]['uuid'] == "93d538fe-1328-4b4c-a327-f61a80f25a3c"
    assert response.body[0]['title'] == 'Test Movie'


@pytest.mark.asyncio
async def test_person_films_multiple_match(session: aiohttp.ClientSession, es_client: AsyncElasticsearch,
                                           person_movies: List, persons: List):
    looked_person_id = persons[1]['id']
    person_film_endpoint = f'/person/{looked_person_id}/film'
    response = await make_get_request(session, person_film_endpoint)

    expected_film_ids = [person_movies[0]['id'], person_movies[1]['id']]

    assert response.status == 200
    assert len(response.body) == 2
    assert response.body[0]['uuid'] in expected_film_ids
    assert response.body[1]['uuid'] in expected_film_ids
