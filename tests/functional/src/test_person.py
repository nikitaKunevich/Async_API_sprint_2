import asyncio
from typing import List

import aiohttp
import pytest
from aioredis import Redis

from elasticsearch import AsyncElasticsearch

from db.models import Film, Person
from settings import Settings

from utils import make_get_request

settings = Settings()


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
async def test_person_list(session: aiohttp.ClientSession, es_client: AsyncElasticsearch,
                           persons: List):
    response = await make_get_request(session, '/person')
    assert response.status == 200
    assert len(response.body) == 3


@pytest.mark.asyncio
async def test_person_search(session: aiohttp.ClientSession, es_client: AsyncElasticsearch,
                             persons: List):
    # Единственное совпадение
    response = await make_get_request(session, '/person', {'query': 'Chris'})
    assert response.status == 200
    assert len(response.body) == 1
    assert response.body[0]['uuid'] == '0040371d-f875-4d42-ab17-ffaf3cacfb91'
    assert response.body[0]['full_name'] == 'Chris Cooper'
    assert response.body[0]['roles'] == ["actor"]
    assert response.body[0]['film_ids'] == ["93d538fe-1328-4b4c-a327-f61a80f25a3c"]

    # Несколько совпадений
    response = await make_get_request(session, '/person', {'query': 'June'})
    assert response.status == 200
    assert len(response.body) == 2
    expected_person_ids = {persons[1]['id'], persons[2]['id']}
    assert response.body[0]['uuid'] in expected_person_ids
    assert response.body[1]['uuid'] in expected_person_ids

    # Лучшее совпадение
    response = await make_get_request(session, '/person', {'query': 'June Laverick2'})
    assert response.status == 200
    assert len(response.body) == 2
    assert response.body[0]['uuid'] == '0040371d-f875-4d42-ab17-ffaf3cacfb92'
    assert response.body[0]['full_name'] == 'June Laverick2'


@pytest.mark.asyncio
async def test_person_search_paginated_and_limited_result(session: aiohttp.ClientSession,
                                                          es_client: AsyncElasticsearch,
                                                          persons: List):
    response = await make_get_request(session, '/person', {'page[size]': 2})
    assert response.status == 200
    assert len(response.body) == 2

    response = await make_get_request(session, '/person', {'page[size]': 2, 'page[number]': 2})
    assert response.status == 200
    assert len(response.body) == 1

    response = await make_get_request(session, '/person', {'page[size]': -1})
    assert response.status == 422

    response = await make_get_request(session, '/person', {'page[size]': 2, 'page[number]': 4444})
    assert response.status == 404


@pytest.mark.asyncio
async def test_person_search_sorted_result(session: aiohttp.ClientSession, es_client: AsyncElasticsearch,
                                           persons: List):
    response_films_star_trek = await make_get_request(session, '/person', {'sort': 'incorrect_sort'})
    assert response_films_star_trek.status == 404

    response_films_star_trek = await make_get_request(session, '/person', {'sort': '-full_name'})
    assert response_films_star_trek.status == 200
    assert len(response_films_star_trek.body) == 3


@pytest.mark.asyncio
async def test_person_search_not_found(session: aiohttp.ClientSession, es_client: AsyncElasticsearch, persons: List):
    response = await make_get_request(session, '/person', {'query': 'Something'})
    assert response.status == 404


@pytest.mark.asyncio
async def test_person_detail(session: aiohttp.ClientSession, es_client: AsyncElasticsearch, persons: List):
    lookup_person_id = persons[0]['id']
    person_detail_endpoint = f'/person/{lookup_person_id}'
    response = await make_get_request(session, person_detail_endpoint)

    assert response.status == 200
    assert response.body['uuid'] == '0040371d-f875-4d42-ab17-ffaf3cacfb91'
    assert response.body['full_name'] == 'Chris Cooper'
    assert response.body['roles'] == ["actor"]
    assert response.body['film_ids'] == ["93d538fe-1328-4b4c-a327-f61a80f25a3c"]


@pytest.mark.asyncio
async def test_person_detail_not_found(session: aiohttp.ClientSession, es_client: AsyncElasticsearch, persons: List):
    person_detail_endpoint = '/person/1'
    response = await make_get_request(session, person_detail_endpoint)

    assert response.status == 404


@pytest.mark.asyncio
async def test_person_films(session: aiohttp.ClientSession, es_client: AsyncElasticsearch,
                            person_movies: List, persons: List):
    # Один фильм
    lookup_person_id = persons[0]['id']
    person_film_endpoint = f'/person/{lookup_person_id}/film'
    response = await make_get_request(session, person_film_endpoint)

    assert response.status == 200
    assert len(response.body) == 1
    assert response.body[0]['uuid'] == "93d538fe-1328-4b4c-a327-f61a80f25a3c"
    assert response.body[0]['title'] == 'Test Movie'

    # Несколько фильмов
    lookup_person_id = persons[1]['id']
    person_film_endpoint = f'/person/{lookup_person_id}/film'
    response = await make_get_request(session, person_film_endpoint)

    expected_film_ids = {person_movies[0]['id'], person_movies[1]['id']}

    assert response.status == 200
    assert len(response.body) == 2
    assert response.body[0]['uuid'] in expected_film_ids
    assert response.body[1]['uuid'] in expected_film_ids

    # Ни одного фильма
    lookup_person_id = persons[2]['id']
    person_film_endpoint = f'/person/{lookup_person_id}/film'
    response = await make_get_request(session, person_film_endpoint)

    assert response.status == 404


@pytest.mark.asyncio
async def test_redis_cache(session: aiohttp.ClientSession, redis: Redis, persons: List):
    await redis.flushall()
    assert not (await redis.keys("Person:query:*"))
    response = await make_get_request(session, '/person', {'query': 'Chris'})

    assert response.status == 200
    assert await redis.keys("Person:query:*")
