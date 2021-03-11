import asyncio
from typing import List

import pytest
from aioredis import Redis

from elasticsearch import AsyncElasticsearch

from db.models import Film, Person


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


# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_person_list(make_get_request, es_client: AsyncElasticsearch,
                           persons: List):
    response = await make_get_request('/person')
    assert response.status == 200
    assert len(response.body) == 3


# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_person_search_single_match(make_get_request, es_client: AsyncElasticsearch,
                                          persons: List):
    response = await make_get_request('/person', {'query': 'Chris'})
    assert response.status == 200
    assert len(response.body) == 1
    assert response.body[0]['uuid'] == '0040371d-f875-4d42-ab17-ffaf3cacfb91'
    assert response.body[0]['full_name'] == 'Chris Cooper'
    assert response.body[0]['roles'] == ["actor"]
    assert response.body[0]['film_ids'] == ["93d538fe-1328-4b4c-a327-f61a80f25a3c"]


# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_person_search_multiple_match(make_get_request, es_client: AsyncElasticsearch,
                                            persons: List):
    response = await make_get_request('/person', {'query': 'June'})
    assert response.status == 200
    assert len(response.body) == 2
    expected_person_ids = {persons[1]['id'], persons[2]['id']}
    assert response.body[0]['uuid'] in expected_person_ids
    assert response.body[1]['uuid'] in expected_person_ids


# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_person_search_best_match(make_get_request, es_client: AsyncElasticsearch,
                                        persons: List):
    response = await make_get_request('/person', {'query': 'June Laverick2'})
    assert response.status == 200
    assert len(response.body) == 2
    assert response.body[0]['uuid'] == '0040371d-f875-4d42-ab17-ffaf3cacfb92'
    assert response.body[0]['full_name'] == 'June Laverick2'


# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_person_search_none_match(make_get_request, es_client: AsyncElasticsearch,
                                        persons: List):
    response = await make_get_request('/person', {'query': 'Some query'})
    assert response.status == 404


# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_person_search_limit_result(make_get_request,
                                          es_client: AsyncElasticsearch,
                                          persons: List):
    response = await make_get_request('/person', {'page[size]': 2})
    assert response.status == 200
    assert len(response.body) == 2


# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_person_search_paginated_and_limited_result(make_get_request, es_client: AsyncElasticsearch,
                                                          persons: List):
    response = await make_get_request('/person', {'page[size]': 2, 'page[number]': 2})
    assert response.status == 200
    assert len(response.body) == 1


# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_person_search_negative_pagination(make_get_request, es_client: AsyncElasticsearch, persons: List):
    response = await make_get_request('/person', {'page[size]': -1})
    assert response.status == 422


# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_person_search_too_big_pagination(make_get_request, es_client: AsyncElasticsearch, persons: List):
    response = await make_get_request('/person', {'page[size]': 2, 'page[number]': 4444})
    assert response.status == 404


# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_person_search_invalid_pagination_parameters(make_get_request, es_client: AsyncElasticsearch,
                                                           persons: List):
    response = await make_get_request('/person', {'page[size]': 'sdfsd', 'page[number]': 'sdfsd'})
    assert response.status == 422


# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_person_search_sorted_result(make_get_request, es_client: AsyncElasticsearch,
                                           persons: List):
    response = await make_get_request('/person', {'sort': '-full_name'})
    assert response.status == 200
    assert len(response.body) == 3
    result_persons = [person['full_name'] for person in response.body]
    expected_persons = sorted([person['full_name'] for person in persons], reverse=True)
    assert result_persons == expected_persons


# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_person_search_unknown_sort_field(make_get_request, es_client: AsyncElasticsearch,
                                                persons: List):
    response = await make_get_request('/person', {'sort': 'incorrect_sort'})
    assert response.status == 400


# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_person_search_invalid_sort_field(make_get_request, es_client: AsyncElasticsearch,
                                                persons: List):
    response = await make_get_request('/person', {'sort': '$^@#^@!%&*'})
    assert response.status == 422


# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_person_search_not_found(make_get_request, es_client: AsyncElasticsearch, persons: List):
    response = await make_get_request('/person', {'query': 'Something'})
    assert response.status == 404


# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_person_detail(make_get_request, es_client: AsyncElasticsearch, persons: List):
    lookup_person_id = persons[0]['id']
    person_detail_endpoint = f'/person/{lookup_person_id}'
    response = await make_get_request(person_detail_endpoint)

    assert response.status == 200
    assert response.body['uuid'] == '0040371d-f875-4d42-ab17-ffaf3cacfb91'
    assert response.body['full_name'] == 'Chris Cooper'
    assert response.body['roles'] == ["actor"]
    assert response.body['film_ids'] == ["93d538fe-1328-4b4c-a327-f61a80f25a3c"]


# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_person_detail_not_found(make_get_request, es_client: AsyncElasticsearch, persons: List):
    person_detail_endpoint = '/person/1'
    response = await make_get_request(person_detail_endpoint)

    assert response.status == 404


# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_person_with_single_films(make_get_request, es_client: AsyncElasticsearch,
                                        person_movies: List, persons: List):
    # Один фильм
    lookup_person_id = persons[0]['id']
    person_film_endpoint = f'/person/{lookup_person_id}/film'
    response = await make_get_request(person_film_endpoint)

    assert response.status == 200
    assert len(response.body) == 1
    assert response.body[0]['uuid'] == "93d538fe-1328-4b4c-a327-f61a80f25a3c"
    assert response.body[0]['title'] == 'Test Movie'


# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_person_with_multiple_film(make_get_request, es_client: AsyncElasticsearch, person_movies: List,
                                         persons: List):
    lookup_person_id = persons[1]['id']
    person_film_endpoint = f'/person/{lookup_person_id}/film'
    response = await make_get_request(person_film_endpoint)

    expected_film_ids = {person_movies[0]['id'], person_movies[1]['id']}

    assert response.status == 200
    assert len(response.body) == 2
    assert response.body[0]['uuid'] in expected_film_ids
    assert response.body[1]['uuid'] in expected_film_ids


# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_person_without_films(make_get_request, es_client: AsyncElasticsearch, person_movies: List,
                                    persons: List):
    lookup_person_id = persons[2]['id']
    person_film_endpoint = f'/person/{lookup_person_id}/film'
    response = await make_get_request(person_film_endpoint)

    assert response.status == 404


# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_redis_cache(make_get_request, redis: Redis, persons: List):
    await redis.flushall()
    assert not (await redis.keys("Person:query:*"))
    response = await make_get_request('/person', {'query': 'Chris'})

    assert response.status == 200
    assert await redis.keys("Person:query:*")
