from operator import attrgetter
from typing import List

import aiohttp
import aioredis
import pydantic
import pytest
from aiohttp import ClientResponseError

from elasticsearch import AsyncElasticsearch

import api_v1.models
import db.models
from utils import make_get_request

"""
Тест-план
все граничные случаи по валидации данных;
поиск конкретного жанра;
вывести все жанры;
поиск с учётом кеша в Redis.
"""


@pytest.fixture(scope='session')
def test_genres():
    action_films = [
        db.models.FilmShort(id='a7334652-f73e-4f2b-88dd-f3ef08ee4cea', title='Action One', imdb_rating=5.5),
        db.models.FilmShort(id='c66258d4-9a34-47d0-85cc-5d4584090207', title='Action Two', imdb_rating=4.5),
    ]
    documentary_films = [
        db.models.FilmShort(id='0c13e610-84f3-4573-9301-2df5235fe627', title='Doc One', imdb_rating=1.5),
        db.models.FilmShort(id='a5a69721-a1ba-4d74-a1f9-905ddaac0eed', title='Doc Two', imdb_rating=9.5),
    ]
    horror_films = [
        db.models.FilmShort(id='a7334652-f73e-4f2b-88dd-f3ef08ee4cea', title='Action One', imdb_rating=5.5),
    ]
    genres = [
        db.models.Genre(id='5017d3c9-3cb5-4cd1-a329-3c99a253bcf3', name='Action', filmworks=action_films),
        db.models.Genre(id='59e89fb7-639c-41fa-b829-a9261bad1114', name='Documentary', filmworks=documentary_films),
        db.models.Genre(id='9854793d-bb4f-45bf-8b2f-934bbf42adc9', name='Horror', filmworks=horror_films),
    ]
    return genres


@pytest.mark.asyncio
async def test_can_get_all_genres(session: aiohttp.ClientSession, es_client: AsyncElasticsearch, test_genres):
    # GIVEN multiple genres
    for genre in test_genres:
        await es_client.index('genres', id=genre.id, body=genre.dict(), refresh='wait_for')

    # WHEN query for all genres
    all_genres_response = await make_get_request(session, '/genre/')
    received = pydantic.parse_obj_as(List[api_v1.models.GenreDetail], all_genres_response.body)

    # THEN all genres are returned
    expected = [api_v1.models.GenreDetail.from_db_model(genre) for genre in test_genres]
    assert expected == received


@pytest.mark.asyncio
async def test_genres_sort(session: aiohttp.ClientSession, es_client: AsyncElasticsearch, test_genres):
    # GIVEN some genres
    for genre in test_genres:
        await es_client.index('genres', id=genre.id, body=genre.dict(), refresh='wait_for')

    # WHEN sorted by name DESC
    all_genres_response = await make_get_request(session, '/genre/', {"sort": "-name"})

    # THEN all genres are returned by name DESC
    received = pydantic.parse_obj_as(List[api_v1.models.GenreDetail], all_genres_response.body)
    expected = [api_v1.models.GenreDetail.from_db_model(genre) for genre in test_genres]
    expected = sorted(expected, key=attrgetter('name'))
    assert expected == received


@pytest.mark.asyncio
async def test_genres_query_in_cache(session: aiohttp.ClientSession, es_client: AsyncElasticsearch,
                                     redis: aioredis.Redis, test_genres):
    # GIVEN some genres
    for genre in test_genres:
        await es_client.index('genres', id=genre.id, body=genre.dict(), refresh='wait_for')
    page_number = 1
    page_size = 2
    first_two_genres_response = await make_get_request(session, '/genre/',
                                                       {'page[size]': page_size, 'page[number]': page_number})
    received_api_models = pydantic.parse_obj_as(List[api_v1.models.GenreDetail], first_two_genres_response.body)
    redis_key = "Genre:query:{'from': 0, 'size': 2}"
    cached_value = await redis.get(redis_key)
    assert cached_value
    cached_models = pydantic.parse_raw_as(List[db.models.Genre], cached_value)
    assert received_api_models == [api_v1.models.GenreDetail.from_db_model(genre) for genre in cached_models]


@pytest.mark.asyncio
async def test_genres_id_in_cache(session: aiohttp.ClientSession, es_client: AsyncElasticsearch,
                                  redis: aioredis.Redis, test_genres):
    # GIVEN some genres
    for genre in test_genres:
        await es_client.index('genres', id=genre.id, body=genre.dict(), refresh='wait_for')
    genre_response = await make_get_request(session, '/genre/5017d3c9-3cb5-4cd1-a329-3c99a253bcf3')
    print(genre_response.body)
    received_api_model = pydantic.parse_obj_as(api_v1.models.GenreDetail, genre_response.body)
    redis_key = "Genre:id:5017d3c9-3cb5-4cd1-a329-3c99a253bcf3"
    cached_value = await redis.get(redis_key)
    assert cached_value
    cached_model = pydantic.parse_raw_as(db.models.Genre, cached_value)
    assert received_api_model == api_v1.models.GenreDetail.from_db_model(cached_model)


@pytest.mark.asyncio
async def test_genres_id(session: aiohttp.ClientSession, es_client: AsyncElasticsearch, test_genres):
    # GIVEN some genres
    for genre in test_genres:
        await es_client.index('genres', id=genre.id, body=genre.dict(), refresh='wait_for')

    # WHEN queries for genre
    action_genre = await make_get_request(session, '/genre/5017d3c9-3cb5-4cd1-a329-3c99a253bcf3')

    # THEN only that genre is returned
    received = pydantic.parse_obj_as(api_v1.models.GenreDetail, action_genre.body)
    expected = [api_v1.models.GenreDetail.from_db_model(genre) for genre in test_genres if genre.name == 'Action'][0]
    assert expected == received


@pytest.mark.asyncio
async def test_id_not_found(session: aiohttp.ClientSession, es_client: AsyncElasticsearch, test_genres):
    # GIVEN some genres
    for genre in test_genres:
        await es_client.index('genres', id=genre.id, body=genre.dict(), refresh='wait_for')

    # WHEN queries for genre
    with pytest.raises(ClientResponseError) as cre:
        await make_get_request(session, '/genre/random_id')
    assert cre.value.status == 404
    with pytest.raises(ClientResponseError) as cre:
        await make_get_request(session, '/genre/0000')
    assert cre.value.status == 404

    response = await make_get_request(session, '/genre/5017d3c9-3cb5-4cd1-a329-3c99a253bcf3')
    genre = pydantic.parse_obj_as(api_v1.models.GenreDetail, response.body)
    assert genre.uuid == '5017d3c9-3cb5-4cd1-a329-3c99a253bcf3'
