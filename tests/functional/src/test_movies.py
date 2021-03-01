from elasticsearch import AsyncElasticsearch
import pytest
import asyncio

from db.models import Film


@pytest.fixture
async def films(es_client):
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

    await asyncio.gather(*[es_client.index('movies', body=Film.parse_obj(film).dict(), id=film['id'], refresh='wait_for') for film in films])

    return films


@pytest.mark.asyncio
async def test_search(es_client: AsyncElasticsearch, make_get_request, films):
    # Проверка находится ли внутри elastic
    assert await es_client.get(id=films[0]['id'], index='movies')

    # Выполнение запроса
    requests = [
        make_get_request('/film/?query=Star'),               # Удачный запрос 1
        make_get_request('/film/?query=Night%20Owl'),        # Удачный запрос 2
        make_get_request('/film/?query=SomethingNotFound'),  # Неудачный запрос 3
    ]

    r1, r2, r3 = await asyncio.gather(*requests)

    # 1
    assert r1.status == 200
    assert len(r1.body) == 1
    assert r1.body[0]['uuid'] == films[1]['id']

    # 2
    assert r2.status == 200
    assert len(r2.body) == 1
    assert r2.body[0]['uuid'] == films[0]['id']

    # 3
    assert r3.status == 404


@ pytest.mark.asyncio
async def test_search_by_id(es_client, make_get_request, films):
    requests = [
        make_get_request('/film/08293f6d-90e4-467d-b2c3-ee85b5b0d326'),  # 1 Удачный тест
        make_get_request('/film/00000000-0000-0000-0000-000000000000'),  # 2 Нет такого id
        make_get_request('/film/1'),                                     # 3 Странный id
    ]

    responses = await asyncio.gather(*requests)
    r1, r2, r3 = responses

    # 1
    assert r1.status == 200
    assert r1.body['title'] == 'Night Owl'

    # 2
    assert r2.status == 404

    # 3
    assert r3.status == 404
