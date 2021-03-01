import aiohttp
import pytest

from elasticsearch import AsyncElasticsearch

import api_v1.models
import db.models
from utils import make_get_request


@pytest.mark.asyncio
async def test_search_detailed(session: aiohttp.ClientSession, es_client: AsyncElasticsearch):
    # Заполнение данных для теста
    first_film = db.models.Film.parse_obj({
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
    })
    second_film = db.models.Film.parse_obj({
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
    })

    await es_client.index('movies', body=first_film.dict(), refresh='wait_for')
    await es_client.index('movies', body=second_film.dict(), refresh='wait_for')

    # Выполнение запроса
    response = await make_get_request(session, '/film/', {'query': 'Star'})
    # Проверка результата
    assert response.status == 200
    expected = api_v1.models.FilmShort.from_db_model(second_film)
    assert len(response.body) == 1
    received = response.body[0]
    assert expected == received
