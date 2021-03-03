import asyncio
import json
from dataclasses import dataclass
from pathlib import Path

import aiohttp
import aioredis
import pytest
from aioredis.commands import Redis
from db.models import Film
from elasticsearch import AsyncElasticsearch
from multidict import CIMultiDictProxy

from .settings import Settings


@pytest.fixture(scope='session')
def settings() -> Settings:
    return Settings()


@pytest.fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope='session', autouse=True)
async def redis(settings) -> Redis:
    redis = await aioredis.create_redis_pool((settings.redis_host, settings.redis_port), minsize=10, maxsize=20)
    await redis.flushall()
    yield redis
    redis.close()
    await redis.wait_closed()


@pytest.fixture(scope='session')
async def es_client(settings):
    async def create_indices(indx_name, schema_path):
        return await client.indices.create(indx_name, body=json.load(open(schema_path)))

    async def delete_indices(indx_name):
        await client.indices.delete(indx_name)

    def get_schema_path(name):
        return str(Path(__file__).parent.parent.parent / 'schemas' / f'es.{name}.schema.json')

    index_names = ['movies', 'genres', 'persons']
    client = AsyncElasticsearch(hosts=settings.es_url)
    try:
        await asyncio.gather(*[delete_indices(name) for name in index_names])
    except:
        pass

    await asyncio.gather(*[create_indices(index, get_schema_path(index)) for index in index_names])

    yield client

    await asyncio.gather(*[delete_indices(name) for name in index_names])
    await client.close()


@ pytest.fixture(scope='session')
async def session():
    session = aiohttp.ClientSession()
    yield session
    await session.close()


@dataclass
class HTTPResponse:
    body: dict
    headers: CIMultiDictProxy[str]
    status: int


@pytest.fixture
def make_get_request(session, settings):
    async def inner(method: str, params: dict = None) -> HTTPResponse:
        params = params or {}
        url = settings.api_host + '/v1' + method  # в боевых системах старайтесь так не делать!
        async with session.get(url, params=params) as response:
            body = await response.json()
            print(f"{body=}")
            return HTTPResponse(
                body=body,
                headers=response.headers,
                status=response.status,
            )
    return inner


@pytest.fixture
async def films(es_client):
    # Заполнение данных для теста

    # 2 фильма Star Wars, 2 фильма Star Trek
    test_films = [
        {
            "imdb_rating": 8.1,
            "id": "fff4777b-7fe5-4be9-a6ea-6acb75b96fb0",
            "title": "Star Wars: The Old Republic - Rise of the Hutt Cartel",
            "description": "With the Sith emperor defeated, the republic & empire grow ever more desperate to win the conflict. All eyes turn on the planet Makeb, where a mysterious substance called isotope-5 has been discovered. This substance has the power to fuel a massive army for whoever wields it. Unfortunately for both sides, the hutt cartel has control of the planet, and will not give it up lightly...",
            "actors_names": ["a", "aa"],
            "writers_names": ["w", "ww"],
            "directors_names": ["d"],
            "genres_names": ["Action"],
            "actors": [
                {
                    "id": "237f5326-c9b6-43c9-9a1a-80cd21088e20",
                    "name": "a"
                },
                {
                    "id": "1a9ca60c-affe-4e69-aa03-9d07d1b977c1",
                    "name": "aa"
                }
            ],
            "writers": [
                {
                    "id": "279842e9-28f3-49f4-9914-46f8b4ad92c9",
                    "name": "w"
                },
                {
                    "id": "503db68c-d0f8-468a-a4f2-d6f87b81afcc",
                    "name": "ww"
                }
            ],
            "directors": [
                {
                    "id": "f87486ec-7682-407b-8346-23947d944820",
                    "name": "d"
                }
            ],
            "genres": [
                {
                    "id": "6ae86dab-dbc2-478c-9151-ddfd8c92c10a",
                    "name": "Action"
                }
            ]
        },
        {
            "imdb_rating": 7.1,
            "id": "fdaf4660-6456-4e99-8009-c9e0db67ea90",
            "title": "Star Wars: The New Republic Anthology",
            "description": "Boba Fett has been trapped for 30 years in the Great Pit of Carkoon; makes his escape and is now allies with the Rebellion.",
            "actors_names": ["a2", "aa2"],
            "writers_names": ["w2", "ww2"],
            "directors_names": ["d2", "dd2"],
            "genres_names": ["Thriller"],
            "actors": [
                {
                    "id": "929d9e18-f5d1-4198-97b2-4e2ddbe28f76",
                    "name": "a2"
                },
                {
                    "id": "620a94fe-f76e-4a0e-b6c9-f8f7c14d811d",
                    "name": "aa2"
                }
            ],
            "writers": [
                {
                    "id": "845de5a7-b57f-42a2-a2d1-ad07d88e1689",
                    "name": "w2"
                },
                {
                    "id": "bb871757-9e79-4bd6-978c-a683ae0c127c",
                    "name": "ww2"
                }
            ],
            "directors": [
                {
                    "id": "c038eeb8-529f-4c50-877f-3d0af97bee9f",
                    "name": "d2"
                },
                {
                    "id": "8a2b3048-d9b7-4874-9ef5-cb9155b9fc38",
                    "name": "dd2"
                }
            ],
            "genres": [
                {
                    "id": "9a09f1b3-a3f8-4939-b175-29beeba1a6b3",
                    "name": "Thriller"
                }
            ]
        },
        {
            "imdb_rating": 5.7,
            "id": "fdfc8350-4ec4-45c5-9ce9-9139c3e2fce6",
            "title": "Star Trek: Horizon",
            "description": "In a time prior to the United Federation of Planets, a young coalition of worlds led by Earth battle the Romulan Star Empire for their very survival.",
            "actors_names": ["a3", "aa3"],
            "writers_names": ["w3", "ww3"],
            "directors_names": ["d3"],
            "genres_names": ["War"],
            "actors": [
                {
                    "id": "b29a3239-800d-4c6c-9f3f-3280f503566e",
                    "name": "a3"
                },
                {
                    "id": "84df81ae-7845-4d78-963e-47b0a8a4b972",
                    "name": "aa3"
                }
            ],
            "writers": [
                {
                    "id": "454cbe05-efcd-4f17-91ae-3c6b670e3e76",
                    "name": "w3"
                },
                {
                    "id": "2ee03ec0-ea75-42f5-841e-189ad7aadd72",
                    "name": "ww3"
                }
            ],
            "directors": [
                {
                    "id": "45cbadba-a15d-4036-87e8-a64e749f0ad8",
                    "name": "d3"
                }
            ],
            "genres": [
                {
                    "id": "67ae3870-b50d-4508-b2a6-ade667149ceb",
                    "name": "War"
                }
            ]
        },
        {
            "imdb_rating": 7.5,
            "id": "fd7d8ea7-396a-42af-8b0a-ec2747e27506",
            "title": "Star Trek: Enterprise",
            "description": "The year is 2151. Earth has spent the last 88 years since learning how to travel faster than the speed of light studying under the wisdom of their alien ally called the 'Vulcans'. Now, the first crew of human explorers sets out into deep space on a ship called the 'Enterprise' to see what is beyond our solar system.",
            "actors_names": ["a4", "aa4"],
            "writers_names": ["w4", "ww4"],
            "directors_names": ["d4"],
            "genres_names": ["Mystery"],
            "actors": [
                {
                    "id": "80227dc8-3e00-4b41-a2e3-350baacd9acb",
                    "name": "a4"
                },
                {
                    "id": "914732e3-8616-4c68-860f-64f42f78754d",
                    "name": "aa4"
                }
            ],
            "writers": [
                {
                    "id": "449b2581-7459-4ab4-b504-ef9b6244a816",
                    "name": "w4"
                },
                {
                    "id": "d4017eca-1417-4bd4-8414-2ca2686bf94b",
                    "name": "ww4"
                }
            ],
            "directors": [
                {
                    "id": "b2a9534a-2f6b-4754-9bd2-be7ce2233b51",
                    "name": "d4"
                }
            ],
            "genres": [
                {
                    "id": "e86e1386-64d4-4e0c-9162-af3363082727",
                    "name": "Mystery"
                }
            ]
        }
    ]

    films_obj = [Film.parse_obj(film) for film in test_films]
    await asyncio.gather(*[es_client.index('movies', body=film.dict(), id=film.id, refresh='wait_for') for film in films_obj])

    return films_obj


@pytest.fixture
@pytest.mark.asyncio
async def make_group_get_request(make_get_request):
    async def inner(urls: list) -> list[HTTPResponse]:
        requests = [make_get_request(url) for url in urls]
        return await asyncio.gather(*requests)
    return inner
