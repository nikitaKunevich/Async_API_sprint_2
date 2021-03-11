from aioredis import Redis
import pytest
from api_v1.models import FilmShort, FilmDetails
from db.models import Film

API_URL = '/film/'


@pytest.mark.asyncio
async def test_find_one(make_get_request, films):
    expected_film = next(filter(lambda f: f.title.find('Hutt Cartel') >= 0, films))
    expected_film = FilmShort.from_db_model(Film.parse_obj(expected_film))
    film_response = await make_get_request(API_URL, {'query': 'Hutt Cartel'})

    assert film_response.status == 200

    assert len(film_response.body) == 1
    assert expected_film == FilmShort.parse_obj(film_response.body[0])


@pytest.mark.asyncio
async def test_find_multiple_films(make_get_request, films):
    film1, film2 = films[2], films[3]
    response_films_star_trek = await make_get_request(API_URL, {'query': 'Trek'})

    assert response_films_star_trek.status == 200
    assert len(response_films_star_trek.body) == 2

    films_star_trek_es_ids = {f['uuid'] for f in response_films_star_trek.body}
    assert film1.id in films_star_trek_es_ids
    assert film2.id in films_star_trek_es_ids

# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_filter_genre(make_get_request, films):
    response_films_star_trek = await make_get_request(
        API_URL,
        {'query': 'Trek', 'filter[genre]': '67ae3870-b50d-4508-b2a6-ade667149ceb'}
    )
    assert response_films_star_trek.status == 200
    assert len(response_films_star_trek.body) == 1

# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_filter_unknown_genre(make_get_request, films):
    response_films_star_trek = await make_get_request(
        API_URL,
        {'query': 'Trek', 'filter[genre]': '80746937-2fc6-45d7-a3e6-a5854aa66092'}
    )
    assert response_films_star_trek.status == 404

# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_invalid_genre_filter(make_get_request, films):
    response_films_star_trek = await make_get_request(API_URL, {'query': 'trek', 'filter[genre]': 1})
    assert response_films_star_trek.status == 422

# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_invalid_sort_(make_get_request, films):
    response_films_star_trek = await make_get_request(API_URL, {'query': 'trek', 'sort': 'S'})
    assert response_films_star_trek.status == 400


@pytest.mark.asyncio
async def test_sort_rating_desc_multiple(make_get_request, films):
    response_films_star_trek = await make_get_request(API_URL, {'query': 'trek', 'sort': '-imdb_rating'})
    assert response_films_star_trek.status == 200
    assert len(response_films_star_trek.body) == 2

    assert response_films_star_trek.body[0]['uuid'] == films[3].id

# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_paging_one_on_page(make_get_request, films):
    response_films_star_trek = await make_get_request(API_URL, {'query': 'Trek', 'page[size]': 1})
    assert response_films_star_trek.status == 200
    assert len(response_films_star_trek.body) == 1

# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_paging_second_page(make_get_request, films):
    response_films_star_trek = await make_get_request(API_URL, {'query': 'Trek', 'page[size]': 1, 'page[number]': 2})
    assert response_films_star_trek.status == 200
    assert len(response_films_star_trek.body) == 1

# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_paging_empty_page(make_get_request, films):
    response_films_star_trek = await make_get_request(API_URL, {'query': 'Trek', 'page[size]': 1, 'page[number]': 4})
    assert response_films_star_trek.status == 404
    assert 'detail' in response_films_star_trek.body

# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_paging_invalid_page_size(make_get_request, films):
    response_films_star_trek = await make_get_request(API_URL, {'query': 'Trek', 'page[size]': -1})
    assert response_films_star_trek.status == 422

# noinspection PyUnusedLocal
@pytest.mark.asyncio
async def test_paging_invalid_page_size(make_get_request, films):
    response_films_star_trek = await make_get_request(API_URL, {'query': 'Trek', 'page[number]': -1})
    assert response_films_star_trek.status == 422


@pytest.mark.asyncio
async def test_all_search(make_get_request):
    response_films_star_trek = await make_get_request(API_URL, {'query': 'Star'})

    assert response_films_star_trek.status == 200
    assert len(response_films_star_trek.body) == 4


@pytest.mark.asyncio
async def test_search_not_found(make_get_request):
    response_not_found = await make_get_request(API_URL, {'query': 'NotFoundFilm'})

    assert response_not_found.status == 404
    assert 'detail' in response_not_found.body


@pytest.mark.asyncio
async def test_detailed_info(make_get_request, films):
    film_title = 'Star Wars: The New Republic Anthology'
    expected_film = next(filter(lambda f: f.title == film_title, films))
    expected_film = FilmDetails.from_db_model(Film.parse_obj(expected_film))

    response_film_star_wars = await make_get_request(f'{API_URL}{expected_film.uuid}')
    assert response_film_star_wars.status == 200
    print(f'!!test_detailed_info: {response_film_star_wars}')
    received_film = FilmDetails.parse_obj(response_film_star_wars.body)

    assert received_film == expected_film


@pytest.mark.asyncio
async def test_get_film_unknown_id(make_get_request):
    response_not_found = await make_get_request('/film/6bcc7f85-9e5d-45a9-91ec-25903212c8b7')

    assert response_not_found.status == 404
    assert 'detail' in response_not_found.body


@pytest.mark.asyncio
async def test_get_film_invalid_id(make_get_request):
    response_not_found = await make_get_request('/film/-1')
    assert response_not_found.status == 404
    assert 'detail' in response_not_found.body


@pytest.mark.asyncio
async def test_redis_cache(make_get_request, redis: Redis):
    await redis.flushall()
    assert not (await redis.keys("Film:query:*"))
    response = await make_get_request(API_URL, {'query': 'Trek'})

    assert response.status == 200
    assert await redis.keys("Film:query:*")
