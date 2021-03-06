from aioredis import Redis
import pytest
import urllib.parse


@pytest.mark.asyncio
async def test_single_search(make_group_get_request, films):
    query_str = '/film/?query='
    response_film_star_wars, response_film_star_trek = await make_group_get_request([
        query_str + urllib.parse.quote('Hutt Cartel'),
        query_str + urllib.parse.quote('Horizon')
    ])

    assert response_film_star_wars.status == 200

    assert len(response_film_star_wars.body) == 1
    film_star_wars_es = response_film_star_wars.body[0]
    film_star_wars = films[0]
    assert film_star_wars_es['title'] == film_star_wars.title
    assert film_star_wars_es['uuid'] == film_star_wars.id
    assert film_star_wars_es['imdb_rating'] == film_star_wars.imdb_rating

    assert response_film_star_trek.status == 200

    assert len(response_film_star_trek.body) == 1
    film_star_trek_es = response_film_star_trek.body[0]
    film_star_trek = films[2]
    assert film_star_trek_es['title'] == film_star_trek.title
    assert film_star_trek_es['uuid'] == film_star_trek.id
    assert film_star_trek_es['imdb_rating'] == film_star_trek.imdb_rating


@pytest.mark.asyncio
async def test_many_search(make_get_request, films):
    film1, film2 = films[2], films[3]
    response_films_star_trek = await make_get_request('/film/?query=' + urllib.parse.quote('Trek'))

    assert response_films_star_trek.status == 200
    assert len(response_films_star_trek.body) == 2

    films_star_trek_es_ids = {f['uuid'] for f in response_films_star_trek.body}
    assert film1.id in films_star_trek_es_ids
    assert film2.id in films_star_trek_es_ids


@pytest.mark.asyncio
async def test_query_params(make_get_request, films):
    response_films_star_trek = await make_get_request('/film/?query=' + urllib.parse.quote('Trek') + '&filter[genre]=67ae3870-b50d-4508-b2a6-ade667149ceb')
    assert response_films_star_trek.status == 200
    assert len(response_films_star_trek.body) == 1

    response_films_star_trek = await make_get_request('/film/?query=' + urllib.parse.quote('Trek') + '&filter[genre]=80746937-2fc6-45d7-a3e6-a5854aa66092')
    assert response_films_star_trek.status == 404

    response_films_star_trek = await make_get_request('/film/?query=' + urllib.parse.quote('trek') + '&filter[genre]=1')
    assert response_films_star_trek.status == 422

    response_films_star_trek = await make_get_request('/film/?query=' + urllib.parse.quote('trek') + '&sort=S')
    assert response_films_star_trek.status == 400

    response_films_star_trek = await make_get_request('/film/?query=' + urllib.parse.quote('trek') + '&sort=-imdb_rating')
    assert response_films_star_trek.status == 200
    assert len(response_films_star_trek.body) == 2

    assert response_films_star_trek.body[0]['uuid'] == films[3].id

    response_films_star_trek = await make_get_request('/film/?query=' + urllib.parse.quote('Trek') + '&page[size]=1')
    assert response_films_star_trek.status == 200
    assert len(response_films_star_trek.body) == 1

    response_films_star_trek = await make_get_request('/film/?query=' + urllib.parse.quote('Trek') + '&page[size]=1&page[number]=2')
    assert response_films_star_trek.status == 200
    assert len(response_films_star_trek.body) == 1

    response_films_star_trek = await make_get_request('/film/?query=' + urllib.parse.quote('Trek') + '&page[size]=1&page[number]=3')
    assert response_films_star_trek.status == 404
    assert 'detail' in response_films_star_trek.body

    response_films_star_trek = await make_get_request('/film/?query=' + urllib.parse.quote('Trek') + '&page[size]=-1')
    assert response_films_star_trek.status == 422

    response_films_star_trek = await make_get_request('/film/?query=' + urllib.parse.quote('Trek') + '&page[size]=1&page[number]=-1')
    assert response_films_star_trek.status == 422


@pytest.mark.asyncio
async def test_all_search(make_get_request):
    response_films_star_trek = await make_get_request('/film/?query=' + urllib.parse.quote('Star'))

    assert response_films_star_trek.status == 200
    assert len(response_films_star_trek.body) == 4


@pytest.mark.asyncio
async def test_search_not_found(make_get_request):
    response_not_found = await make_get_request('/film/?query=NotFoundFilm&' + urllib.parse.quote(''))

    assert response_not_found.status == 404
    assert 'detail' in response_not_found.body


@pytest.mark.asyncio
async def test_detailed_info(make_get_request, films):
    film_star_wars = films[1]
    response_film_star_wars = await make_get_request(f'/film/{film_star_wars.id}')

    assert response_film_star_wars.status == 200
    film_star_wars_es = response_film_star_wars.body

    assert film_star_wars_es['uuid'] == film_star_wars.id
    assert film_star_wars_es['title'] == film_star_wars.title
    assert film_star_wars_es['imdb_rating'] == film_star_wars.imdb_rating
    assert film_star_wars_es['description'] == film_star_wars.description

    assert len(film_star_wars_es['genre']) == len(film_star_wars.genres)
    fg = {g.id: g for g in film_star_wars.genres}
    for genre in film_star_wars_es['genre']:
        assert genre['uuid'] in fg
        assert genre['name'] == fg[genre['uuid']].name

    assert len(film_star_wars_es['actors']) == len(film_star_wars.actors)
    fa = {a.id: a for a in film_star_wars.actors}
    for actor in film_star_wars_es['actors']:
        assert actor['uuid'] in fa
        assert actor['full_name'] == fa[actor['uuid']].name

    assert len(film_star_wars_es['writers']) == len(film_star_wars.writers)
    fw = {w.id: w for w in film_star_wars.writers}
    for writer in film_star_wars_es['writers']:
        assert writer['uuid'] in fw
        assert writer['full_name'] == fw[writer['uuid']].name

    assert len(film_star_wars_es['directors']) == len(film_star_wars.directors)
    fd = {d.id: d for d in film_star_wars.directors}
    for director in film_star_wars_es['directors']:
        assert director['uuid'] in fd
        assert director['full_name'] == fd[director['uuid']].name


@pytest.mark.asyncio
async def test_detailed_not_found(make_get_request):
    response_not_found = await make_get_request('/film/6bcc7f85-9e5d-45a9-91ec-25903212c8b7')

    assert response_not_found.status == 404
    assert 'detail' in response_not_found.body

    response_not_found = await make_get_request('/ film/-1?')
    assert response_not_found.status == 404
    assert 'detail' in response_not_found.body


@pytest.mark.asyncio
async def test_redis_cache(make_get_request, redis: Redis):
    await redis.flushall()
    assert not (await redis.keys("Film:query:*"))
    response = await make_get_request('/film/?query=Trek')

    assert response.status == 200
    assert await redis.keys("Film:query:*")
