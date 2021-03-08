import logging

import uvicorn as uvicorn
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse

import config
from api_v1 import film, genre, person
from db import elastic, cache

app = FastAPI(
    title=config.PROJECT_NAME,
    docs_url='/api/openapi',
    openapi_url='/api/openapi.json',
    default_response_class=ORJSONResponse,
)


@app.on_event('startup')
async def startup():
    await cache.get_cache()
    elastic.es = AsyncElasticsearch(config.ES_URL)


@app.on_event('shutdown')
async def shutdown():
    await cache.cache.close()
    await elastic.es.close()


app.include_router(film.router, prefix='/v1/film', tags=['film'])
app.include_router(person.router, prefix='/v1/person', tags=['person'])
app.include_router(genre.router, prefix='/v1/genre', tags=['genre'])

if __name__ == '__main__':
    uvicorn.run(
        'main:app',
        host='0.0.0.0',
        port=8000,
        log_config=config.LOGGING,
        log_level=logging.DEBUG,
    )
