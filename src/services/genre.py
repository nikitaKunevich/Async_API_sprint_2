from aioredis import Redis
from fastapi import Depends

from db.cache import ModelCache, get_cache
from db.elastic import AsyncElasticsearch, get_elastic
from db.models import Genre
from services.base import BaseElasticSearchService, ElasticSearchStorage


class GenreService(BaseElasticSearchService):
    model = Genre


def get_genre_service(
        redis: Redis = Depends(get_cache),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenreService:
    return GenreService(ModelCache(Genre, redis), ElasticSearchStorage(elastic=elastic, index='genres'))
