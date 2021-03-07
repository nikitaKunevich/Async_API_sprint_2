from aioredis import Redis
from fastapi import Depends

from config import CACHE_TTL
from db.cache import ModelCache
from db.elastic import AsyncElasticsearch, get_elastic
from db.redis import get_redis
from db.models import Genre
from services.base import BaseElasticSearchService, ElasticSearchStorage


class GenreService(BaseElasticSearchService):
    model = Genre


def get_genre_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenreService:
    return GenreService(ModelCache[Genre](redis, Genre, CACHE_TTL),
                        ElasticSearchStorage(elastic=elastic, index='genres'))
