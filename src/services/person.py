import logging
from functools import cache

from aioredis import Redis
from elasticsearch import AsyncElasticsearch
from fastapi import Depends

from config import CACHE_TTL
from db.cache import ModelCache
from db.elastic import get_elastic
from db.redis import get_redis
from db.models import Person
from services.base import BaseElasticSearchService, ElasticSearchStorage

logger = logging.getLogger(__name__)


class PersonService(BaseElasticSearchService):
    model = Person
    index = 'persons'


@cache
def get_person_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    return PersonService(ModelCache[Person](redis, Person, CACHE_TTL),
                         ElasticSearchStorage(elastic=elastic, index='persons'))
