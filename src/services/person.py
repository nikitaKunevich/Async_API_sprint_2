import logging
from functools import cache

from aioredis import Redis
from elasticsearch import AsyncElasticsearch
from fastapi import Depends

from db.cache import ModelCache, get_cache
from db.elastic import get_elastic
from db.models import Person
from services.base import BaseElasticSearchService, ElasticSearchStorage

logger = logging.getLogger(__name__)


class PersonService(BaseElasticSearchService):
    model = Person


@cache
def get_person_service(
        redis: Redis = Depends(get_cache),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    return PersonService(ModelCache(Person, redis),
                         ElasticSearchStorage(elastic=elastic, index='persons'))
