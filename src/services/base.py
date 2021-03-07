import logging
from abc import ABC, abstractmethod
from typing import List, Optional

import elasticsearch.exceptions
import elasticsearch.exceptions
from elasticsearch import AsyncElasticsearch
from elasticsearch_dsl.search import Search
from fastapi import HTTPException
from pydantic import parse_obj_as
from starlette import status

from db.cache import ModelCache

logger = logging.getLogger(__name__)


class AbstractStorage(ABC):
    @abstractmethod
    async def get_by_id(self, instance_id: str):
        pass

    @abstractmethod
    async def bulk_get_by_ids(self, ids: List[str]):
        pass

    @abstractmethod
    async def search(self, query: Search):
        pass

    @abstractmethod
    async def build_search_query(self, *args, **kwargs):
        pass

    @staticmethod
    @abstractmethod
    def prepare_query(s: Search, search_query, sort):
        pass

    @staticmethod
    @abstractmethod
    def get_paginated_query(*args, **kwargs):
        pass


class ElasticSearchStorage(AbstractStorage):
    def __init__(self, elastic: AsyncElasticsearch, index: str, query_builder=None):
        self._query_builder = query_builder
        self.elastic = elastic
        self.index = index

    async def get_by_id(self, instance_id: str) -> Optional[dict]:
        try:
            doc = await self.elastic.get(self.index, instance_id)
            return doc['_source']
        except elasticsearch.exceptions.NotFoundError:
            return None

    async def bulk_get_by_ids(self, ids: List[str]) -> List[dict]:
        try:
            res = await self.elastic.mget(body={'ids': ids}, index=self.index)
            return [doc['_source'] for doc in res['docs']]
        except elasticsearch.exceptions.NotFoundError:
            return []

    @staticmethod
    def prepare_query(s, search_query, sort):
        if search_query:
            s = s.query('match', full_name=search_query)
        if sort:
            s = s.sort(sort)
        return s

    async def build_search_query(self, search_query: str = "",
                                 search_filter: Optional[str] = None,
                                 sort: Optional[str] = None):
        s = Search(using=self.elastic, index=self.index)
        if not self._query_builder:
            s = self.prepare_query(s, search_query, sort)
        else:
            s = self._query_builder.prepare_query(s, search_query, search_filter, sort)
        return s

    async def search(self, query: Search) -> List[dict]:
        try:
            search_result = await self.elastic.search(index=self.index, body=query)
        except elasticsearch.exceptions.RequestError as re:
            if re.error == 'search_phase_execution_exception':
                # Если используется sort которого нет в elastic
                raise HTTPException(status.HTTP_400_BAD_REQUEST, 'Malformed request')
            raise
        items = [hit['_source'] for hit in search_result['hits']['hits']]
        return items

    @staticmethod
    def get_paginated_query(search: Search, page_number: int, page_size: int) -> dict:
        start = (page_number - 1) * page_size
        return search[start: start + page_size].to_dict()


class AbstractService(ABC):
    # TODO: replace ModelCache with AbstractCache
    def __init__(self, cache: ModelCache, storage: AbstractStorage):
        self.cache = cache
        self.storage = storage

    @abstractmethod
    async def search(self, *args, **kwargs):
        pass

    @abstractmethod
    async def get_by_id(self, instance_id: str):
        pass

    @abstractmethod
    async def bulk_get_by_ids(self, ids: List[str]):
        pass


class BaseElasticSearchService(AbstractService):
    model = None

    def __init__(self, cache: ModelCache, storage: AbstractStorage):
        super(BaseElasticSearchService, self).__init__(cache, storage)
        if not self.model:
            raise Exception

    async def get_by_id(self, instance_id: str):
        instance = await self.cache.get_by_id(instance_id)
        if not instance:
            instance_data = await self.storage.get_by_id(instance_id)
            if not instance_data:
                return None
            instance = self.model(**instance_data)
            logger.debug(f'got {instance.__class__.__name__} from elastic: {instance}')
            await self.cache.set_by_id(instance_id, instance)
        return instance

    async def search(self, search_query: str,
                     search_filter: Optional[str] = None,
                     sort: Optional[str] = None, page_number: int = 1, page_size: int = 50):
        query = await self.storage.build_search_query(search_query, search_filter, sort)
        query = self.storage.get_paginated_query(query, page_number, page_size)
        items = await self.cache.get_by_elastic_query(query)
        if not items:
            items_data = await self.storage.search(query=query)
            items = parse_obj_as(List[self.model], items_data)
            await self.cache.set_by_elastic_query(query, items)
        return items

    async def bulk_get_by_ids(self, ids: List[str]) -> List:
        if not ids:
            return []
        res = await self.storage.bulk_get_by_ids(ids)
        return parse_obj_as(List[self.model], res)
