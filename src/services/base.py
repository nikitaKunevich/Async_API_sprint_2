import asyncio
import logging
from abc import ABC, abstractmethod
from typing import List, Optional

from pydantic import parse_obj_as

from db.cache import ModelCache
from db.models import BaseESModel
from db.storage import AbstractStorage

logger = logging.getLogger(__name__)


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
            raise Exception('Missing model')

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
                     sort: Optional[str] = None, page_number: Optional[int] = None, page_size: Optional[int] = None):
        query = await self.storage.build_search_query(search_query, search_filter, sort, page_number, page_size)
        items = await self.cache.get_by_elastic_query(query)
        if not items:
            items_data = await self.storage.search(query=query)
            items = parse_obj_as(List[self.model], items_data)
            await self.cache.set_by_elastic_query(query, items)
        return items

    async def bulk_get_by_ids(self, ids: List[str]) -> List:
        if not ids:
            return []
        # noinspection PyTypeChecker
        instances = await asyncio.gather(*[self.cache.get_by_id(instance_id) for instance_id in ids])
        instances: List[BaseESModel] = [instance for instance in instances if instance is not None]
        instance_id_mapping = {instance.id: instance for instance in instances}
        not_cached_ids = [instance_id for instance_id in ids if instance_id not in instance_id_mapping]

        res = await self.storage.bulk_get_by_ids(not_cached_ids)
        instances.extend(parse_obj_as(List[self.model], res))
        if instances:
            await asyncio.gather(*[self.cache.set_by_id(instance.id, instance) for instance in instances])
        if not instances:
            return []
        return instances
