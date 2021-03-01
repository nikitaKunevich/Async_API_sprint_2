from dataclasses import dataclass

from multidict import CIMultiDictProxy

from settings import Settings


@dataclass
class HTTPResponse:
    body: dict
    headers: CIMultiDictProxy[str]
    status: int


settings = Settings()


async def make_get_request(session, method: str, params: dict = None) -> HTTPResponse:
    params = params or {}
    url = settings.api_host + '/v1' + method
    async with session.get(url, params=params) as response:
        return HTTPResponse(
            body=await response.json(),
            headers=response.headers,
            status=response.status,
        )
