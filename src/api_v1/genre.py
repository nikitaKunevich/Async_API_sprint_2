import logging
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status

from api_v1.constants import GENRE_NOT_FOUND
from api_v1.models import GenreDetail, Genre
from services.genre import GenreService, get_genre_service

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get('/', response_model=List[Genre])
async def genres_all(
        query: Optional[str] = Query(""),
        sort: Optional[str] = Query(None, regex='^-?[a-zA-Z_]+$'),
        page_number: int = Query(1, alias='page[number]'),
        page_size: int = Query(50, alias='page[size]'),
        genre_service: GenreService = Depends(get_genre_service)) -> List[Genre]:
    genres = await genre_service.search(
        search_query=query,
        sort=sort,
        page_size=page_size, page_number=page_number)
    if not genres:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=GENRE_NOT_FOUND)

    return [Genre.from_db_model(genre) for genre in genres]


@router.get('/{genre_id:uuid}', response_model=GenreDetail)
async def genre_detail(
        genre_id: UUID,
        genre_service=Depends(get_genre_service)
) -> GenreDetail:
    genre = await genre_service.get_by_id(str(genre_id))
    if not genre:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=GENRE_NOT_FOUND)
    return GenreDetail.from_db_model(genre)
