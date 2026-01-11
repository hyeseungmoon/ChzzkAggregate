from pydantic import BaseModel
from typing import List
from domain.live_item import LiveItem

class PageInfo(BaseModel):
    next: str | None

class LiveResponseBody(BaseModel):
    data: List[LiveItem]
    page: PageInfo

class SuccessResponse[T](BaseModel):
    code: int
    message: None
    content: T

class FailResponse(BaseModel):
    code: int
    message: str

