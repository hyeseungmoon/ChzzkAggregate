from typing import List
from pydantic import ConfigDict, BaseModel
from pydantic.alias_generators import to_camel

class LiveItem(BaseModel):
    live_id: int
    live_title: str
    live_thumbnail_image_url: str
    concurrent_user_count: int
    open_date: str
    adult: bool
    tags: List[str]

    category_type: str | None
    live_category: str | None
    live_category_value: str

    channel_id: str
    channel_name: str
    channel_image_url: str | None

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
    )