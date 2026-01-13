from typing import List
from pydantic import ConfigDict, BaseModel, field_validator
from datetime import datetime
from pydantic.alias_generators import to_camel

class LiveSnapshot(BaseModel):
    live_id: int
    live_title: str
    # live_thumbnail_image_url: str
    concurrent_user_count: int
    open_date: datetime
    adult: bool
    tags: List[str]
    timestamp: datetime

    category_type: str
    live_category: str
    live_category_value: str

    channel_id: str
    # channel_name: str
    # channel_image_url: str | None

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        extra="ignore"
    )

    @field_validator(
        "live_title",
        "category_type",
        "live_category",
        "live_category_value",
        mode="before",
    )
    @classmethod
    def none_to_empty_str(cls, v):
        return "" if v is None else v