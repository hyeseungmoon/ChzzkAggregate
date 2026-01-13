from typing import List
from pydantic import ConfigDict, BaseModel, field_validator
from pydantic.alias_generators import to_camel
from datetime import datetime

class Channel(BaseModel):
    channel_id: str
    channel_name: str
    channel_image_url: str
    verified_mark: bool

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        extra="ignore"
    )

    @field_validator(
        "channel_image_url",
        mode="before",
    )
    @classmethod
    def none_to_empty_str(cls, v):
        return "" if v is None else v