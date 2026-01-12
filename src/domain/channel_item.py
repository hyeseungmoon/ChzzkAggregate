from typing import List
from pydantic import ConfigDict, BaseModel
from pydantic.alias_generators import to_camel

class ChannelItem(BaseModel):
    channel_id: str
    channel_name: str
    channel_image_url: str | None
    follower_count: int
    verified_mark: bool

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
    )