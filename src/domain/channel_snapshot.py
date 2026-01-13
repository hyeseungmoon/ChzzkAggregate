from typing import List
from pydantic import ConfigDict, BaseModel
from pydantic.alias_generators import to_camel
from datetime import datetime

class ChannelSnapshot(BaseModel):
    channel_id: str
    follower_count: int
    timestamp: datetime

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
    )