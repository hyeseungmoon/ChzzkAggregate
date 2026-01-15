from typing import List

from domain.channel import Channel
from repositories.mongo.base_mongo_repository import BaseMongoRepository
from repositories.interface.channel_repository_interface import ChannelRepository


class MongoChannelRepository(BaseMongoRepository[Channel],
                             ChannelRepository):

    def __init__(self, uri: str) -> None:
        super().__init__(uri, "channels")

        self.collection.create_index(
            [("channel_id", 1)],
            unique=True,
        )

    def save_channels(self, channels: List[Channel]) -> int:
        channels_dict_list = list(map(self._to_dict, channels))
        return self._insert_batch(channels_dict_list)

    def _to_dict(self, entity:Channel) -> dict:
        return entity.model_dump()

