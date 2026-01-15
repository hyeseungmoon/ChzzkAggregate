from typing import List

from domain.channel_snapshot import ChannelSnapshot
from repositories.interface.channel_snapshot_repository_interface import ChannelSnapshotRepository
from repositories.mongo.base_mongo_repository import BaseMongoRepository

class MongoChannelSnapshotRepository(BaseMongoRepository[ChannelSnapshot],
                                     ChannelSnapshotRepository):
    def __init__(self, uri: str) -> None:
        super().__init__(uri, "channel_snapshots")

        self.collection.create_index(
            [("timestamp", 1)]
        )

        self.collection.create_index(
            [("channel_id", 1)]
        )

        self.collection.create_index(
            [("timestamp", 1), ("channel_id", 1)],
            unique=True,
        )

    def save_channel_snapshots(self, channel_snapshots: List[ChannelSnapshot]) -> int:
        channel_snapshots_dict_list = list(map(self._to_dict, channel_snapshots))
        return self._insert_batch(channel_snapshots_dict_list)

    def _to_dict(self, entity:ChannelSnapshot) -> dict:
        return entity.model_dump()