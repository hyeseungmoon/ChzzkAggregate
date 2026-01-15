from typing import List

from domain.live_snapshot import LiveSnapshot
from repositories.interface.live_snapshot_repository_interface import LiveSnapshotRepository
from repositories.mongo.base_mongo_repository import BaseMongoRepository

class MongoLiveSnapshotRepository(BaseMongoRepository[LiveSnapshot],
                                  LiveSnapshotRepository):
    def __init__(self, uri: str) -> None:
        super().__init__(uri, "live_snapshots")

        self.collection.create_index(
            [("timestamp", 1)]
        )

        self.collection.create_index(
            [("channel_id", 1)]
        )

        self.collection.create_index(
            [("timestamp", 1), ("channel_id", 1)],
        )

        self.collection.create_index(
            [("timestamp", 1), ("live_id", 1)],
            unique=True,
        )

        self.collection.create_index(
            [("live_category", 1)]
        )

    def save_live_snapshots(self, live_snapshots: List[LiveSnapshot]) -> int:
        live_snapshots_dict_list = list(map(self._to_dict, live_snapshots))
        return self._insert_batch(live_snapshots_dict_list)

    def _to_dict(self, entity: LiveSnapshot) -> dict:
        return entity.model_dump()
