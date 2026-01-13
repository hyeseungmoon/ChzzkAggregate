from repositories.base_mongo_repository import BaseMongoRepository

class ChannelSnapshotRepository(BaseMongoRepository):
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
