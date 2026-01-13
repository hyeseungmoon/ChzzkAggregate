from repositories.base_mongo_repository import BaseMongoRepository

class ChannelRepository(BaseMongoRepository):
    def __init__(self, uri: str) -> None:
        super().__init__(uri, "channels")

        self.collection.create_index(
            [("channel_id", 1)],
            unique=True,
        )

