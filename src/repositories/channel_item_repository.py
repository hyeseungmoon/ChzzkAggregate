from repositories.base_mongo_repository import BaseMongoRepository

class ChannelItemRepository(BaseMongoRepository):
    def __init__(self, uri: str) -> None:
        super().__init__(uri, "channel_items")
