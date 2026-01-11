from pymongo import MongoClient
from typing import List


class LiveItemRepository:
    def __init__(self, uri: str) -> None:
        self.client = MongoClient(uri)
        self.db = self.client.get_database("live_items_db")
        self.collection = self.db.get_collection("live_items")

    def insert_batch(self, documents: List[dict]) -> int:
        result = self.collection.insert_many(documents, ordered=False)
        return len(result.inserted_ids)
