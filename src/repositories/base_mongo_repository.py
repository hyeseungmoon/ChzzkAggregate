import logging

from pymongo import MongoClient
from typing import List

from pymongo.errors import BulkWriteError


class BaseMongoRepository:
    def __init__(
        self,
        uri: str,
        collection_name: str,
        db_name: str = "chzzk_db",
    ) -> None:
        self.client = MongoClient(uri)
        self.db = self.client.get_database(db_name)
        self.collection = self.db.get_collection(collection_name)
        self.collection_name = collection_name

    def insert_batch(self, items: List[dict]) -> int:
        if not items:
            return 0
        try:
            result = self.collection.insert_many(items, ordered=False)
            return len(result.inserted_ids)

        except BulkWriteError as bwe:
            write_errors = bwe.details.get("writeErrors", [])
            for err in write_errors:
                if err.get("code") != 11000:  # DuplicateKey
                    logging.exception("insert_batch failed")
                    logging.debug(
                        "failed docs count=%d, errors=%s",
                        len(bwe.details.get("writeErrors", [])),
                        bwe.details.get("writeErrors"),
                    )
                    raise
            logging.info("Skipping Duplicate insertion")
        except Exception:
            raise
