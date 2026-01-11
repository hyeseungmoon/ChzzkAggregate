from unittest import TestCase
import os
import unittest
from dotenv import load_dotenv
from repositories.live_item_repository import LiveItemRepository
from client.chzzk_client import ChzzkClient
import logging
from main import parse

class TestIntegrationParse(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        load_dotenv()
        cls.repo = LiveItemRepository(os.environ["DATABASE_URI"])
        cls.repo.db = cls.repo.client.get_database("test_live_items_db")
        cls.repo.collection = cls.repo.db.get_collection("live_items")
        cls.client = ChzzkClient(os.environ["CLIENT_ID"], os.environ["CLIENT_SECRET"])

        logging.root.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logging.root.addHandler(stream_handler)

    def test_parse_integration(self):
        parse(self.client, self.repo)
        count = self.repo.collection.count_documents({})
        self.assertGreater(count, 0, "DB에 데이터가 삽입되지 않았습니다.")

    @classmethod
    def tearDownClass(cls):
        cls.repo.client.drop_database("test_live_items_db")

