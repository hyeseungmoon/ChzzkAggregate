import os
from unittest import TestCase
from pymongo import MongoClient
from dotenv import load_dotenv

class TestLiveItemRepository(TestCase):
    @classmethod
    def setUpClass(cls):
        load_dotenv()
        cls.uri = os.environ.get("DATABASE_URI")
        cls.client = MongoClient(cls.uri)

    def test_connection(self):
        try:
            server_info = self.client.server_info()
            self.assertIn("version", server_info)
        except Exception as e:
            self.fail(f"DB 연결 실패: {e}")
