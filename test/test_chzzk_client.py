import os
from unittest import TestCase
from dotenv import load_dotenv
from schemas.response_models import LiveResponseBody
from client.chzzk_client import ChzzkClient, ChzzkAPIError


class TestChzzkClient(TestCase):
    @classmethod
    def setUpClass(cls):
        load_dotenv()
        cls.client = ChzzkClient(os.environ["CLIENT_ID"], os.environ["CLIENT_SECRET"])

    # 정상 GET 요청이 성공해야 함
    def test_get_lives_returns_200(self):
        res = self.client.get("/open/v1/lives", LiveResponseBody)
        self.assertEqual(res.code, 200)

    # 응답 content에 접근 가능해야 함
    def test_get_lives_content_is_accessible(self):
        res = self.client.get("/open/v1/lives", LiveResponseBody)
        self.assertGreaterEqual(len(res.content.data), 0)

    # 잘못된 클라이언트 정보로 요청하면 401 예외 발생
    def test_get_lives_with_invalid_client_raises_401(self):
        invalid_client = ChzzkClient("invalid", "invalid")

        with self.assertRaises(ChzzkAPIError) as ctx:
            invalid_client.get("/open/v1/lives", LiveResponseBody)

        exc = ctx.exception
        self.assertEqual(exc.error.code, 401)
        self.assertIn("INVALID_CLIENT", exc.error.message)
