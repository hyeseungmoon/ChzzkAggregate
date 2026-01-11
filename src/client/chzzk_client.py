import requests
from schemas.response_models import SuccessResponse, FailResponse
from typing import TypeVar

T = TypeVar("T")

class ChzzkAPIError(Exception):
    def __init__(self, error: FailResponse):
        self.error = error
        super().__init__(f"Error Code: {error.code}, Message: {error.message}")

class ChzzkClient:
    def __init__(self, client_id, client_secret):
        self.base_url:str = "https://openapi.chzzk.naver.com"
        self.client_session = requests.Session()
        self.client_session.headers.update({
            "Client-Id": client_id,
            "Client-Secret": client_secret,
            "Content-Type": "application/json"
        })

    def _request(self, method:str, path:str, **kwargs) -> requests.Response:
        url = f"{self.base_url}/{path.lstrip("/")}"
        res = self.client_session.request(method, url, **kwargs)
        return res

    def get[T](self, path: str, content_type:T, **kwargs) -> SuccessResponse[T]:
        res = self._request("GET", path, **kwargs)
        if res.status_code != 200:
            raise ChzzkAPIError(FailResponse(**res.json()))
        return SuccessResponse[content_type](**res.json())
