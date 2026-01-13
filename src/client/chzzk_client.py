import logging
import requests
from pydantic import BaseModel

from typing import List, Union, Dict, Any

class SuccessResponse(BaseModel):
    code: int
    message: None
    content: Union[str, int, float, bool, None, Dict[str, Any], List[Any]]

class FailResponse(BaseModel):
    code: int
    message: str

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

    def get(self, path: str, **kwargs) -> SuccessResponse:
        res = self._request("GET", path, **kwargs)
        if res.status_code != 200:
            raise ChzzkAPIError(FailResponse(**res.json()))
        return SuccessResponse(**res.json())

def parse_live_data(chzzk_client:ChzzkClient) -> List[dict]:
    logging.info("Start Parsing Live info")
    raw = []
    try:
        content = chzzk_client.get(path="/open/v1/lives", params={}).content
        next_page = content.get("page").get("next")
        raw.extend(content.get("data", []))

        while next_page:
            content = chzzk_client.get(path="/open/v1/lives", params={"next": next_page}).content
            next_page = content.get("page").get("next")
            raw.extend(content.get("data", []))

    except ChzzkAPIError as e:
        logging.error("Chzzk API error: %s", e)
        return []

    except Exception as e:
        logging.error("Unexpected error while fetching data: %s", e)
        return []

    logging.info(f"Parsed {len(raw)} Live info")
    logging.info("Parsing Done")
    return raw

def parse_channel_data(chzzk_client:ChzzkClient, channel_ids: List[str]) -> List[dict]:
    logging.info("Start Parsing Channel Items")
    channel_items = []
    try:
        for batch_channel_ids in channel_ids[::20]:
            content = chzzk_client.get("/open/v1/channels", params={"channelIds": batch_channel_ids}).content
            documents = content.get("data")
            channel_items.extend(documents)

    except Exception:
        logging.exception("Unexpected error while fetching data")
        return []

    logging.info(f"Parsed {len(channel_items)} Live info")
    logging.info("Parsing Done")
    return channel_items

# import os
# from dotenv import load_dotenv
# import json
#
# if __name__ == "__main__":
#     load_dotenv()
#     chzzk_client = ChzzkClient(os.environ["CHZZK_CLIENT"], os.environ["CHZZK_SECERT"])
#     parse_live_items(chzzk_client)
