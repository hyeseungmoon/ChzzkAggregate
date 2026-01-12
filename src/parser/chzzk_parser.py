import datetime
from client.chzzk_client import ChzzkClient, ChzzkAPIError
from repositories.channel_item_repository import ChannelItemRepository
from repositories.live_Item_repository import LiveItemRepository
from schemas.response_models import LiveResponseBody, ChannelResponseBody
import logging
from typing import List, Protocol


class BatchInsertRepository(Protocol):
    def insert_batch(self, item_list: List[dict]) -> int:
        ...


class ChzzkParser:
    def __init__(self, client:ChzzkClient,
                 live_item_repo:LiveItemRepository,
                 channel_item_repo:ChannelItemRepository):
        self.api_client = client
        self.live_item_repo = live_item_repo
        self.channel_item_repo = channel_item_repo


    def _add_time_stamp(self, items: List[dict]) -> None:
        current_datetime = datetime.datetime.now()
        for document in items:
            document["time_stamp"] = current_datetime

    def parse_live_items(self) -> List[dict]:
        logging.info("Start Parsing Live info")
        live_items = []
        try:
            content = self.api_client.get(path="/open/v1/lives", content_type=LiveResponseBody, params={}).content
            documents = content.model_dump()["data"]
            next_page = content.page.next
            live_items.extend(documents)

            while next_page:
                content = self.api_client.get(path="/open/v1/lives", content_type=LiveResponseBody,
                                     params={"next": next_page}).content
                documents = content.model_dump()["data"]
                next_page = content.page.next
                live_items.extend(documents)

        except ChzzkAPIError as e:
            logging.error("Chzzk API error: %s", e)
            return []

        except Exception as e:
            logging.error("Unexpected error while fetching data: %s", e)
            return []

        self._add_time_stamp(live_items)

        logging.info(f"Parsed {len(live_items)} Live info")
        logging.info("Parsing Done")
        return live_items


    def parse_channel_items(self, channel_ids: List[str]) -> List[dict]:
        logging.info("Start Parsing Channel Items")
        channel_items = []
        try:
            for batch_channel_ids in channel_ids[::20]:
                content = self.api_client.get("/open/v1/channels", content_type=ChannelResponseBody, params={"channelIds": batch_channel_ids}).content
                documents = content.model_dump()["data"]
                channel_items.extend(documents)
            self._add_time_stamp(channel_items)

        except Exception:
            logging.exception("Unexpected error while fetching data")
            return []

        logging.info(f"Parsed {len(channel_items)} Live info")
        logging.info("Parsing Done")
        return channel_items


    def _save_items(
            self,
            repo: BatchInsertRepository,
            item_list: List[dict]
    ) -> None:

        try:
            inserted_cnt = repo.insert_batch(item_list)
            logging.info("Inserted %d Docs", inserted_cnt)

        except Exception:
            logging.exception("insert_batch failed")


    def save_live_items(self, item_list:List[dict]) -> None:
        logging.info("Saving LiveItems To DB")
        self._save_items(self.live_item_repo, item_list)


    def save_channel_items(self, item_list:List[dict]) -> None:
        logging.info("Saving ChannelItems To DB")
        self._save_items(self.channel_item_repo, item_list)

