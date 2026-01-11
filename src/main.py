from pymongo.errors import BulkWriteError, PyMongoError

from repositories.live_item_repository import LiveItemRepository
from client.chzzk_client import ChzzkClient, ChzzkAPIError
from dotenv import load_dotenv
import os
from schemas.response_models import LiveResponseBody
import datetime
import logging

def parse(client:ChzzkClient, repo:LiveItemRepository) -> None:
    live_item_list = []
    try:
        content = client.get(path="/open/v1/lives", content_type=LiveResponseBody, params={}).content
        documents = content.model_dump()["data"]
        next_page = content.page.next
        live_item_list.extend(documents)

        while next_page:
            content = client.get(path="/open/v1/lives", content_type=LiveResponseBody,
                                 params={"next": next_page}).content
            documents = content.model_dump()["data"]
            next_page = content.page.next
            live_item_list.extend(documents)

            if len(live_item_list) % 1000 == 0:
                logging.info(f"Fetched {len(live_item_list)} Live Streams")

    except ChzzkAPIError as e:
        logging.error("Chzzk API error: %s", e)
        return

    except Exception as e:
        logging.error("Unexpected error while fetching data: %s", e)
        return

    current_datetime = datetime.datetime.now()
    for document in live_item_list:
        document["parsed_date"] = current_datetime

    try:
        inserted_cnt = repo.insert_batch(live_item_list)
        logging.info("Inserted %d Docs" % inserted_cnt)
    except BulkWriteError as bwe:
        for error in bwe.details['writeErrors']:
            failed_doc = error['op']
            logging.error("Failed document: %s | Error: %s", failed_doc, error['errmsg'])
        logging.warning("Some documents failed to insert")
    except PyMongoError as e:
        logging.error("MongoDB error: %s", e)

if __name__ == "__main__":
    load_dotenv()
    logging.root.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logging.root.addHandler(stream_handler)

    client = ChzzkClient(os.environ["CLIENT_ID"], os.environ["CLIENT_SECRET"])
    repo = LiveItemRepository(os.environ["DATABASE_URI"])

    logging.info("Start Fetching Live info")
    parse(client, repo)
    logging.info("Fetching Done")