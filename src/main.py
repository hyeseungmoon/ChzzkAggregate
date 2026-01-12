from repositories.live_Item_repository import LiveItemRepository
from repositories.channel_item_repository import ChannelItemRepository
from client.chzzk_client import ChzzkClient
from dotenv import load_dotenv
from parser.chzzk_parser import ChzzkParser
import os
import logging
from typing import List

def extract_channel_ids(live_items: List[dict]) -> List[str]:
    return [d["channel_id"] for d in live_items]

if __name__ == "__main__":
    load_dotenv()
    logging.root.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logging.root.addHandler(stream_handler)

    client = ChzzkClient(os.environ["CLIENT_ID"], os.environ["CLIENT_SECRET"])
    live_item_repo = LiveItemRepository(os.environ["DATABASE_URI"])
    channel_item_repo = ChannelItemRepository(os.environ["DATABASE_URI"])
    parser = ChzzkParser(client, live_item_repo, channel_item_repo)

    live_items = parser.parse_live_items()
    channel_ids = extract_channel_ids(live_items)
    channel_items = parser.parse_channel_items(channel_ids)
    parser.save_live_items(live_items)
    parser.save_channel_items(channel_items)

