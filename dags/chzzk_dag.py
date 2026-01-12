from typing import List

from airflow import DAG
from pendulum import datetime, timezone
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook

from client.chzzk_client import ChzzkClient
from parser.chzzk_parser import ChzzkParser
from repositories.channel_item_repository import ChannelItemRepository
from repositories.live_Item_repository import LiveItemRepository


def get_chzzk_credentials():
    conn = HttpHook.get_connection("chzzk_api_conn")
    client_id = conn.login
    client_secret = conn.password

    return client_id, client_secret

def get_mongo_credentials():
    conn = HttpHook.get_connection("mongo_conn")
    mongo_uri = conn.password

    return mongo_uri

def extract_channel_ids(live_items: List[dict]) -> List[str]:
    return [d["channel_id"] for d in live_items]

def get_parser():
    client_id, client_secret = get_chzzk_credentials()
    database_uri = get_mongo_credentials()

    client = ChzzkClient(client_id, client_secret)
    live_item_repo = LiveItemRepository(database_uri)
    channel_item_repo = ChannelItemRepository(database_uri)

    return ChzzkParser(client, live_item_repo, channel_item_repo)

with DAG(
    dag_id="chzzk_parse_dag",
    description="parse Chzzk's Live items and Channel items",
    start_date=datetime(2026, 1, 12, tz=timezone("Asia/Seoul")),
    schedule="*/6 * * * *",
    tags=["chzzk"],
) as dag:


    def task_parse_live_items(**context):
        parser = get_parser()
        return parser.parse_live_items()

    parse_live_items = PythonOperator(
        task_id='parse_live_items',
        python_callable=task_parse_live_items,
        dag=dag
    )


    def task_extract_channel_ids(**context):
        ti = context["ti"]
        live_items = ti.xcom_pull(task_ids="parse_live_items")
        return extract_channel_ids(live_items)


    extract_channel_ids_task = PythonOperator(
        task_id="extract_channel_ids",
        python_callable=task_extract_channel_ids,
    )


    def task_parse_channel_items(**context):
        ti = context["ti"]
        channel_ids = ti.xcom_pull(task_ids="extract_channel_ids")
        parser = get_parser()
        return parser.parse_channel_items(channel_ids)


    parse_channel_items = PythonOperator(
        task_id="parse_channel_items",
        python_callable=task_parse_channel_items,
    )


    def task_save_live_items(**context):
        ti = context["ti"]
        live_items = ti.xcom_pull(task_ids="parse_live_items")
        parser = get_parser()
        parser.save_live_items(live_items)


    save_live_items = PythonOperator(
        task_id="save_live_items",
        python_callable=task_save_live_items,
    )


    def task_save_channel_items(**context):
        ti = context["ti"]
        channel_items = ti.xcom_pull(task_ids="parse_channel_items")
        parser = get_parser()
        parser.save_channel_items(channel_items)


    save_channel_items = PythonOperator(
        task_id="save_channel_items",
        python_callable=task_save_channel_items,
    )

    (
        parse_live_items
        >> extract_channel_ids_task
        >> parse_channel_items
        >> save_channel_items
    )

    parse_live_items >> save_live_items
