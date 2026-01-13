import logging
from typing import List
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator
from airflow.sdk import TaskGroup
import json
from io import BytesIO
from datetime import datetime
from client.chzzk_client import ChzzkClient, parse_live_data, parse_channel_data
from domain.channel import Channel
from domain.channel_snapshot import ChannelSnapshot
from domain.live_snapshot import LiveSnapshot
from repositories.channel_snapshot_repository import ChannelSnapshotRepository
from repositories.live_snapshot_repository import LiveSnapshotRepository
from repositories.channel_repository import ChannelRepository

CHZZK_API_CONN_ID = "CHZZK_API_CONN"
S3_CONN_ID = "S3_CONN"
MONGODB_CONN_ID = "MONGODB_CONN"
BUCKET_NAME = "chzzk-bucket"
LIVE_RAW_DATA_S3_PREFIX = "live_raw_data"
CHANNEL_RAW_DATA_S3_PREFIX = "channel_raw_data"


def get_chzzk_client():
    hook = HttpHook(http_conn_id=CHZZK_API_CONN_ID)
    conn = hook.get_connection(hook.http_conn_id)
    client_id, client_secret = conn.login, conn.password
    chzzk_client = ChzzkClient(client_id, client_secret)
    return chzzk_client


def extract_channel_ids(live_items: List[dict]) -> List[str]:
    return [d["channelId"] for d in live_items]


def push_items_to_bucket(hook:S3Hook, items:List[dict], item_name:str, ts_nodash: str) -> str:
    key = f"{item_name}/{ts_nodash}.json"

    hook.load_file_obj(
        file_obj=BytesIO(json.dumps(items, ensure_ascii=False).encode("utf-8")),
        key=key,
        bucket_name=BUCKET_NAME,
        replace=True
    )
    return key


with DAG(
    dag_id="chzzk_data_pipeline",
    start_date=datetime(2026, 1, 13),
    description="Chzzk DataPipeline",
    catchup=False,
    max_active_runs=1,
    schedule="*/30 * * * *",
    tags={"chzzk_api", "s3", "chzzk", "live_items", "channel_items"},
) as dag:

    def collect_chzzk_live_raw_data(**context):
        chzzk_client = get_chzzk_client()
        live_items = parse_live_data(chzzk_client)

        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        push_items_to_bucket(hook, live_items, LIVE_RAW_DATA_S3_PREFIX, context["ts_nodash"])

    def collect_chzzk_channel_raw_data_from_live(**context):
        chzzk_client = get_chzzk_client()
        live_raw_data_key = f"{LIVE_RAW_DATA_S3_PREFIX}/{context["ts_nodash"]}.json"
        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        live_items = json.loads(hook.read_key(live_raw_data_key, bucket_name=BUCKET_NAME))
        channel_ids = extract_channel_ids(live_items)
        channel_items = parse_channel_data(chzzk_client, channel_ids)
        push_items_to_bucket(hook, channel_items, CHANNEL_RAW_DATA_S3_PREFIX, context["ts_nodash"])

    def load_snapshots_from_s3(
            *,
            s3_prefix: str,
            domain_model,
            repository_cls,
            ts_nodash: str,
    ):
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        mongo_hook = MongoHook(mongo_conn_id=MONGODB_CONN_ID)

        key = f"{s3_prefix}/{ts_nodash}.json"
        raw_data = json.loads(s3_hook.read_key(key, bucket_name=BUCKET_NAME))

        timestamp = datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S")
        repo = repository_cls(mongo_hook.uri)

        def safe_parse(data):
            try:
                snapshot = domain_model(**data, timestamp=timestamp)
                return snapshot.model_dump()
            except Exception as e:
                logging.error(
                    f"Parsing failed for {data.get('live_id')}: {e}"
                )
                return None

        snapshots = list(filter(None, map(safe_parse, raw_data)))
        repo.insert_batch(snapshots)


    def load_channels_from_s3(
            *,
            s3_prefix: str,
            domain_model,
            repository_cls,
            ts_nodash: str,
    ):
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        mongo_hook = MongoHook(mongo_conn_id=MONGODB_CONN_ID)

        key = f"{s3_prefix}/{ts_nodash}.json"
        raw_data = json.loads(s3_hook.read_key(key, bucket_name=BUCKET_NAME))

        repo = repository_cls(mongo_hook.uri)

        def safe_parse(data):
            try:
                channel = domain_model(**data)
                return channel.model_dump()
            except Exception as e:
                logging.error(
                    f"Channel parsing failed for {data.get('channel_id')}: {e}"
                )
                return None

        channels = list(filter(None, map(safe_parse, raw_data)))
        repo.insert_batch(channels)


    def load_chzzk_live_snapshots(**context):
        load_snapshots_from_s3(
            s3_prefix=LIVE_RAW_DATA_S3_PREFIX,
            domain_model=LiveSnapshot,
            repository_cls=LiveSnapshotRepository,
            ts_nodash=context["ts_nodash"],
        )

    def load_chzzk_channel_snapshots(**context):
        load_snapshots_from_s3(
            s3_prefix=CHANNEL_RAW_DATA_S3_PREFIX,
            domain_model=ChannelSnapshot,
            repository_cls=ChannelSnapshotRepository,
            ts_nodash=context["ts_nodash"],
        )

    def load_chzzk_channels(**context):
        load_channels_from_s3(
            s3_prefix=CHANNEL_RAW_DATA_S3_PREFIX,
            domain_model=Channel,
            repository_cls=ChannelRepository,
            ts_nodash=context["ts_nodash"],
        )

    start_task = EmptyOperator(
        task_id='start_task'
    )

    end_task = EmptyOperator(
        task_id='end_task'
    )

    latest_only_task = LatestOnlyOperator(
        task_id='latest_only_task'
    )

    with TaskGroup("collect_raw_data") as collect_raw_data:
        collect_chzzk_live_raw_data_task = PythonOperator(
            task_id='collect_chzzk_live_raw_data',
            python_callable=collect_chzzk_live_raw_data,
        )
        collect_chzzk_channel_raw_data_from_live_task = PythonOperator(
            task_id='collect_chzzk_channel_raw_data_from_live',
            python_callable=collect_chzzk_channel_raw_data_from_live,
        )

        collect_chzzk_live_raw_data_task >> collect_chzzk_channel_raw_data_from_live_task

    with TaskGroup("load_to_db") as load_to_db:
        load_chzzk_live_snapshots_task = PythonOperator(
            task_id='load_chzzk_live_snapshots',
            python_callable=load_chzzk_live_snapshots,
            trigger_rule="none_failed",
        )

        load_chzzk_channel_snapshots_task = PythonOperator(
            task_id='load_chzzk_channel_snapshots',
            python_callable=load_chzzk_channel_snapshots,
            trigger_rule="none_failed",
        )

        load_chzzk_channels_task = PythonOperator(
            task_id='load_chzzk_channels',
            python_callable=load_chzzk_channels,
            trigger_rule="none_failed",
        )

    start_task >> latest_only_task >> collect_raw_data >> load_to_db >> end_task
