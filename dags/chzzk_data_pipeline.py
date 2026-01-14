import logging
from typing import List
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator
from airflow.sdk import task_group, task
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

S3_BUCKET_NAME = "chzzk-bucket"
S3_PREFIX_LIVE_RAW_DATA = "live_raw_data"
S3_PREFIX_CHANNEL_RAW_DATA = "channel_raw_data"


def get_chzzk_client():
    hook = HttpHook(http_conn_id=CHZZK_API_CONN_ID)
    conn = hook.get_connection(hook.http_conn_id)
    client_id, client_secret = conn.login, conn.password
    chzzk_client = ChzzkClient(client_id, client_secret)
    return chzzk_client


def extract_channel_ids(live_items: List[dict]) -> List[str]:
    return [d["channelId"] for d in live_items]


def get_s3_key(s3_prefix:str, ts_nodash:str):
    return f"{s3_prefix}/{ts_nodash}.json"


with DAG(
    dag_id="chzzk_data_pipeline",
    start_date=datetime(2026, 1, 13),
    description="Chzzk DataPipeline",
    catchup=False,
    max_active_runs=1,
    # schedule="*/30 * * * *",
    schedule=None,
    tags={"chzzk_api", "s3", "chzzk", "live_items", "channel_items"}):
    @task
    def collect_chzzk_live_raw_data(ts_nodash, **context):
        chzzk_client = get_chzzk_client()
        live_items = parse_live_data(chzzk_client)
        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        key = get_s3_key(S3_PREFIX_LIVE_RAW_DATA, ts_nodash)
        hook.load_file_obj(
            file_obj=BytesIO(json.dumps(live_items, ensure_ascii=False).encode("utf-8")),
            key=key,
            bucket_name=S3_BUCKET_NAME,
            replace=True
        )

    @task
    def extract_channel_ids_from_live_raw_data(ts_nodash, **context):
        key = get_s3_key(S3_PREFIX_LIVE_RAW_DATA, ts_nodash)
        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        live_items = json.loads(hook.read_key(key, bucket_name=S3_BUCKET_NAME))
        return [d["channelId"] for d in live_items]

    @task
    def collect_chzzk_channel_raw_data(channel_ids, **context):
        ts_nodash = context["ts_nodash"]
        chzzk_client = get_chzzk_client()
        channel_items = parse_channel_data(chzzk_client, channel_ids)
        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        key = get_s3_key(S3_PREFIX_CHANNEL_RAW_DATA, ts_nodash)
        hook.load_file_obj(
            file_obj=BytesIO(json.dumps(channel_items, ensure_ascii=False).encode("utf-8")),
            key=key,
            bucket_name=S3_BUCKET_NAME,
            replace=True
        )

    def load_snapshots_from_s3(
            *,
            s3_prefix: str,
            domain_model,
            repository_cls,
            ts_nodash: str,
    ):
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        mongo_hook = MongoHook(mongo_conn_id=MONGODB_CONN_ID)

        key = get_s3_key(s3_prefix, ts_nodash)
        raw_data = json.loads(s3_hook.read_key(key, bucket_name=S3_BUCKET_NAME))

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

    @task
    def load_chzzk_live_snapshots(ts_nodash, **context):
        load_snapshots_from_s3(
            s3_prefix=S3_PREFIX_LIVE_RAW_DATA,
            domain_model=LiveSnapshot,
            repository_cls=LiveSnapshotRepository,
            ts_nodash=ts_nodash,
        )

    @task
    def load_chzzk_channel_snapshots(ts_nodash, **context):
        load_snapshots_from_s3(
            s3_prefix=S3_PREFIX_CHANNEL_RAW_DATA,
            domain_model=ChannelSnapshot,
            repository_cls=ChannelSnapshotRepository,
            ts_nodash=ts_nodash,
        )

    @task
    def load_chzzk_channels(ts_nodash, **context):
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        mongo_hook = MongoHook(mongo_conn_id=MONGODB_CONN_ID)

        key = f"{S3_PREFIX_CHANNEL_RAW_DATA}/{ts_nodash}.json"
        raw_data = json.loads(s3_hook.read_key(key, bucket_name=S3_BUCKET_NAME))

        repo = ChannelRepository(mongo_hook.uri)

        def safe_parse(data):
            try:
                channel = Channel(**data)
                return channel.model_dump()
            except Exception as e:
                logging.error(
                    f"Channel parsing failed for {data.get('channel_id')}: {e}"
                )
                return None

        channels = list(filter(None, map(safe_parse, raw_data)))
        repo.insert_batch(channels)

    start_task = EmptyOperator(
        task_id='start_task'
    )

    end_task = EmptyOperator(
        task_id='end_task'
    )

    latest_only_task = LatestOnlyOperator(
        task_id='latest_only_task'
    )

    @task_group
    def collect_raw_data():
        t1 = collect_chzzk_live_raw_data()
        t2 = extract_channel_ids_from_live_raw_data()
        t1 >> t2 >> collect_chzzk_channel_raw_data(t2)

    @task_group
    def load_to_db():
        load_chzzk_live_snapshots()
        load_chzzk_channel_snapshots()
        load_chzzk_channels()

    start_task >> latest_only_task >> collect_raw_data() >> load_to_db() >> end_task
