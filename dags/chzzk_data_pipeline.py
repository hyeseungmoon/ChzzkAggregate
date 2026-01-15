import logging
from typing import List
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
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

from repositories.sqlalchemy.sqlalchemy_channel_repository import SqlAlchemyChannelRepository
from repositories.sqlalchemy.sqlalchemy_channel_snapshot_repository import SqlAlchemyChannelSnapshotRepository
from repositories.sqlalchemy.sqlalchemy_live_snapshot_repository import SqlAlchemyLiveSnapshotRepository

CHZZK_API_CONN_ID = "CHZZK_API_CONN"
S3_CONN_ID = "S3_CONN"
MONGODB_CONN_ID = "MONGODB_CONN"
POSTGRES_CONN_ID = "POSTGRES_CONN"

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
    schedule="*/30 * * * *",
    tags={"chzzk_api", "s3", "chzzk", "live_items", "channel_items"}):
    @task
    def collect_chzzk_live_raw_data(**context):
        ts_nodash = context["ts_nodash"]
        logging.info(ts_nodash)
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
    def extract_channel_ids_from_live_raw_data(**context):
        ts_nodash = context["ts_nodash"]
        logging.info(ts_nodash)
        key = get_s3_key(S3_PREFIX_LIVE_RAW_DATA, ts_nodash)
        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        live_items = json.loads(hook.read_key(key, bucket_name=S3_BUCKET_NAME))
        return [d["channelId"] for d in live_items]

    @task
    def collect_chzzk_channel_raw_data(channel_ids, **context):
        ts_nodash = context["ts_nodash"]
        logging.info(ts_nodash)
        chzzk_client = get_chzzk_client()
        channel_items = parse_channel_data(chzzk_client, channel_ids)
        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        key = get_s3_key(S3_PREFIX_CHANNEL_RAW_DATA, ts_nodash)
        logging.info(key)
        hook.load_file_obj(
            file_obj=BytesIO(json.dumps(channel_items, ensure_ascii=False).encode("utf-8")),
            key=key,
            bucket_name=S3_BUCKET_NAME,
            replace=True
        )

    @task
    def load_chzzk_live_snapshots(ts_nodash, **context):
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        live_snapshots_repository = SqlAlchemyLiveSnapshotRepository(postgres_hook.sqlalchemy_url)

        key = get_s3_key(S3_PREFIX_LIVE_RAW_DATA, ts_nodash)
        raw_data_list = json.loads(s3_hook.read_key(key, bucket_name=S3_BUCKET_NAME))
        timestamp = datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S")
        live_snapshots = [
            LiveSnapshot(**raw_data, timestamp=timestamp)
            for raw_data in raw_data_list
        ]
        live_snapshots_repository.save_live_snapshots(live_snapshots)

    @task
    def load_chzzk_channel_snapshots(ts_nodash, **context):
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        channel_snapshots_repository = SqlAlchemyChannelSnapshotRepository(postgres_hook.sqlalchemy_url)

        key = get_s3_key(S3_PREFIX_CHANNEL_RAW_DATA, ts_nodash)
        raw_data_list = json.loads(s3_hook.read_key(key, bucket_name=S3_BUCKET_NAME))
        timestamp = datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S")
        channel_snapshots = [
            ChannelSnapshot(**raw_data, timestamp=timestamp)
            for raw_data in raw_data_list
        ]
        channel_snapshots_repository.save_channel_snapshots(channel_snapshots)

    @task
    def load_chzzk_channels(ts_nodash, **context):
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        channel_repository = SqlAlchemyChannelRepository(postgres_hook.sqlalchemy_url)

        key = get_s3_key(S3_PREFIX_CHANNEL_RAW_DATA, ts_nodash)
        raw_data_list = json.loads(s3_hook.read_key(key, bucket_name=S3_BUCKET_NAME))
        channels = [
            Channel(**raw_data)
            for raw_data in raw_data_list
        ]
        channel_repository.save_channels(channels)

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
        t3 = collect_chzzk_channel_raw_data(t2)
        t1 >> t2 >> t3

    @task_group
    def load_to_db():
        load_chzzk_channels() >> [load_chzzk_live_snapshots(), load_chzzk_channel_snapshots()]

    start_task >> latest_only_task >> collect_raw_data() >> load_to_db() >> end_task
