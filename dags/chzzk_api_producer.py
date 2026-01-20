import logging
from typing import List
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook
from airflow import DAG, Asset
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import task, Metadata, Label
import json
from io import BytesIO
from datetime import datetime

from airflow.utils.types import DagRunType

from client.chzzk_client import ChzzkClient, parse_live_data, parse_channel_data
from assets import s3_live_data, s3_channel_data, parse_s3_uri

CHZZK_API_CONN_ID = "CHZZK_API_CONN"
S3_CONN_ID = "S3_CONN"
S3_BUCKET_NAME = "hyeseungmoon-bucket"
S3_LIVE_PREIX = "live_data_raw"
S3_CHANNEL_PREIX = "channel_data_raw"

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
    dag_id="chzzk_api_producer",
    start_date=datetime(2026, 1, 13),
    description="Chzzk DataPipeline",
    catchup=False,
    max_active_runs=1,
    schedule="*/30 * * * *",
    tags={"chzzk_api", "s3", "chzzk", "live_items", "channel_items"}):
    @task.branch
    def is_run_backfill(**context):
        dag_run = context["dag_run"]
        if dag_run.run_type == DagRunType.BACKFILL_JOB:
            return "skip_task"
        return "real_task"


    @task
    def check_s3_key_exists(*, ts_nodash) -> list[Asset]:
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        for asset in [s3_live_data, s3_channel_data]:
            key = get_s3_key(s3_prefix=S3_LIVE_PREIX, ts_nodash=ts_nodash)
            if not s3_hook.check_for_key(key, bucket_name=S3_BUCKET_NAME):
                raise Exception(f"S3 key {key} doesn't exist")
            else:
                yield Metadata(
                    asset=asset,
                    extra={"keys": [key]}
                )

    @task
    def collect_chzzk_live_raw_data(*, ts_nodash:str) -> Asset:
        chzzk_client = get_chzzk_client()
        live_items = parse_live_data(chzzk_client)
        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        bucket_name, prefix = parse_s3_uri(s3_live_data.uri)
        key = get_s3_key(prefix, ts_nodash)
        hook.load_file_obj(
            file_obj=BytesIO(json.dumps(live_items, ensure_ascii=False).encode("utf-8")),
            key=key,
            bucket_name=bucket_name,
            replace=True
        )

        yield Metadata(
            s3_live_data,
            extra={"keys": [key]}
        )

    @task
    def extract_channel_ids_from_live_raw_data(*, ts_nodash:str) -> List[str]:
        bucket_name, prefix = parse_s3_uri(s3_live_data.uri)
        key = get_s3_key(prefix, ts_nodash)
        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        live_items = json.loads(hook.read_key(key, bucket_name=bucket_name))
        return [d["channelId"] for d in live_items]

    @task
    def collect_chzzk_channel_raw_data(channel_ids, *, ts_nodash:str) -> Asset:
        chzzk_client = get_chzzk_client()
        channel_items = parse_channel_data(chzzk_client, channel_ids)
        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        bucket_name, prefix = parse_s3_uri(s3_channel_data.uri)
        key = get_s3_key(prefix, ts_nodash)
        logging.info(key)
        hook.load_file_obj(
            file_obj=BytesIO(json.dumps(channel_items, ensure_ascii=False).encode("utf-8")),
            key=key,
            bucket_name=bucket_name,
            replace=True
        )
        yield Metadata(
            asset=s3_channel_data,
            extra={"keys": [key]}
        )
        return s3_channel_data

    end_task = EmptyOperator(task_id="trigger_asset", outlets=[s3_live_data, s3_channel_data])
    start_task = EmptyOperator(task_id="start_task")

    t1 = collect_chzzk_live_raw_data()
    t2 = extract_channel_ids_from_live_raw_data()
    t3 = collect_chzzk_channel_raw_data(t2)

    is_run_backfill = is_run_backfill()
    start_task >> is_run_backfill
    is_run_backfill >> Label("Yes") >> check_s3_key_exists() >> end_task
    is_run_backfill >> Label("No") >> t1 >> t2 >> t3 >> end_task


