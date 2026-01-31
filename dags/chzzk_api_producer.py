import logging
from typing import List
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import task, Metadata, Label, dag
import json
from io import BytesIO
from datetime import datetime

from airflow.utils.types import DagRunType

from client.chzzk_client import ChzzkClient, parse_live_data, parse_channel_data
from assets import s3_live_data, s3_channel_data, parse_s3_uri


CHZZK_API_CONN_ID = "CHZZK_API_CONN"

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


@dag(start_date=datetime(2026, 1, 13),
    description="Chzzk DataPipeline",
    catchup=False,
    max_active_runs=1,
    schedule="*/30 * * * *",
    params={"S3_BUCKET_NAME": "hyeseungmoon-chzzk-bucket",
            "S3_LIVE_PREFIX": "live_data_raw",
            "S3_CHANNEL_PREFIX": "channel_data_raw",
            "S3_CONN_ID": "S3_CONN"},
    tags={"chzzk", "producer"})
def chzzk_api_producer():
    @task.branch
    def is_run_backfill(**context):
        dag_run = context["dag_run"]
        if dag_run.run_type == DagRunType.BACKFILL_JOB:
            return "check_s3_key_exists"
        return "collect_chzzk_live_raw_data"

    @task
    def check_s3_key_exists(*, ts_nodash, params):
        s3_live_prefix = params["S3_LIVE_PREFIX"]
        s3_channel_prefix = params["S3_CHANNEL_PREFIX"]
        s3_conn_id = params["S3_CONN_ID"]
        s3_bucket_name = params["S3_BUCKET_NAME"]

        s3_hook = S3Hook(aws_conn_id=s3_conn_id)
        live_key = get_s3_key(s3_prefix=s3_live_prefix, ts_nodash=ts_nodash)
        channel_key = get_s3_key(s3_prefix=s3_channel_prefix, ts_nodash=ts_nodash)
        for key in [live_key, channel_key]:
            if not s3_hook.check_for_key(key, bucket_name=s3_bucket_name):
                raise Exception(f"S3 key {key} doesn't exist")

        for asset, key in zip([s3_live_data, s3_channel_data], [live_key, channel_key]):
            yield Metadata(
                asset=asset,
                extra={"keys": [key]}
            )

    @task
    def collect_chzzk_live_raw_data(*, params, ts_nodash:str):
        s3_conn_id = params["S3_CONN_ID"]

        chzzk_client = get_chzzk_client()
        live_items = parse_live_data(chzzk_client)
        hook = S3Hook(aws_conn_id=s3_conn_id)
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
    def extract_channel_ids_from_live_raw_data(*, params, ts_nodash:str) -> List[str]:
        s3_conn_id = params["S3_CONN_ID"]

        bucket_name, prefix = parse_s3_uri(s3_live_data.uri)
        key = get_s3_key(prefix, ts_nodash)
        hook = S3Hook(aws_conn_id=s3_conn_id)
        live_items = json.loads(hook.read_key(key, bucket_name=bucket_name))
        return [d["channelId"] for d in live_items]

    @task
    def collect_chzzk_channel_raw_data(channel_ids, *, params, ts_nodash:str):
        s3_conn_id = params["S3_CONN_ID"]

        chzzk_client = get_chzzk_client()
        channel_items = parse_channel_data(chzzk_client, channel_ids)
        hook = S3Hook(aws_conn_id=s3_conn_id)
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


chzzk_api_producer()