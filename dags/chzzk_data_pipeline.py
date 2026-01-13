from typing import List
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import json
from io import BytesIO
from datetime import datetime
from client.chzzk_client import ChzzkClient, parse_live_data, parse_channel_data

CHZZK_API_CONN_ID = "chzzk_api_conn"
S3_CONN_ID = "rustfs-bucket"
BUCKET_NAME = "chzzk-bucket"

def get_chzzk_client():
    hook = HttpHook(http_conn_id=CHZZK_API_CONN_ID)
    conn = hook.get_connection(hook.http_conn_id)
    client_id, client_secret = conn.login, conn.password
    chzzk_client = ChzzkClient(client_id, client_secret)
    return chzzk_client


def extract_channel_ids(live_items: List[dict]) -> List[str]:
    return [d["channelId"] for d in live_items]


def push_items_to_bucket(hook:S3Hook, items:List[dict], item_name:str, ds: str, ts: str) -> str:
    ds = ds
    ts_dt = datetime.fromisoformat(ts)
    ts_formatted = ts_dt.strftime("%H-%M")
    key = f"{item_name}/{ds}/{ts_formatted}.json"

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

    def chzzk_parse_live_items(**context):
        chzzk_client = get_chzzk_client()
        live_items = parse_live_data(chzzk_client)

        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        key = push_items_to_bucket(hook, live_items, "live_items", context["ds"], context["ts"])
        return key

    chzzk_parse_live_items_task = PythonOperator(
        task_id='chzzk_parse_live_items',
        python_callable=chzzk_parse_live_items,
    )

    def chzzk_parse_channel_items(**context):
        ti = context["ti"]
        chzzk_client = get_chzzk_client()
        key = ti.xcom_pull(task_ids="chzzk_parse_live_items")

        hook = S3Hook(aws_conn_id='rustfs-bucket')
        live_items = json.loads(hook.read_key(key, bucket_name="chzzk-bucket"))
        channel_ids = extract_channel_ids(live_items)
        channel_items = parse_channel_data(chzzk_client, channel_ids)

        push_items_to_bucket(hook, channel_items, "channel_items", context["ds"], context["ts"])


    chzzk_parse_channel_items_taks = PythonOperator(
        task_id='chzzk_parse_channel_items',
        python_callable=chzzk_parse_channel_items,
    )

    chzzk_parse_live_items_task >> chzzk_parse_channel_items_taks