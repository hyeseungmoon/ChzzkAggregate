from airflow.sdk import task
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from assets import s3_live_data, s3_channel_data

DEST_CONN_ID = "S3_CONN"
DEST_BUCKET_NAME = "hyeseungmoon-bucket"
S3_LIVE_PREFIX = "live_data_raw"
S3_CHANNEL_PREFIX = "channel_data_raw"

SRC_CONN_ID = "S3_CONN"
SRC_BUCKET_NAME = "hyeseungmoon-bucket"

with DAG(
    dag_id="chzzk_s3_to_s3_producer",
    schedule=None,
    tags=["chzzk"],
) as dag:
    @task(task_id="parse_source_keys")
    def parse_source_keys():
        hook = S3Hook(SRC_CONN_ID)
        return hook.list_prefixes(
            bucket_name=SRC_BUCKET_NAME,
            prefix=S3_LIVE_PREFIX,
        )


    @task(task_id="parse_dest_keys")
    def parse_dest_keys():
        hook = S3Hook(DEST_CONN_ID)
        return hook.list_prefixes(
            bucket_name=DEST_BUCKET_NAME,
            prefix=S3_CHANNEL_PREFIX,
        )


    @task(
        outlets=[
            s3_live_data, s3_channel_data
        ]
    )
    def transfer_missing_objects(source_keys, dest_keys):
        pass

    source_keys = parse_source_keys()
    dest_keys = parse_dest_keys()
    transfer_missing_objects(source_keys, dest_keys)
