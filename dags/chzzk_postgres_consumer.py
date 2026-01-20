import json
from datetime import datetime

from airflow import DAG, Asset
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import task

from assets import s3_live_data, s3_channel_data
from domain.channel import Channel
from domain.channel_snapshot import ChannelSnapshot
from domain.live_snapshot import LiveSnapshot
from repositories.sqlalchemy.sqlalchemy_channel_repository import SqlAlchemyChannelRepository
from repositories.sqlalchemy.sqlalchemy_channel_snapshot_repository import SqlAlchemyChannelSnapshotRepository
from repositories.sqlalchemy.sqlalchemy_live_snapshot_repository import SqlAlchemyLiveSnapshotRepository

POSTGRES_CONN_ID = "POSTGRES_CONN"

with DAG(
    dag_id='chzzk_postgres_consumer',
    schedule=[s3_live_data, s3_channel_data],
    description="Chzzk DataPipeline",
    tags={"chzzk_api", "s3", "chzzk", "live_items", "channel_items"}) as dag:
    @task(inlets=[s3_live_data, s3_channel_data])
    def load_keys(*, inlet_events):
        events = inlet_events[s3_live_data]
        keys = events.extra.get("keys")
        return keys

    @task
    def load_chzzk_channels(keys, **context):
        s3_hook = S3Hook(aws_conn_id=s3_live_data.extra.get("S3_CONN_ID"))

        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        channel_repository = SqlAlchemyChannelRepository(postgres_hook.sqlalchemy_url)

        batch = []
        for key in keys:
            raw_data_list = json.loads(s3_hook.read_key(key, bucket_name=s3_live_data.extra.get("S3_BUCKET_NAME")))
            batch.extend(
                Channel(**raw_data)
                for raw_data in raw_data_list
            )
        channel_repository.save_channels(batch)


    @task
    def load_chzzk_live_snapshots(keys, **context):
        ts_nodash = context["ts_nodash"]
        s3_hook = S3Hook(aws_conn_id=s3_live_data.extra.get("S3_CONN_ID"))
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        live_snapshots_repository = SqlAlchemyLiveSnapshotRepository(postgres_hook.sqlalchemy_url)

        for key in keys:
            raw_data_list = json.loads(s3_hook.read_key(key, bucket_name=s3_live_data.extra.get("S3_BUCKET_NAME")))
            timestamp = datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S")
            live_snapshots = [
                LiveSnapshot(**raw_data, timestamp=timestamp)
                for raw_data in raw_data_list
            ]
            live_snapshots_repository.save_live_snapshots(live_snapshots)


    @task
    def load_chzzk_channel_snapshots(keys, **context):
        ts_nodash = context["ts_nodash"]
        s3_hook = S3Hook(aws_conn_id=s3_live_data.extra.get("S3_CONN_ID"))

        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        channel_snapshots_repository = SqlAlchemyChannelSnapshotRepository(postgres_hook.sqlalchemy_url)

        for key in keys:
            raw_data_list = json.loads(s3_hook.read_key(key, bucket_name=s3_live_data.extra.get("S3_BUCKET_NAME")))
            timestamp = datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S")
            channel_snapshots = [
                ChannelSnapshot(**raw_data, timestamp=timestamp)
                for raw_data in raw_data_list
            ]
            channel_snapshots_repository.save_channel_snapshots(channel_snapshots)

    keys = load_keys()
    keys = load_chzzk_channels(keys)
    load_chzzk_live_snapshots(keys)
    load_chzzk_channel_snapshots(keys)