from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import task

from repositories.sqlalchemy.base_sqlalchemy_repository import Base

POSTGRES_CONN_ID = "POSTGRES_CONN"

with DAG(
    dag_id="db_init",
    description="Initialize Database recreate tables",
    catchup=False,
    max_active_runs=1,
    schedule=None,
) as dag_init:
    @task
    def init_db():
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = postgres_hook.get_sqlalchemy_engine()
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)