FROM apache/airflow:3.1.5

COPY requirements.txt /requirements.txt

RUN pip install -r /requirements.txt

USER airflow