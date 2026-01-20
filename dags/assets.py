from urllib.parse import urlparse

from airflow import Asset


def parse_s3_uri(uri: str):
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError("Not an S3 URI")
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    return bucket, key

s3_live_data = Asset(
    name="s3_live_data",
    uri="s3://hyeseungmoon-chzzk-bucket/live_data_raw"
)

s3_channel_data = Asset(
    name="s3_channel_data",
    uri="s3://hyeseungmoon-chzzk-bucket/channel_data_raw"
)