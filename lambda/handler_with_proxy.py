import uuid
from os import getenv
from json import loads
from boto3 import client
from pandas import json_normalize
from datetime import datetime, timezone
from urllib.request import build_opener, ProxyHandler

def consume_api(event, context):
    # validate env variables
    endpoint = getenv("ENDPOINT_URL")
    if not endpoint:
        raise RuntimeError("ENDPOINT_URL not configured")

    bucket = getenv("S3_BUCKET")
    if not bucket:
        raise RuntimeError("S3_BUCKET not configured")

    prefix = getenv("S3_PREFIX", "")
    if not bucket:
        raise RuntimeError("S3_PREFIX not configured")

    sm = client("secretsmanager")
    res = sm.get_secret_value(SecretId="PROXY_URL")
    secret = res.get("SecretString")

    if not secret:
        raise RuntimeError("SecretString its empty Secrets Manager")

    # configure proxy
    secret = loads(secret)
    proxies = {"http": secret['PROXY_URL'], "https": secret['PROXY_URL']}
    opener = build_opener(ProxyHandler(proxies))

    # make request
    with opener.open(endpoint) as resp:
        code = resp.code
        if code >= 200 and code < 400:
            content = resp.read()
            # make path
            now = datetime.now(timezone.utc)
            date_path = now.strftime("%Y/%m/%d")
            time_part = now.strftime("%H%M%S")
            key = f"{prefix}{date_path}/{time_part}-{uuid.uuid4().hex}.parquet"
            # read data
            content = loads(content.decode("utf-8"))
            df = json_normalize(content["results"], sep="_")
            df['location_postcode'] = df['location_postcode'].astype(str)
            # put on s3
            s3 = client("s3")
            s3.put_object(
                Bucket=bucket,    
                Key=key,
                Body=df.to_parquet(),
                ContentType="application/vnd.apache.parquet",
            )
            return "request succesfully"
        else:
            raise ValueError("Error {code} in {endpoint} request")
    return
