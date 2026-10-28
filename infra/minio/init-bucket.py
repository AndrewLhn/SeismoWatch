"""Create the raw-snapshot bucket once the S3 endpoint accepts requests.

Replaces the historical `mc mb` bootstrap: the MinIO client is no longer
published in a pinnable form (upstream archived in 2026), and a plain S3 call
works against any MinIO-compatible server, fork included.
"""

import os
import sys
import time

from minio import Minio

ENDPOINT = os.environ["MINIO_ENDPOINT"].replace("http://", "").replace("https://", "")
BUCKET = os.getenv("SEISMIC_BUCKET", "seismic-raw")
ATTEMPTS = 30

client = Minio(
    ENDPOINT,
    access_key=os.environ["MINIO_ROOT_USER"],
    secret_key=os.environ["MINIO_ROOT_PASSWORD"],
    secure=False,
)

for attempt in range(1, ATTEMPTS + 1):
    try:
        if client.bucket_exists(BUCKET):
            print(f"bucket present: {BUCKET}")
        else:
            client.make_bucket(BUCKET)
            print(f"bucket created: {BUCKET}")
        sys.exit(0)
    except Exception as error:  # noqa: BLE001 - server still starting
        print(f"attempt {attempt}: S3 endpoint not ready ({error})")
        time.sleep(2)

sys.exit(f"S3 endpoint not reachable after {ATTEMPTS} attempts")
