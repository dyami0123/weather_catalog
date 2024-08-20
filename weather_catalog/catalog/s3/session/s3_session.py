import os

import boto3
import s3fs
import yaml


class S3SessionObject:

    region: str = "us-west-2"

    s3_client: boto3.client
    s3_fs: s3fs.S3FileSystem

    def __init__(self):
        secrets = self._read_secrets()
        self.s3_client = boto3.client(
            "s3", aws_access_key_id=secrets["aws_access_key_id"], aws_secret_access_key=secrets["aws_secret_access_key"]
        )

        self.fs = s3fs.S3FileSystem(
            key=secrets["aws_access_key_id"],
            secret=secrets["aws_secret_access_key"],
            region_name="us-west-2",
        )

    def _read_secrets(self) -> dict:
        secrets_path = os.path.join(os.path.dirname(__file__), "secrets.yaml")
        # TODO: this is bad practice, should fix to avoid using a secrets file
        with open(secrets_path, "r") as f:
            secrets = yaml.safe_load(f)

        if not all(x in secrets for x in ["aws_access_key_id", "aws_secret_access_key"]):
            raise ValueError(f"Missing AWS credentials in secrets.yaml, please add them to {secrets_path}")
        return secrets


S3Session = S3SessionObject()
