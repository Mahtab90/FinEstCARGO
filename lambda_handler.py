import boto3
import os
import json
import base64
from aws_encryption_sdk import encrypt, CommitmentPolicy

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
kms_key = os.environ.get('KMS_KEY_ID')
S3_BUCKET = os.environ.get('CARGO_INGEST_BUCKET')
AUDIT_TABLE = os.environ.get('CARGO_AUDIT_TABLE')

def handler(event, context):
    # Validate schema
    body = event.get('body') if isinstance(event.get('body'), str) else json.dumps(event.get('body'))
    record = json.loads(body)
    # schema validation here (jsonschema)
    # Encrypt with KMS envelope (aws-encryption-sdk recommended)
    ciphertext, header = encrypt(source=body.encode('utf-8'), key_id=kms_key, commitment_policy=CommitmentPolicy.REQUIRE_ENCRYPT_REQUIRE_DECRYPT)
    key = "ingestion/record-{}.bin".format(context.aws_request_id)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=ciphertext)
    # write audit log
    table = dynamodb.Table(AUDIT_TABLE)
    table.put_item(Item={'request_id': context.aws_request_id, 'timestamp': int(context.timestamp), 's3_key': key, 'hash': header})
    return {"statusCode":200, "body":"ok"}
