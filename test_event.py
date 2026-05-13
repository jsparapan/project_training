# test_event.py — run locally to put a test order into Kinesis
# Usage: python test_event.py
#
# Requirements: pip install boto3
# Make sure your AWS CLI is configured before running.

import base64
import json
import boto3

STREAM_NAME = "order-events"
REGION      = "eu-west-1"

client = boto3.client("kinesis", region_name=REGION)

orders = [
    {
        "order_id": "ord-001",
        "user_id":  "usr-42",
        "total":    99.99,
        "status":   "placed",
        "items": [
            {"sku": "SHOE-001", "qty": 1, "price": 79.99},
            {"sku": "LACE-002", "qty": 2, "price": 9.99},
        ],
    },
    {
        "order_id": "ord-002",
        "user_id":  "usr-17",
        "total":    249.00,
        "status":   "placed",
        "items": [
            {"sku": "JACKET-003", "qty": 1, "price": 249.00},
        ],
    },
    {
        "order_id": "ord-003",
        "user_id":  "usr-42",
        "total":    15.50,
        "status":   "placed",
        "items": [
            {"sku": "SOCK-010", "qty": 3, "price": 5.00},
            {"sku": "TAG-001",  "qty": 1, "price": 0.50},
        ],
    },
    {
        "order_id": "ord-004",
        "user_id":  "usr-42",
        "total":    115.50,
        "status":   "placed",
        "items": [
            {"sku": "SOCK-010", "qty": 3, "price": 5.00},
            {"sku": "TAG-001",  "qty": 1, "price": 0.50},
            {"sku": "CAP-001",  "qty": 1, "price": 100.00},
        ],
    },
]

for order in orders:
    data = base64.b64encode(json.dumps(order).encode()).decode()
    resp = client.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(order),          # Kinesis handles encoding
        PartitionKey=order["order_id"],
    )
    print(f"Sent {order['order_id']} → shard {resp['ShardId']} seq {resp['SequenceNumber']}")

print("\nDone. Check Lambda logs in CloudWatch: /aws/lambda/order-processor")