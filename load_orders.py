"""
load_orders.py
--------------
Sends a configurable batch of orders into Kinesis.

Usage:
    python load_orders.py                    # 20 random orders
    python load_orders.py --count 100        # 100 random orders
    python load_orders.py --count 500 --tps 10   # 500 orders at 10/sec

Requirements:
    pip install boto3 faker
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

import boto3
from faker import Faker

STREAM_NAME = "order-events"
REGION      = "eu-west-1"

client = boto3.client("kinesis", region_name=REGION)
fake   = Faker()

SKUS = [
    {"sku": "SHOE-001", "name": "Running Shoes",   "price": 79.99},
    {"sku": "SHOE-002", "name": "Casual Trainers",  "price": 59.99},
    {"sku": "JACKET-001","name": "Windbreaker",     "price": 89.99},
    {"sku": "JACKET-002","name": "Down Jacket",     "price": 149.99},
    {"sku": "SOCK-001",  "name": "Sports Socks",    "price": 9.99},
    {"sku": "HAT-001",   "name": "Baseball Cap",    "price": 19.99},
    {"sku": "BAG-001",   "name": "Tote Bag",        "price": 24.99},
    {"sku": "BELT-001",  "name": "Leather Belt",    "price": 34.99},
    {"sku": "SCARF-001", "name": "Wool Scarf",      "price": 29.99},
    {"sku": "GLOVE-001", "name": "Winter Gloves",   "price": 22.99},
]

STATUSES = ["placed", "placed", "placed", "confirmed", "shipped"]


def generate_order() -> dict:
    items = random.sample(SKUS, k=random.randint(1, 4))
    order_items = [
        {"sku": i["sku"], "name": i["name"],
         "qty": random.randint(1, 3), "price": i["price"]}
        for i in items
    ]
    total = round(sum(i["price"] * i["qty"] for i in order_items), 2)

    return {
        "order_id": f"ord-{uuid.uuid4().hex[:8]}",
        "user_id":  f"usr-{random.randint(1, 200):03d}",
        "total":    total,
        "status":   random.choice(STATUSES),
        "items":    order_items,
        "placed_at": datetime.now(timezone.utc).isoformat(),
    }


def send_batch(orders: list[dict]) -> int:
    """Send up to 500 records in one PutRecords call (Kinesis limit)."""
    records = [
        {
            "Data":         json.dumps(o).encode(),
            "PartitionKey": o["order_id"],
        }
        for o in orders
    ]
    resp   = client.put_records(StreamName=STREAM_NAME, Records=records)
    failed = resp.get("FailedRecordCount", 0)
    return len(records) - failed


def main():
    parser = argparse.ArgumentParser(description="Load test orders into Kinesis")
    parser.add_argument("--count", type=int, default=20,
                        help="Number of orders to send (default: 20)")
    parser.add_argument("--tps",   type=float, default=0,
                        help="Throttle to N orders/sec (default: no throttle)")
    args = parser.parse_args()

    print(f"Sending {args.count} orders to stream '{STREAM_NAME}'...")
    sent  = 0
    batch = []

    for i in range(args.count):
        batch.append(generate_order())

        # Flush every 500 (Kinesis PutRecords limit)
        if len(batch) == 500 or i == args.count - 1:
            sent += send_batch(batch)
            print(f"  [{sent}/{args.count}] sent", end="\r")
            batch = []

        if args.tps > 0:
            time.sleep(1 / args.tps)

    print(f"\nDone — {sent} orders delivered to Kinesis.")
    print("Watch logs: aws logs tail /aws/lambda/order-processor --follow --region eu-west-1")


if __name__ == "__main__":
    main()