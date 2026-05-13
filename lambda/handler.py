import base64
import json
import logging
import os
import boto3
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ------------------------------------------------------------------ #
# Clients — instantiated outside the handler so they're reused       #
# across warm invocations                                             #
# ------------------------------------------------------------------ #
secrets_client = boto3.client("secretsmanager")
sns_client = boto3.client("sns")
sqs_client = boto3.client("sqs")

# Environment variables injected by CDK
DB_PROXY_ENDPOINT = os.environ["DB_PROXY_ENDPOINT"]
DB_SECRET_ARN     = os.environ["DB_SECRET_ARN"]
SNS_TOPIC_ARN     = os.environ["SNS_TOPIC_ARN"]
DLQ_URL           = os.environ["DLQ_URL"]
DB_NAME           = os.environ.get("DB_NAME", "orders")

# Module-level connection — reused on warm starts
_db_conn = None


# ------------------------------------------------------------------ #
# DB helpers                                                          #
# ------------------------------------------------------------------ #
def get_db_credentials() -> dict:
    """Fetch username/password from Secrets Manager."""
    secret = secrets_client.get_secret_value(SecretId=DB_SECRET_ARN)
    return json.loads(secret["SecretString"])


def get_connection():
    """Return a live psycopg2 connection, creating one if needed."""
    global _db_conn
    if _db_conn is None or _db_conn.closed:
        creds = get_db_credentials()
        _db_conn = psycopg2.connect(
            host=DB_PROXY_ENDPOINT,
            port=5432,
            dbname=DB_NAME,
            user=creds["username"],
            password=creds["password"],
            sslmode="require",
            connect_timeout=5,
        )
        logger.info("New DB connection established via RDS Proxy")
    return _db_conn


def ensure_table_exists(conn) -> None:
    """
    Idempotent table creation — safe to call on every cold start.
    In production this would be handled by a migration tool (Flyway / Alembic).
    """
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id    VARCHAR(64)    PRIMARY KEY,
                user_id     VARCHAR(64)    NOT NULL,
                total       NUMERIC(12, 2) NOT NULL,
                status      VARCHAR(32)    NOT NULL DEFAULT 'placed',
                items       JSONB,
                created_at  TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
                updated_at  TIMESTAMPTZ    NOT NULL DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_orders_user_id
                ON orders (user_id);

            CREATE INDEX IF NOT EXISTS idx_orders_status
                ON orders (status);
        """)
        conn.commit()
        logger.info("orders table verified / created")


# ------------------------------------------------------------------ #
# Business logic                                                      #
# ------------------------------------------------------------------ #
def validate_order(order: dict) -> None:
    """Raise ValueError if required fields are missing or invalid."""
    required = {"order_id", "user_id", "total"}
    missing = required - order.keys()
    if missing:
        raise ValueError(f"Order is missing required fields: {missing}")
    if float(order["total"]) <= 0:
        raise ValueError(f"Order total must be positive, got: {order['total']}")


def upsert_order(conn, order: dict) -> None:
    """Insert the order, or update it if the order_id already exists."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO orders (order_id, user_id, total, status, items)
            VALUES (%(order_id)s, %(user_id)s, %(total)s, %(status)s, %(items)s)
            ON CONFLICT (order_id) DO UPDATE
                SET status     = EXCLUDED.status,
                    total      = EXCLUDED.total,
                    items      = EXCLUDED.items,
                    updated_at = NOW();
            """,
            {
                "order_id": order["order_id"],
                "user_id":  order["user_id"],
                "total":    float(order["total"]),
                "status":   order.get("status", "placed"),
                "items":    json.dumps(order.get("items", [])),
            },
        )
        conn.commit()
        logger.info("Upserted order %s for user %s", order["order_id"], order["user_id"])


def publish_confirmation(order: dict) -> None:
    """Publish an order confirmation event to SNS."""
    message = {
        "event":    "ORDER_CONFIRMED",
        "order_id": order["order_id"],
        "user_id":  order["user_id"],
        "total":    order["total"],
        "status":   order.get("status", "placed"),
    }
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"Order confirmed: {order['order_id']}",
        Message=json.dumps(message),
        MessageAttributes={
            "event_type": {
                "DataType":    "String",
                "StringValue": "ORDER_CONFIRMED",
            }
        },
    )
    logger.info("SNS confirmation published for order %s", order["order_id"])


def send_to_dlq(raw_record: dict, error: Exception) -> None:
    """Forward an unparseable or invalid record to the DLQ with error context."""
    sqs_client.send_message(
        QueueUrl=DLQ_URL,
        MessageBody=json.dumps({
            "error":       str(error),
            "raw_record":  raw_record,
        }),
    )
    logger.warning("Record sent to DLQ: %s", str(error))


# ------------------------------------------------------------------ #
# Handler                                                             #
# ------------------------------------------------------------------ #
def lambda_handler(event, context):
    """
    Triggered by Kinesis Data Streams.

    Each Kinesis record payload is base64-encoded JSON with the shape:
        {
            "order_id": "ord-001",
            "user_id":  "usr-42",
            "total":    99.99,
            "status":   "placed",          # optional, defaults to "placed"
            "items":    [{"sku": "A", "qty": 2}]  # optional
        }
    """
    records = event.get("Records", [])
    logger.info("Received batch of %d Kinesis record(s)", len(records))

    succeeded = 0
    failed    = 0

    # Acquire (or reuse) DB connection and ensure schema exists
    try:
        conn = get_connection()
        ensure_table_exists(conn)
    except Exception as exc:
        logger.error("Failed to connect to database: %s", exc)
        # Re-raise so Lambda retries the entire batch
        raise

    for record in records:
        raw = record  # keep for DLQ forwarding
        try:
            # Kinesis records are base64-encoded
            payload_bytes = base64.b64decode(record["kinesis"]["data"])
            order = json.loads(payload_bytes)
            logger.info("Processing order: %s", order.get("order_id", "unknown"))

            validate_order(order)
            upsert_order(conn, order)
            publish_confirmation(order)

            succeeded += 1

        except (ValueError, KeyError, json.JSONDecodeError) as exc:
            # Bad data — send to DLQ, do not retry
            logger.error("Invalid record skipped: %s", exc)
            send_to_dlq(raw, exc)
            failed += 1

        except psycopg2.Error as exc:
            # DB error — rollback and re-raise so Kinesis retries the batch
            logger.error("Database error on order %s: %s", order.get("order_id"), exc)
            try:
                conn.rollback()
            except Exception:
                pass
            raise

    logger.info("Batch complete — succeeded: %d, failed to DLQ: %d", succeeded, failed)
    return {"succeeded": succeeded, "failed": failed}