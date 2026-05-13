"""
query_handler.py
----------------
Lambda function that queries Aurora and returns orders as JSON.
Exposed via a Lambda Function URL — Kong proxies requests to this URL.

Supported routes (passed as query string parameters):
    GET /           → all orders (limit 50)
    GET /?order_id=ord-001          → single order by ID
    GET /?user_id=usr-42            → all orders for a user
    GET /?status=placed             → all orders by status
    GET /?limit=10&offset=0        → paginated results
"""

import base64
import json
import logging
import os
import boto3
import psycopg2
from psycopg2.extras import RealDictCursor
from decimal import Decimal
from datetime import datetime, date

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secrets_client = boto3.client("secretsmanager")

DB_PROXY_ENDPOINT = os.environ["DB_PROXY_ENDPOINT"]
DB_SECRET_ARN     = os.environ["DB_SECRET_ARN"]
DB_NAME           = os.environ.get("DB_NAME", "orders")

_db_conn = None


# ------------------------------------------------------------------ #
# Helpers                                                             #
# ------------------------------------------------------------------ #
def get_db_credentials() -> dict:
    secret = secrets_client.get_secret_value(SecretId=DB_SECRET_ARN)
    return json.loads(secret["SecretString"])


def get_connection():
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
        logger.info("New DB connection established")
    return _db_conn


def json_serialiser(obj):
    """Handle types that json.dumps can't serialise by default."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serialisable")


def response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "X-Powered-By":  "order-pipeline",
        },
        "body": json.dumps(body, default=json_serialiser),
    }


# ------------------------------------------------------------------ #
# Query builders                                                      #
# ------------------------------------------------------------------ #
def get_all_orders(conn, limit: int, offset: int) -> list:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT order_id, user_id, total, status, items, created_at, updated_at
            FROM   orders
            ORDER  BY created_at DESC
            LIMIT  %s OFFSET %s
            """,
            (limit, offset),
        )
        return [dict(row) for row in cur.fetchall()]


def get_order_by_id(conn, order_id: str) -> dict | None:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM orders WHERE order_id = %s",
            (order_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_orders_by_user(conn, user_id: str, limit: int, offset: int) -> list:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT order_id, user_id, total, status, items, created_at, updated_at
            FROM   orders
            WHERE  user_id = %s
            ORDER  BY created_at DESC
            LIMIT  %s OFFSET %s
            """,
            (user_id, limit, offset),
        )
        return [dict(row) for row in cur.fetchall()]


def get_orders_by_status(conn, status: str, limit: int, offset: int) -> list:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT order_id, user_id, total, status, items, created_at, updated_at
            FROM   orders
            WHERE  status = %s
            ORDER  BY created_at DESC
            LIMIT  %s OFFSET %s
            """,
            (status, limit, offset),
        )
        return [dict(row) for row in cur.fetchall()]


def get_summary(conn) -> dict:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT
                COUNT(*)       AS total_orders,
                SUM(total)     AS total_revenue,
                AVG(total)     AS avg_order_value,
                MAX(created_at) AS latest_order
            FROM orders
        """)
        return dict(cur.fetchone())


# ------------------------------------------------------------------ #
# Handler                                                             #
# ------------------------------------------------------------------ #
def lambda_handler(event, context):
    logger.info("Event: %s", json.dumps(event))

    # Lambda Function URL puts query params here
    params = event.get("queryStringParameters") or {}

    limit  = min(int(params.get("limit",  50)), 100)  # cap at 100
    offset = int(params.get("offset", 0))

    try:
        conn = get_connection()

        # Route by query parameter
        if "order_id" in params:
            order = get_order_by_id(conn, params["order_id"])
            if not order:
                return response(404, {"error": f"Order {params['order_id']} not found"})
            return response(200, {"order": order})

        elif "user_id" in params:
            orders = get_orders_by_user(conn, params["user_id"], limit, offset)
            return response(200, {
                "user_id": params["user_id"],
                "count":   len(orders),
                "orders":  orders,
            })

        elif "status" in params:
            orders = get_orders_by_status(conn, params["status"], limit, offset)
            return response(200, {
                "status": params["status"],
                "count":  len(orders),
                "orders": orders,
            })

        elif params.get("summary") == "true":
            summary = get_summary(conn)
            return response(200, {"summary": summary})

        else:
            orders = get_all_orders(conn, limit, offset)
            return response(200, {
                "count":  len(orders),
                "limit":  limit,
                "offset": offset,
                "orders": orders,
            })

    except psycopg2.Error as exc:
        logger.error("Database error: %s", exc)
        return response(500, {"error": "Database error", "detail": str(exc)})

    except Exception as exc:
        logger.error("Unexpected error: %s", exc)
        return response(500, {"error": "Internal server error"})