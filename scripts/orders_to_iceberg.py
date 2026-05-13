"""
orders_to_iceberg.py
--------------------
AWS Glue 4.0 PySpark job.

Reads the orders table from Aurora Postgres via JDBC and writes it
as an Iceberg table in S3, registered in the Glue Data Catalog.

Job parameters (passed via Glue job arguments):
    --DB_SECRET_ARN   : Secrets Manager ARN for Aurora credentials
    --DATABASE_NAME   : Glue catalog database name (e.g. orders_catalog)
    --TABLE_NAME      : Iceberg table name (e.g. orders)
    --S3_OUTPUT       : S3 path for Iceberg warehouse (e.g. s3://bucket/iceberg/)

Run modes:
    - Full load  : first run, writes all rows
    - Incremental: subsequent runs, merges new/updated rows using order_id as key
"""

import sys
import json
import boto3
from datetime import datetime, timezone

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DecimalType, TimestampType
)


# ------------------------------------------------------------------ #
# 1. Bootstrap — Glue context, Spark session, job args               #
# ------------------------------------------------------------------ #
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "DB_SECRET_ARN",
    "DATABASE_NAME",
    "TABLE_NAME",
    "S3_OUTPUT",
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Convenience vars
DATABASE_NAME = args["DATABASE_NAME"]   # orders_catalog
TABLE_NAME    = args["TABLE_NAME"]      # orders
S3_OUTPUT     = args["S3_OUTPUT"]       # s3://order-data-lake-.../iceberg/
FULL_TABLE    = f"glue_catalog.{DATABASE_NAME}.{TABLE_NAME}"

print(f"[INFO] Job started at {datetime.now(timezone.utc).isoformat()}")
print(f"[INFO] Target table : {FULL_TABLE}")
print(f"[INFO] S3 warehouse : {S3_OUTPUT}")


# ------------------------------------------------------------------ #
# 2. Fetch Aurora credentials from Secrets Manager                   #
# ------------------------------------------------------------------ #
def get_jdbc_url_and_props(secret_arn: str) -> tuple[str, dict]:
    """Return (jdbc_url, connection_properties) from a Secrets Manager secret."""
    client = boto3.client("secretsmanager")
    secret = json.loads(
        client.get_secret_value(SecretId=secret_arn)["SecretString"]
    )

    host     = secret["host"]
    port     = secret.get("port", 5432)
    dbname   = secret.get("dbname", "orders")
    username = secret["username"]
    password = secret["password"]

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{dbname}"
    props    = {
        "user":     username,
        "password": password,
        "driver":   "org.postgresql.Driver",
    }
    print(f"[INFO] JDBC target: {host}:{port}/{dbname}")
    return jdbc_url, props


jdbc_url, jdbc_props = get_jdbc_url_and_props(args["DB_SECRET_ARN"])


# ------------------------------------------------------------------ #
# 3. Read from Aurora                                                 #
# ------------------------------------------------------------------ #
print("[INFO] Reading orders from Aurora...")

orders_df = (
    spark.read
    .format("jdbc")
    .option("url",      jdbc_url)
    .option("dbtable",  "orders")
    .option("user",     jdbc_props["user"])
    .option("password", jdbc_props["password"])
    .option("driver",   jdbc_props["driver"])
    # Push down a predicate so we only fetch rows updated in last 48h
    # on incremental runs. Remove for a full reload.
    .option("fetchsize", "1000")
    .load()
)

row_count = orders_df.count()
print(f"[INFO] Rows read from Aurora: {row_count}")

if row_count == 0:
    print("[INFO] No rows to process. Exiting.")
    job.commit()
    sys.exit(0)


# ------------------------------------------------------------------ #
# 4. Light transformations                                            #
# ------------------------------------------------------------------ #
# Normalise column types and add a partition column (order_date)
orders_transformed = (
    orders_df
    .withColumn("total",      F.col("total").cast(DecimalType(12, 2)))
    .withColumn("created_at", F.col("created_at").cast(TimestampType()))
    .withColumn("updated_at", F.col("updated_at").cast(TimestampType()))
    # Partition key — Iceberg will organise files by this
    .withColumn("order_date", F.to_date(F.col("created_at")))
    # items is stored as a string in Postgres JSONB; keep as string in Iceberg
    .withColumn("items",      F.col("items").cast(StringType()))
)

print("[INFO] Schema after transformation:")
orders_transformed.printSchema()


# ------------------------------------------------------------------ #
# 5. Create Iceberg table if it does not exist                       #
# ------------------------------------------------------------------ #
spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{DATABASE_NAME}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {FULL_TABLE} (
        order_id    STRING,
        user_id     STRING,
        total       DECIMAL(12,2),
        status      STRING,
        items       STRING,
        created_at  TIMESTAMP,
        updated_at  TIMESTAMP,
        order_date  DATE
    )
    USING iceberg
    PARTITIONED BY (order_date)
    LOCATION '{S3_OUTPUT}{TABLE_NAME}/'
    TBLPROPERTIES (
        'table_type'                        = 'ICEBERG',
        'format'                            = 'parquet',
        'write.parquet.compression-codec'   = 'snappy',
        'write.metadata.delete-after-commit.enabled' = 'true',
        'write.metadata.previous-versions-max'       = '10'
    )
""")
print(f"[INFO] Iceberg table {FULL_TABLE} verified / created")


# ------------------------------------------------------------------ #
# 6. Merge (upsert) — idempotent, safe to re-run                    #
# ------------------------------------------------------------------ #
# Register the incoming batch as a temp view so we can use it in SQL
orders_transformed.createOrReplaceTempView("incoming_orders")

print("[INFO] Running MERGE INTO Iceberg table...")

spark.sql(f"""
    MERGE INTO {FULL_TABLE} AS target
    USING incoming_orders            AS source
    ON target.order_id = source.order_id

    WHEN MATCHED AND source.updated_at > target.updated_at THEN
        UPDATE SET
            target.user_id    = source.user_id,
            target.total      = source.total,
            target.status     = source.status,
            target.items      = source.items,
            target.updated_at = source.updated_at,
            target.order_date = source.order_date

    WHEN NOT MATCHED THEN
        INSERT (order_id, user_id, total, status, items,
                created_at, updated_at, order_date)
        VALUES (source.order_id, source.user_id, source.total,
                source.status, source.items, source.created_at,
                source.updated_at, source.order_date)
""")

print("[INFO] MERGE complete")


# ------------------------------------------------------------------ #
# 7. Verify — print row counts and latest records                    #
# ------------------------------------------------------------------ #
result = spark.sql(f"SELECT COUNT(*) as total_rows FROM {FULL_TABLE}")
result.show()

latest = spark.sql(f"""
    SELECT order_date, status, COUNT(*) as orders, SUM(total) as revenue
    FROM   {FULL_TABLE}
    GROUP  BY order_date, status
    ORDER  BY order_date DESC
""")
print("[INFO] Daily summary in Iceberg:")
latest.show()


# ------------------------------------------------------------------ #
# 8. Compact small files (optional but good practice)                #
# ------------------------------------------------------------------ #
print("[INFO] Running file compaction...")
spark.sql(f"""
    CALL glue_catalog.system.rewrite_data_files(
        table => '{DATABASE_NAME}.{TABLE_NAME}',
        strategy => 'sort',
        sort_order => 'order_date ASC NULLS LAST'
    )
""")
print("[INFO] Compaction complete")


# ------------------------------------------------------------------ #
# 9. Commit                                                           #
# ------------------------------------------------------------------ #
print(f"[INFO] Job finished at {datetime.now(timezone.utc).isoformat()}")
job.commit()