# Order Events Pipeline

A fully serverless, event-driven data pipeline built on AWS, demonstrating real-world use of Kinesis, Lambda, Aurora Serverless, Glue, Iceberg, Kong, and CDK — deployed and managed via GitHub Actions CI/CD.

---

## Architecture overview

```
Producer (test_event.py)
        │
        ▼
Kinesis Data Streams (order-events)
        │
        ▼
Lambda (order-processor)
  ├── validates & upserts → Aurora Serverless v2 (Postgres 15)
  ├── publishes confirmation → SNS (order-confirmations)
  └── on failure → SQS Dead-Letter Queue
        │
        ▼
EventBridge (daily at 02:00 UTC)
        │
        ▼
Lambda (glue-job-trigger)
        │
        ▼
Glue Job (orders-to-iceberg)
  └── reads Aurora → writes Iceberg tables → S3 data lake
        │
        ▼
Athena (ad-hoc SQL queries on Iceberg)

Client → Kong Gateway (Docker) → Lambda Function URL (order-query) → Aurora
```

---

## Tech stack

| Layer      | Technology                        | Purpose                                      |
|------------|-----------------------------------|----------------------------------------------|
| Ingest     | Amazon Kinesis Data Streams       | Real-time event ingestion                    |
| Compute    | AWS Lambda (Python 3.12)          | Event processing, API queries                |
| Storage    | Aurora Serverless v2 (Postgres)   | Transactional order store                    |
| Messaging  | Amazon SNS                        | Order confirmation notifications             |
| Queuing    | Amazon SQS + DLQ                  | Async work queue and failure handling        |
| Eventing   | Amazon EventBridge                | Scheduled pipeline orchestration             |
| ETL        | AWS Glue 4.0 (PySpark)           | Batch export from Aurora to data lake        |
| Table fmt  | Apache Iceberg                    | Open table format with time travel           |
| Query      | Amazon Athena                     | Ad-hoc SQL on the data lake                  |
| API GW     | Kong (Docker)                     | API key auth and rate limiting               |
| IaC        | AWS CDK (Python)                  | All infrastructure defined as code          |
| CI/CD      | GitHub Actions + OIDC             | Automated diff on PR, deploy on main         |

---

## Project structure

```
project_training/
├── .github/
│   └── workflows/
│       └── deploy.yml              # CI/CD pipeline
├── lambda/                         # Phase 2 — order processor
│   ├── handler.py
│   └── requirements.txt
├── lambda_query/                   # Phase 4 — orders query API
│   ├── query_handler.py
│   └── requirements.txt
├── scripts/                        # Phase 3 — Glue PySpark job
│   └── orders_to_iceberg.py
├── schema/                         # Database schema
│   └── init.sql
├── kong/                           # Phase 4 — Kong local setup
│   ├── docker-compose.yml
│   └── kong_setup.sh
├── test_event.py                   # Send test orders to Kinesis
├── app.py                          # CDK entry point
├── cdk.json                        # CDK configuration
├── requirements.txt                # CDK Python dependencies
└── project_training/
    └── project_training_stack.py   # Full CDK stack definition
```

---

## Prerequisites

| Tool          | Version   | Install                              |
|---------------|-----------|--------------------------------------|
| Python        | 3.9+      | https://python.org                   |
| Node.js       | 18+       | https://nodejs.org                   |
| AWS CDK CLI   | latest    | `npm install -g aws-cdk`             |
| AWS CLI       | v2        | https://aws.amazon.com/cli           |
| Docker        | latest    | https://docker.com                   |
| Git           | latest    | https://git-scm.com                  |

AWS account requirements:
- IAM user (not root) with AdministratorAccess
- Account must be fully activated (billing console shows data)
- Bootstrapped for CDK: `cdk bootstrap aws://ACCOUNT_ID/eu-west-1`

---

## Quick start

### 1. Clone and set up the environment

```bash
git clone https://github.com/YOUR_USERNAME/project-training.git
cd project-training

python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure AWS credentials

```bash
aws configure
# Enter your IAM user Access Key ID and Secret Access Key
# Region: eu-west-1
```

Verify:
```bash
aws sts get-caller-identity
```

### 3. Bootstrap CDK (once per account/region)

```bash
cdk bootstrap aws://YOUR_ACCOUNT_ID/eu-west-1
```

### 4. Deploy the stack

```bash
cdk deploy --context account=YOUR_ACCOUNT_ID
```

After deploy, note the outputs — you will need them in later steps:
- `KinesisStreamName`
- `AuroraSecretArn`
- `RdsProxyEndpoint`
- `ConfirmationTopicArn`
- `QueryFunctionUrl`

---

## Phase-by-phase guide

### Phase 1 — Infrastructure

Everything is provisioned by `cdk deploy`. Review what was created:

```bash
aws cloudformation describe-stack-resources \
  --stack-name ProjectTrainingStack \
  --region eu-west-1 \
  --query "StackResources[*].{Type:ResourceType,Name:LogicalResourceId}"
```

### Phase 2 — Lambda order processor

**Initialise the database schema** via the RDS Query Editor in the AWS console:
- Go to https://eu-west-1.console.aws.amazon.com/rds → Query Editor
- Connect using the `AuroraSecretArn` output value
- Paste and run `schema/init.sql`

**Subscribe to order confirmation emails:**
```bash
aws sns subscribe \
  --topic-arn YOUR_CONFIRMATION_TOPIC_ARN \
  --protocol email \
  --notification-endpoint your@email.com \
  --region eu-west-1
```
Confirm the subscription link sent to your inbox.

**Send test orders:**
```bash
source .venv/bin/activate
pip install boto3
python test_event.py
```

**Watch Lambda logs:**
```bash
aws logs tail /aws/lambda/order-processor --follow --region eu-west-1
```

**Verify data in Aurora** via RDS Query Editor:
```sql
SELECT order_id, user_id, total, status, created_at
FROM orders
ORDER BY created_at DESC;
```

### Phase 3 — Glue + Iceberg

**Upload the Glue script to S3:**
```bash
aws s3 cp scripts/orders_to_iceberg.py \
  s3://order-data-lake-YOUR_ACCOUNT-eu-west-1/scripts/orders_to_iceberg.py \
  --region eu-west-1
```

**Run the Glue job:**
```bash
aws glue start-job-run --job-name orders-to-iceberg --region eu-west-1
```

**Monitor until SUCCEEDED:**
```bash
aws glue get-job-run \
  --job-name orders-to-iceberg \
  --run-id YOUR_RUN_ID \
  --region eu-west-1 \
  --query "JobRun.JobRunState"
```

**Query in Athena** — go to https://eu-west-1.console.aws.amazon.com/athena:
```sql
-- All orders
SELECT * FROM orders ORDER BY created_at DESC;

-- Daily revenue
SELECT order_date, status, COUNT(*) as orders, SUM(total) as revenue
FROM orders GROUP BY order_date, status ORDER BY order_date DESC;

-- Time travel — data as it was 10 minutes ago
SELECT * FROM orders
FOR SYSTEM_TIME AS OF (NOW() - INTERVAL '10' MINUTE);
```

### Phase 4 — Kong API Gateway

**Start Kong:**
```bash
cd kong
docker compose up -d
```

Verify Kong is running:
```bash
curl http://localhost:8001
```

**Configure Kong** — edit `kong/kong_setup.sh` and set `LAMBDA_URL` to your `QueryFunctionUrl` output, then:
```bash
chmod +x kong/kong_setup.sh
./kong/kong_setup.sh
```

**Test the API:**
```bash
# All orders
curl http://localhost:8000/orders -H "x-api-key: training-secret-key-001"

# Filter by user
curl "http://localhost:8000/orders?user_id=usr-42" -H "x-api-key: training-secret-key-001"

# Single order
curl "http://localhost:8000/orders?order_id=ord-001" -H "x-api-key: training-secret-key-001"

# Summary stats
curl "http://localhost:8000/orders?summary=true" -H "x-api-key: training-secret-key-001"

# No API key — should return 401
curl http://localhost:8000/orders
```

### Phase 5 — GitHub Actions CI/CD

**Push to GitHub:**
```bash
git init && git add .
git commit -m "initial commit"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git
git push -u origin main
```

The deploy job runs automatically on push to `main`.

**Test the PR diff workflow:**
```bash
git checkout -b my-test-branch
# make any small change
git add . && git commit -m "test diff"
git push origin my-test-branch
```
Open a pull request on GitHub — the CDK Diff job posts a comment showing exactly what infrastructure would change.

---

## AWS resources created

| Resource                  | Name / ID                              | Monthly cost (approx.) |
|---------------------------|----------------------------------------|------------------------|
| VPC                       | OrderVpc                               | ~$32 (NAT gateway)     |
| Kinesis Stream            | order-events (1 shard)                 | ~$0.015/hr             |
| Aurora Serverless v2      | OrdersDb (0.5–4 ACU)                  | ~$0.12/ACU-hr          |
| RDS Proxy                 | OrdersProxy                            | ~$0.015/hr             |
| Lambda — processor        | order-processor                        | Pay per invocation     |
| Lambda — query            | order-query                            | Pay per invocation     |
| Lambda — glue trigger     | glue-job-trigger                       | Pay per invocation     |
| SNS Topic                 | order-confirmations                    | Pay per message        |
| SQS Queue                 | order-processing                       | Pay per request        |
| SQS DLQ                   | order-events-dlq                       | Pay per request        |
| S3 Bucket                 | order-data-lake-{account}-{region}     | Pay per GB             |
| Glue Job                  | orders-to-iceberg                      | ~$0.44/DPU-hr          |
| EventBridge Rule          | daily-orders-export                    | Free tier              |

> Run `cdk destroy` when not in use to avoid ongoing charges.

---

## Tearing down

```bash
cdk destroy --context account=YOUR_ACCOUNT_ID
```

Also stop Kong locally:
```bash
cd kong && docker compose down -v
```

---

## Common issues

| Error | Cause | Fix |
|-------|-------|-----|
| `SubscriptionRequiredException` on Kinesis | Account not fully activated | Wait 24h after first billing console visit |
| `Internal error occurred` on Kinesis | Transient AWS issue | Retry `cdk deploy` |
| `Cannot find asset` | Lambda folder in wrong location | Move `lambda/` and `lambda_query/` to project root |
| `Roles may not be assumed by root` | Using root credentials | Create IAM user and reconfigure `aws configure` |
| Glue job `Connection refused` | Missing self-referencing SG rule | Add `glue_sg` with `allow_all_traffic` to itself |
| Empty zip on Lambda deploy | Docker bundling issue | Use `cp -au . /asset-output` in bundling command |