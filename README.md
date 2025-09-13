
# Data Ingestion & Analytics on AWS (CDK Python)

A production‑ready reference that ingests data from public APIs with AWS Lambda, stores results in Amazon S3 as Parquet (Snappy), catalogs datasets with AWS Glue, governs access with Lake Formation, and enables analytics with Amazon Athena. It includes scheduled triggers, a Secrets Manager proxy, and opinionated defaults.

Note: This repository contains a base stack (`aws_test`) and, in a related project, the primary data ingestion stack (`api_consumer`). This README documents the full solution and how to deploy it.

## Architecture
- Lambda functions (Python 3.11):
  - `HttpConsumerRandomUserFunction`: consumes `randomuser.me`, writes to S3 under `randomuser/` as Parquet (Snappy).
  - `HttpConsumerJSONPlaceholderFunction`: consumes `jsonplaceholder.typicode.com`, writes to S3 under `jsonplaceholder/` as Parquet (Snappy).
  - Parquet conversion via `pyarrow`/`pandas` using the AWS SDK for pandas (awswrangler) layer.
- Secrets Manager: optional `PROXY_URL` secret to route outbound traffic via proxy.
- Amazon S3:
  - `ApiConsumerResultsBucket`: curated data (Parquet).
  - `ApiConsumerAthenaResultsBucket`: Athena query results.
- Amazon EventBridge: scheduled rules to invoke Lambdas periodically.
- AWS Glue:
  - Database `api_consumer_db`.
  - Crawler discovers schema and creates/updates tables with prefix `api_consumer_`.
- AWS Lake Formation: data location, database, table, and column‑level permissions for Glue and Athena.
- Amazon Athena: WorkGroup `ApiConsumerWG` writes results to S3 and includes a sample query.

## Prerequisites
- Python 3.11 and pip
- AWS CDK v2 (CLI) with configured AWS credentials
- CDK bootstrap in your account/region (`cdk bootstrap`)

## Quick Start
1) Create and activate a virtualenv
- `python3 -m venv .venv && source .venv/bin/activate`
2) Install dependencies
- `pip install -r requirements.txt`
3) Deploy
- `cdk deploy -c config=dev --all`

If you work with the `api_consumer` project in a separate directory, `cd` into that folder and run `cdk deploy` there to provision the full ingestion and analytics stack.

## Configuration (Layers & Secrets)
- Lambdas write Parquet (Snappy) to S3 and use the AWS SDK for pandas layer. The stack attaches the layer automatically for the target region. If AWS releases a newer version, update the layer ARN in the stack.
- Optional proxy: if you create a `PROXY_URL` secret in Secrets Manager (either plain string or JSON with `PROXY_URL`/`proxy_url`), the proxy‑enabled function will use it automatically.

## Glue & Lake Formation
- Glue Database: `api_consumer_db`.
- Glue Crawler: targets `s3://<ApiConsumerResultsBucket>/randomuser/` and `.../jsonplaceholder/`, scheduled daily at 01:00 UTC.
- Lake Formation grants:
  - Crawler role: Data Location access + CREATE_TABLE/ALTER/DROP/DESCRIBE on the database.
  - Athena role: DESCRIBE on the database and SELECT/DESCRIBE on tables, with example column‑level restrictions for sensitive fields.

## Athena
- WorkGroup: `ApiConsumerWG` with results written to `s3://<ApiConsumerAthenaResultsBucket>/results/` (SSE‑S3).
- After the crawler runs, select database `api_consumer_db` and query your tables in Athena.

## Operations & Testing
- Invoke Lambdas (console or CLI) for ad‑hoc runs.
- Parquet files are partitioned by date: `yyyy/mm/dd/HHMMSS-<uuid>.parquet`.
- Run the Glue Crawler manually if you need to refresh the schema immediately.
- Query via Athena using the `ApiConsumerWG` workgroup.

## Troubleshooting
- Missing PyArrow: ensure the awswrangler layer is attached; the stack adds it automatically per region.
- Mixed types (ArrowInvalid): the code normalizes and casts to string to avoid schema conflicts; if you need strict typing, define a schema and cast accordingly.
- HTTP 403 from endpoints: add a realistic User‑Agent/headers or use the `PROXY_URL` secret.

## Useful CDK Commands
- `cdk ls` — list all stacks in the app
- `cdk synth` — emit the synthesized CloudFormation template
- `cdk deploy` — deploy to your default AWS account/region
- `cdk diff` — compare deployed stack with current state

## Security
- Secrets are read from Secrets Manager and never logged.
- Lake Formation governs access at database, table, and column level.
- Adjust IAM policies and column exclusions to meet your compliance needs.

—
Suggestions or improvements are welcome. Open an issue or contact the project owner.
