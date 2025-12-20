# Project 2: Data Warehousing with AWS (Sparkify ETL)

This repository contains the ETL pipeline for the "Sparkify" music streaming startup course project. The pipeline loads JSON song and log data from S3 into staging tables on Amazon Redshift, then transforms and inserts the data into a star-schema analytics schema.

## Contents / Repository structure

- `create_tables.py` - Connects to Redshift and creates/drops tables defined in `sql_queries.py`.
- `etl.py` - Loads data from S3 into staging tables and then inserts data into the final analytics tables.
- `sql_queries.py` - All SQL statements:
  - CREATE / DROP statements
  - COPY statements for staging tables
  - INSERT statements for final tables
  - Lists for orchestrating actions in `create_tables.py` and `etl.py`
- `dwh.cfg` - Configuration file (credentials, cluster, IAM role ARN, S3 paths). **Do not commit secrets** to public repos.
- `etl_tests.ipynb` - Jupyter notebook used to validate the ETL and inspect data on Redshift.

## Project overview

Goal: Build an ETL pipeline that:
1. Copies raw JSON event and song data from S3 into Redshift staging tables.
2. Transforms and inserts the data into a set of star-schema tables:
   - Fact: `songplays`
   - Dimension: `users`, `songs`, `artists`, `time`
3. Validate by running queries / the provided notebook.

## Prerequisites

- An AWS account with:
  - A Redshift cluster (publicly accessible or accessible from where you run scripts)
  - An IAM role with the necessary S3 read permissions attached to the Redshift cluster
- The S3 dataset used by the Udacity project
- Python 3.7+
- Python packages:
  - psycopg2 (or psycopg2-binary)
  - boto3
  - configparser (built-in for Python 3)
  - jupyter
  - ipython-sql

Install python deps (example):
```bash
pip install psycopg2-binary boto3 jupyter ipython-sql
```

## Configuration

1. fill in your values in `dwh.cfg`

Example `dwh.cfg` (sensitive values shown as placeholders):
```ini
[CLUSTER]
HOST=redshift-cluster-1.xxx.us-west-2.redshift.amazonaws.com
DB_NAME=dev
DB_USER=your_redshift_user
DB_PASSWORD=your_password
DB_PORT=5439

[IAM_ROLE]
ARN=arn:aws:iam::123456789012:role/YourRedshiftRole

[S3]
LOG_DATA=s3://your-bucket/log_data
LOG_JSONPATH=s3://your-bucket/log_json_path.json
SONG_DATA=s3://your-bucket/song_data

[AWS]
KEY=YOUR_AWS_ACCESS_KEY_ID
SECRET=YOUR_AWS_SECRET_ACCESS_KEY
```

- `IAM_ROLE:ARN` must be the role attached to your Redshift cluster with S3 read access.
- `S3` keys should point to the JSON data and optionally the JSONPaths file for log_data.
- The scripts read `dwh.cfg` from the current working directory.

## How to run

1. Ensure `dwh.cfg` is filled and your local environment can reach the Redshift cluster.

2. Create (or recreate) the tables:
```bash
python3 create_tables.py
```
This script will:
- Read `dwh.cfg`
- Connect to Redshift
- Drop existing tables (if any)
- Create staging and final tables

3. Run the ETL:
```bash
python3 etl.py
```
This script will:
- COPY data from S3 into `staging_events` and `staging_songs`
- Insert and transform data into final tables (`songplays`, `users`, `songs`, `artists`, `time`)

4. Validate / inspect results:
- Launch `etl_tests.ipynb`:
```bash
jupyter notebook etl_tests.ipynb
```
- The notebook connects to the Redshift cluster using values from `dwh.cfg`, lists tables and sample rows, and runs basic validation queries.

## Notes on SQL / Design

- Staging tables:
  - `staging_events` stores raw log data
  - `staging_songs` stores raw song metadata
- Fact table:
  - `songplays` — uses an IDENTITY column for `songplay_id` and stores `start_time`, `user_id`, `song_id`, `artist_id`, etc.
- Dimensions:
  - `users`, `songs`, `artists`, `time`
- COPY commands are defined in `sql_queries.py`. They use the `IAM_ROLE` ARN and S3 paths from `dwh.cfg`.

## Idempotency & behavior

- `create_tables.py` uses `CREATE TABLE IF NOT EXISTS`.
- INSERT queries use `SELECT DISTINCT` to reduce duplicates, but depending on how often you run them and data characteristics, additional deduplication or UPSERT logic may be required.

## Common errors & troubleshooting

- Connection issues:
  - Make sure Redshift accepts connections from your IP (VPC configuration / security groups).
  - Ensure host, port, database, user, password are correct.
- COPY failures:
  - Ensure the IAM role ARN in `dwh.cfg` is attached to the Redshift cluster and has access to the S3 bucket.
  - If using access keys instead of IAM role, configure COPY to use `ACCESS_KEY_ID`/`SECRET_ACCESS_KEY` — but IAM role is recommended.
  - Check S3 path and JSONPaths file path (for event logs) are correct and accessible.
- Missing Python dependencies:
  - Install `psycopg2-binary`, `boto3`, etc.
- Data type / casting errors:
  - The provided SQL expects certain fields/types in the Udacity dataset. If using a custom dataset adapt the schema/queries.

## Security

- Do not commit `dwh.cfg` with real credentials to a public repository.
- Prefer using IAM roles attached to the Redshift cluster instead of embedding AWS keys.

## Clean up
- Delete redshift and other resources if not required to avoid additional billing
