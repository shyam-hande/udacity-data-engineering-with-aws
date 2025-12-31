STEDI Human Balance Analytics
=============================

This project implements a Data Lakehouse solution on AWS to process and sanitize data from the STEDI Step Trainer IoT device and mobile app. 
Using AWS Glue, S3, Python, and Spark, I built a multi-stage ETL pipeline that filters data based on customer research consent and ensures high data quality for downstream Machine Learning applications.

Tech Stack
==========
Storage: Amazon S3

ETL Engine: AWS Glue (Spark-based Python scripts)

Query Engine: Amazon Athena

Data Format: Semi-structured JSON

Records per zone
===============

1. Landing Zone (Raw Ingestion)
Raw data is ingested from three primary sources and stored in S3. I defined the following Glue tables to explore the semi-structured JSON data via Athena:

customer_landing: total records are 956

accelerometer_landing: total records are 81273

step_trainer_landing: total records are 28680

2. Trusted Zone (Privacy & Consent Sanitization)
The goal of this stage is to respect customer privacy. Data is filtered to include only records from users who agreed to share data for research.

customer_trusted: total records are 482
accelerometer_trusted: total records are 40981
step_trainer_trusted_3: total records are 14460

3. Curated Zone (Quality & Business Logic)
This stage solves a critical data quality issue where duplicate serial numbers were assigned to customers.

customer_curated_3: total records are 482

machine_learning_curated_3: total records are 43681

Repository Contents
===================
/scripts: Python/Spark ETL scripts for Glue Jobs.

/sql: DDL scripts for Landing Zone table creation.

/screenshots: Athena query results for each stage (Landing/Trusted/Curated).
