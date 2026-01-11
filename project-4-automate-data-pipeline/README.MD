Project 4 - Data Pipelines with Airflow
Project Introduction
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

Datasets
Log data: s3://udacity-dend/log_data
Song data: s3://udacity-dend/song_data
Tools used
Python, SQL
Apache Airflow 3 (code changed as per ned of this version)
Amazon Web Services (AWS IAM, S3, Redshift Serverless, CLI)
Airflow Data pipeline
Copy S3 Data
Create a project S3 bucket using the AWS Cloudshell

aws s3 mb s3://shyam-automate-data-pipelines/
Copy the data from the udacity bucket to the home cloudshell directory:

aws s3 cp s3://udacity-dend/log-data/ ~/log-data/ --recursive
aws s3 cp s3://udacity-dend/song-data/ ~/song-data/ --recursive
Copy the data from the udacity bucket to the home cloudshell directory:

aws s3 cp ~/log-data/ s3://shyam-automate-data-pipelines/log-data/ --recursive
aws s3 cp ~/song-data/ s3://shyam-automate-data-pipelines/song-data/ --recursive
List the data in bucket to be sure it copied over:

aws s3 ls s3://shyam-automate-data-pipelines/log-data/
aws s3 ls s3://shyam-automate-data-pipelines/song-data/
