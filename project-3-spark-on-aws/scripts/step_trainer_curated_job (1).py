import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1767174450318 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="accelerometer_trusted", transformation_ctx="AmazonS3_node1767174450318")

# Script generated for node Amazon S3
AmazonS3_node1767174449093 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="step_trainer_trusted_3", transformation_ctx="AmazonS3_node1767174449093")

# Script generated for node SQL Query
SqlQuery2114 = '''
SELECT *
FROM step_trainer_trusted_3 AS s
INNER JOIN accelerometer_trusted AS a
ON a.sensorreadingtime = s.timestamp;

'''
SQLQuery_node1767174452544 = sparkSqlQuery(glueContext, query = SqlQuery2114, mapping = {"step_trainer_trusted_3":AmazonS3_node1767174450318, "accelerometer_trusted":AmazonS3_node1767174449093}, transformation_ctx = "SQLQuery_node1767174452544")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1767174452544, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1767173876520", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1767174454644 = glueContext.getSink(path="s3://shyam-hande/step_trainer/machine_learning/curated/3/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1767174454644")
AmazonS3_node1767174454644.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="machine_learning_curated_3")
AmazonS3_node1767174454644.setFormat("json")
AmazonS3_node1767174454644.writeFrame(SQLQuery_node1767174452544)
job.commit()