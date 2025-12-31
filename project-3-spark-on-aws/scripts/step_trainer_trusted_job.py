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
AmazonS3_node1767173886833 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://shyam-hande/step_trainer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1767173886833")

# Script generated for node Amazon S3
AmazonS3_node1767173887794 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://shyam-hande/customer/curated/3/"], "recurse": True}, transformation_ctx="AmazonS3_node1767173887794")

# Script generated for node SQL Query
SqlQuery2396 = '''
SELECT 
    s.*
FROM step_trainer_landing AS s
JOIN customers_curated_3 AS c
    ON s.serialnumber = c.serialnumber;
'''
SQLQuery_node1767173890822 = sparkSqlQuery(glueContext, query = SqlQuery2396, mapping = {"customers_curated_3":AmazonS3_node1767173887794, "step_trainer_landing":AmazonS3_node1767173886833}, transformation_ctx = "SQLQuery_node1767173890822")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1767173890822, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1767173876520", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1767173893005 = glueContext.getSink(path="s3://shyam-hande/step_trainer/trusted/3/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1767173893005")
AmazonS3_node1767173893005.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="step_trainer_trusted_3")
AmazonS3_node1767173893005.setFormat("json")
AmazonS3_node1767173893005.writeFrame(SQLQuery_node1767173890822)
job.commit()
