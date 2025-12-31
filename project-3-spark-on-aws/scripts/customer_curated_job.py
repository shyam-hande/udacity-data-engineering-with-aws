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
AmazonS3_node1767172809984 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="accelerometer_trusted", transformation_ctx="AmazonS3_node1767172809984")

# Script generated for node Amazon S3
AmazonS3_node1767172808884 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="customer_trusted", transformation_ctx="AmazonS3_node1767172808884")

# Script generated for node SQL Query
SqlQuery2260 = '''
select * from customer_trusted c 
join accelerometer_trusted a 
on c.user = a.email
where c.shareWithResearchAsOfDate is not null
'''
SQLQuery_node1767172814371 = sparkSqlQuery(glueContext, query = SqlQuery2260, mapping = {"customer_trusted":AmazonS3_node1767172809984, "accelerometer_trusted":AmazonS3_node1767172808884}, transformation_ctx = "SQLQuery_node1767172814371")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1767172814371, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1767172781138", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1767172818571 = glueContext.getSink(path="s3://shyam-hande/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1767172818571")
AmazonS3_node1767172818571.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="customer_curated")
AmazonS3_node1767172818571.setFormat("json")
AmazonS3_node1767172818571.writeFrame(SQLQuery_node1767172814371)
job.commit()