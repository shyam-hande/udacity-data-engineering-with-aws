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
AmazonS3_node1767168455452 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="accelerometer_landing", transformation_ctx="AmazonS3_node1767168455452")

# Script generated for node Amazon S3
AmazonS3_node1767168458996 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="customer_trusted", transformation_ctx="AmazonS3_node1767168458996")

# Script generated for node SQL Query
SqlQuery2105 = '''
select * from accelerometer_landing a 
join customer_trusted c on a.email = c.user

'''
SQLQuery_node1767168461099 = sparkSqlQuery(glueContext, query = SqlQuery2105, mapping = {"accelerometer_landing":AmazonS3_node1767168458996, "customer_trusted":AmazonS3_node1767168455452}, transformation_ctx = "SQLQuery_node1767168461099")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1767168461099, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1767167119147", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1767168463394 = glueContext.getSink(path="s3://shyam-hande/accelerometer_trusted_zone/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1767168463394")
AmazonS3_node1767168463394.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="accelerometer_trusted")
AmazonS3_node1767168463394.setFormat("json")
AmazonS3_node1767168463394.writeFrame(SQLQuery_node1767168461099)
job.commit()