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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1794776195405 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1794776195405")

# Script generated for node customer_trusted
customer_trusted_node1794776143668 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1794776143668")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT DISTINCT c.*
FROM customer_trusted c 
JOIN accelerometer_trusted a ON c.email = a.`user`
'''
SQLQuery_node1794776246384 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_trusted":accelerometer_trusted_node1794776195405, "customer_trusted":customer_trusted_node1794776143668}, transformation_ctx = "SQLQuery_node1794776246384")

# Script generated for node customers_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1794776246384, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1794776118441", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customers_curated_node1794776375275 = glueContext.getSink(path="s3://stedi-lakehouse-865423/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customers_curated_node1794776375275")
customers_curated_node1794776375275.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customers_curated")
customers_curated_node1794776375275.setFormat("json")
customers_curated_node1794776375275.writeFrame(SQLQuery_node1794776246384)
job.commit()
