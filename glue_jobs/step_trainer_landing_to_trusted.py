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

# Script generated for node step_trainer_landing
step_trainer_landing_node1794778795827 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1794778795827")

# Script generated for node customers_curated
customers_curated_node1794778883101 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customers_curated", transformation_ctx="customers_curated_node1794778883101")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT DISTINCT s.*
FROM step_trainer_landing s
JOIN customers_curated c ON s.serialNumber = c.serialNumber

'''
SQLQuery_node1794778929169 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customers_curated":customers_curated_node1794778883101, "step_trainer_landing":step_trainer_landing_node1794778795827}, transformation_ctx = "SQLQuery_node1794778929169")

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1794778929169, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1794778766254", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1794779046659 = glueContext.getSink(path="s3://stedi-lakehouse-865423/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1794779046659")
step_trainer_trusted_node1794779046659.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1794779046659.setFormat("json")
step_trainer_trusted_node1794779046659.writeFrame(SQLQuery_node1794778929169)
job.commit()
