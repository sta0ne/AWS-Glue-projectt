import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame
import gs_derived

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
AmazonS3_node1773057591425 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://cdcprojectt/source/"], "recurse": True}, transformation_ctx="AmazonS3_node1773057591425")

# Script generated for node Change Schema
ChangeSchema_node1773058127856 = ApplyMapping.apply(frame=AmazonS3_node1773057591425, mappings=[("orderid", "string", "orderid", "bigint"), ("customer", "string", "customer", "string"), ("item", "string", "item", "string"), ("quantity", "string", "quantity", "int"), ("price", "string", "price", "decimal"), ("orderdate", "string", "orderdate", "date")], transformation_ctx="ChangeSchema_node1773058127856")

# Script generated for node Derived Column
DerivedColumn_node1773060029402 = ChangeSchema_node1773058127856.gs_derived(colName="line_item_revenue", expr="quantity*price")

# Script generated for node SQL Query
SqlQuery255 = '''
select * from revenue
where price >= 1000
'''
SQLQuery_node1773060913490 = sparkSqlQuery(glueContext, query = SqlQuery255, mapping = {"cdc":ChangeSchema_node1773058127856, "revenue":DerivedColumn_node1773060029402}, transformation_ctx = "SQLQuery_node1773060913490")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1773060913490, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1773056825787", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1773061681193 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1773060913490, connection_type="s3", format="glueparquet", connection_options={"path": "s3://cdcprojectt/target/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1773061681193")

job.commit()