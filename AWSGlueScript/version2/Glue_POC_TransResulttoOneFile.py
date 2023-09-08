import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1692357483646 = glueContext.create_dynamic_frame.from_catalog(
    database="default",
    table_name="glue_poc_table_transactiondetailsinput",
    transformation_ctx="AWSGlueDataCatalog_node1692357483646",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1692357475500 = glueContext.create_dynamic_frame.from_catalog(
    database="default",
    table_name="glue_poc_table_transactionheaderinput",
    transformation_ctx="AWSGlueDataCatalog_node1692357475500",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1692357562186 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1692357475500,
    mappings=[
        ("planid", "long", "right_planid", "long"),
        ("participantid", "long", "right_participantid", "long"),
        ("transactionid", "long", "right_transactionid", "long"),
        ("transactiontype", "long", "right_transactiontype", "long"),
        ("postdate", "string", "right_postdate", "string"),
        ("tradedate", "string", "right_tradedate", "string"),
        ("transactiontypenum", "long", "right_transactiontypenum", "long"),
        ("numdetails", "long", "right_numdetails", "long"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1692357562186",
)

# Script generated for node Join
Join_node1692357534082 = Join.apply(
    frame1=AWSGlueDataCatalog_node1692357483646,
    frame2=RenamedkeysforJoin_node1692357562186,
    keys1=["transactionid"],
    keys2=["right_transactionid"],
    transformation_ctx="Join_node1692357534082",
)

# Script generated for node Amazon S3
result_one = Join_node1692357534082.toDF().repartition(1)
result_one.write.csv('s3://uat1-gwf-cc-storagegateway-us-east-1/test/Glue_POC/Glue_POC_Output2/')
#new line in version 2

job.commit()
