import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

S3_BUCKET = "fintech-transaction-lake-al"

df = spark.read.option("header", True)\
    .csv(f"s3://{S3_BUCKET}/raw/transactions/transactions.csv")

print("Row count: " + str(df.count()))
print("Columns: " + str(df.columns))

df.write.mode("overwrite")\
    .csv(f"s3://{S3_BUCKET}/processed/test_output/")

print("Test output written successfully!")
job.commit()