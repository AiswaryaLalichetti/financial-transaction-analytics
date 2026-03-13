import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

S3_BUCKET = "fintech-transaction-lake-al"
INPUT_PATH = f"s3://{S3_BUCKET}/raw"
OUTPUT_PATH = f"s3://{S3_BUCKET}/processed"

# Load Raw Data
transactions_df = spark.read.option("header", True)\
    .option("inferSchema", True)\
    .csv(f"{INPUT_PATH}/transactions/transactions.csv")

customers_df = spark.read.option("header", True)\
    .option("inferSchema", True)\
    .csv(f"{INPUT_PATH}/customers/customers.csv")

merchants_df = spark.read.option("header", True)\
    .option("inferSchema", True)\
    .csv(f"{INPUT_PATH}/merchants/merchants.csv")

# Clean Transactions
transactions_clean = transactions_df\
    .dropDuplicates(["transaction_id"])\
    .filter(F.col("amount") > 0)\
    .filter(F.col("status").isin(["Completed", "Pending", "Failed"]))\
    .withColumn("transaction_date",
        F.to_timestamp("transaction_date", "yyyy-MM-dd HH:mm:ss"))\
    .withColumn("year", F.year("transaction_date"))\
    .withColumn("month", F.month("transaction_date"))\
    .withColumn("day", F.dayofmonth("transaction_date"))\
    .withColumn("hour", F.hour("transaction_date"))\
    .withColumn("is_weekend",
        F.when(F.dayofweek("transaction_date").isin([1, 7]), 1).otherwise(0))\
    .withColumn("amount_category",
        F.when(F.col("amount") < 100, "Low")
         .when(F.col("amount") < 1000, "Medium")
         .when(F.col("amount") < 3000, "High")
         .otherwise("Very High"))

# Enrich with Customer and Merchant Data
enriched_df = transactions_clean\
    .join(customers_df.select(
        "customer_id", "name", "account_type", "credit_score", "state"),
        on="customer_id", how="left")\
    .join(merchants_df.select(
        "merchant_id", "merchant_name", "category", "city"),
        on="merchant_id", how="left")

# Add Fraud Risk Score
enriched_df = enriched_df\
    .withColumn("fraud_risk_score",
        F.when((F.col("is_fraud") == 1) & (F.col("amount") > 3000), 0.95)
         .when(F.col("is_fraud") == 1, 0.75)
         .when((F.col("amount") > 3000) & (F.col("is_weekend") == 1), 0.45)
         .when(F.col("amount") > 3000, 0.30)
         .otherwise(0.05))

# Save Enriched Transactions as Parquet
enriched_df.write\
    .mode("overwrite")\
    .partitionBy("year", "month")\
    .parquet(f"{OUTPUT_PATH}/transactions_enriched/")

# Save Customer Summary
customer_summary = enriched_df\
    .groupBy("customer_id", "name", "account_type", "state")\
    .agg(
        F.count("transaction_id").alias("total_transactions"),
        F.round(F.sum("amount"), 2).alias("total_spend"),
        F.round(F.avg("amount"), 2).alias("avg_transaction_amount"),
        F.sum("is_fraud").alias("fraud_count"),
        F.max("transaction_date").alias("last_transaction_date")
    )\
    .withColumn("fraud_rate",
        F.round(F.col("fraud_count") / F.col("total_transactions") * 100, 2))

customer_summary.write\
    .mode("overwrite")\
    .parquet(f"{OUTPUT_PATH}/customer_summary/")

# Save Merchant Summary
merchant_summary = enriched_df\
    .groupBy("merchant_id", "merchant_name", "category", "city")\
    .agg(
        F.count("transaction_id").alias("total_transactions"),
        F.round(F.sum("amount"), 2).alias("total_revenue"),
        F.round(F.avg("amount"), 2).alias("avg_transaction_amount"),
        F.sum("is_fraud").alias("fraud_count")
    )

merchant_summary.write\
    .mode("overwrite")\
    .parquet(f"{OUTPUT_PATH}/merchant_summary/")

print("All processed data saved to S3 in Parquet format!")
job.commit()