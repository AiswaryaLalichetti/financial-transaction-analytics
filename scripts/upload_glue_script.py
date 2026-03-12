import boto3

BUCKET_NAME = "fintech-transaction-lake-al"  # your bucket name

s3 = boto3.client("s3")

# Delete old wrong file first
s3.delete_object(Bucket=BUCKET_NAME, Key="scripts/transform_transactions.py/fintech-transform-transactions.py")
print("🗑️ Deleted old wrong file")

# Upload correctly
s3.upload_file(
    "glue_jobs/transform_transactions.py",
    BUCKET_NAME,
    "scripts/transform_transactions.py"
)
print("✅ Glue script uploaded correctly to S3!")