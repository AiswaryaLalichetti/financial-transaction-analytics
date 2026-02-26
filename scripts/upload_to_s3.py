import boto3
import os

# Change 'al' to your own initials to make bucket name unique
BUCKET_NAME = "fintech-transaction-lake-al"

s3 = boto3.client("s3")

# Create S3 bucket
s3.create_bucket(Bucket=BUCKET_NAME)
print(f"✅ Bucket '{BUCKET_NAME}' created")

# Upload files
files = {
    "data/raw/customers.csv": "raw/customers/customers.csv",
    "data/raw/merchants.csv": "raw/merchants/merchants.csv",
    "data/raw/transactions.csv": "raw/transactions/transactions.csv"
}

for local_path, s3_key in files.items():
    s3.upload_file(local_path, BUCKET_NAME, s3_key)
    print(f"📤 Uploaded {local_path} → s3://{BUCKET_NAME}/{s3_key}")

print("✅ All files uploaded to S3 successfully!")