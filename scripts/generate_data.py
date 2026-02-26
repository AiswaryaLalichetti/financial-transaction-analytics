import pandas as pd
from faker import Faker
import random
import uuid
from datetime import datetime, timedelta
import os

fake = Faker()
random.seed(42)

NUM_CUSTOMERS = 500
NUM_TRANSACTIONS = 50000
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 12, 31)

# --- Generate Customers ---
customers = []
for _ in range(NUM_CUSTOMERS):
    customers.append({
        "customer_id": str(uuid.uuid4()),
        "name": fake.name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "address": fake.address().replace("\n", ", "),
        "account_type": random.choice(["Checking", "Savings", "Business"]),
        "credit_score": random.randint(580, 850),
        "state": fake.state_abbr(),
        "join_date": fake.date_between(start_date="-5y", end_date="-1y")
    })

customers_df = pd.DataFrame(customers)

# --- Generate Merchants ---
merchants = []
categories = ["Groceries", "Electronics", "Travel", "Dining", "Healthcare",
              "Utilities", "Retail", "Entertainment", "Fuel", "ATM Withdrawal"]
for _ in range(100):
    merchants.append({
        "merchant_id": str(uuid.uuid4()),
        "merchant_name": fake.company(),
        "category": random.choice(categories),
        "city": fake.city(),
        "state": fake.state_abbr(),
        "country": "US"
    })

merchants_df = pd.DataFrame(merchants)

# --- Generate Transactions ---
transactions = []
customer_ids = customers_df["customer_id"].tolist()
merchant_ids = merchants_df["merchant_id"].tolist()

for _ in range(NUM_TRANSACTIONS):
    txn_date = START_DATE + timedelta(
        seconds=random.randint(0, int((END_DATE - START_DATE).total_seconds()))
    )
    amount = round(random.uniform(5, 5000), 2)
    is_fraud = 1 if (amount > 3000 and random.random() < 0.4) else random.choices([0, 1], weights=[97, 3])[0]

    transactions.append({
        "transaction_id": str(uuid.uuid4()),
        "customer_id": random.choice(customer_ids),
        "merchant_id": random.choice(merchant_ids),
        "transaction_date": txn_date.strftime("%Y-%m-%d %H:%M:%S"),
        "amount": amount,
        "currency": "USD",
        "transaction_type": random.choice(["Purchase", "Refund", "Transfer", "Withdrawal"]),
        "channel": random.choice(["Online", "In-Store", "Mobile", "ATM"]),
        "status": random.choice(["Completed", "Pending", "Failed"]),
        "is_fraud": is_fraud,
        "fraud_reason": fake.sentence() if is_fraud else None
    })

transactions_df = pd.DataFrame(transactions)

# --- Save locally ---
os.makedirs("data/raw", exist_ok=True)
customers_df.to_csv("data/raw/customers.csv", index=False)
merchants_df.to_csv("data/raw/merchants.csv", index=False)
transactions_df.to_csv("data/raw/transactions.csv", index=False)

print(f"✅ Generated {NUM_CUSTOMERS} customers, 100 merchants, {NUM_TRANSACTIONS} transactions")
print("📁 Saved to data/raw/")