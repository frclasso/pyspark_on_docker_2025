
import os
from faker import Faker
import random
from datetime import datetime
import pandas as pd

fake = Faker()

# Generate fake data ["Customer", "Category", "Amount"]
def generate_fake_record(pid):
    customer = fake.name()
    category = random.choice(["Electronics", "Clothing", "Home & Garden", "Sports", "Books"])
    amount = round(random.uniform(10, 1000), 2)
    return (pid, customer, category, amount)

# Number of fake records
num_records = 100000

# Create the data
data = [generate_fake_record(pid) for pid in range(1, num_records + 1)]

# Create DataFrame
records_df = pd.DataFrame(data=data , columns=["PID","Customer", "Category", "Amount"])

# Show sample data
print(records_df.head())

# Ensure the directory exists
output_dir = "/datasets/customer_by_category"
os.makedirs(output_dir, exist_ok=True)

# Save the DataFrame to CSV
records_df.to_csv(f"{output_dir}/customer_by_category.csv", index=False)

# listing
print(os.listdir(output_dir))