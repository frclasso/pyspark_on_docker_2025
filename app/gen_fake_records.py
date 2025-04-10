
from faker import Faker
import random
from datetime import datetime
import pandas as pd

fake = Faker()

# Generate fake data
def generate_fake_record(pid):
    name = fake.name()
    did = random.randint(1, 20)
    dname = f"Dept-{did}"
    visit_date = fake.date_between(start_date='-1y', end_date='today')
    return (pid, name, did, dname, visit_date)

# Number of fake records
num_records = 100000

# Create the data
data = [generate_fake_record(pid) for pid in range(1, num_records + 1)]

# Create DataFrame
records_df = pd.DataFrame(data=data , columns=["PID", "Name", "DID", "DName", "VisitDate"])

# Show sample data
print(records_df.head())

# Optional: Save to CSV
records_df.to_csv("/datasets/fake_patient_visit_data/fake_patient_visit_data.csv", index=False)

