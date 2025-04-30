import pandas as pd
import random
import numpy as np

# Seed for reproducibility
random.seed(42)
np.random.seed(42)

# Define possible payment methods
payment_methods = [
    "Electronic check",
    "Mailed check",
    "Bank transfer (automatic)",
    "Credit card (automatic)"
]

# Generate 1000 rows of data
data = {
    "Tenure": np.random.randint(1, 73, 1000),
    "MonthlyCharges": np.round(np.random.uniform(20.0, 120.0, 1000), 2),
    "PaymentMethod": np.random.choice(payment_methods, 1000),
    "Churn": np.random.choice([0, 1], 1000)
}

# Compute TotalCharges as Tenure * MonthlyCharges with slight noise
data["TotalCharges"] = np.round(data["Tenure"] * data["MonthlyCharges"] * np.random.uniform(0.95, 1.05, 1000), 2)

# Create DataFrame
churn1_records_df = pd.DataFrame(data)

# Save to CSV
# file_path = "datasets/churn/churn1.csv" 
# df.to_csv(file_path, index=False)
