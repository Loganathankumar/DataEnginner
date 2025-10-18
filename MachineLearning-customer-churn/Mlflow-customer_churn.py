import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np

data_types = {
    'customerID': 'string',
    'gender': 'string',
    'SeniorCitizen': 'string',  # load as string first
    'Partner': 'string',
    'Dependents': 'string',
    'tenure': 'string',
    'phoneService': 'string',
    'MultipleLines': 'string',
    'internetService': 'string',
    'OnlineSecurity': 'string',
    'OnlineBackup': 'string',
    'DeviceProtection': 'string',
    'TechSupport': 'string',
    'StreamingTV': 'string',
    'StreamingMovies': 'string',
    'Contract': 'string',
    'PaperlessBilling': 'string',
    'PaymentMethod': 'string',
    'MonthlyCharges': 'string',
    'TotalCharges': 'string',
    'Churn': 'string'
}

# Step 1: Read all as string
telco_df = pd.read_csv(
    r"telco-customer-churn-missing.csv",
    dtype=data_types,
    sep=","
)

# Step 2: Strip whitespace & replace blanks with NaN
telco_df = telco_df.replace(r'^\s*$', np.nan, regex=True)

# Step 3: Convert numeric fields safely
selected_columns = ['tenure', 'TotalCharges', 'MonthlyCharges']
for col in selected_columns:
    telco_df[col] = pd.to_numeric(telco_df[col], errors='coerce')

# Step 4: Compute correlation
telco_corr = telco_df[selected_columns].corr()
# print(telco_corr)

"""
1. Correlation Heatmap
We will import the seaborn library, to create a correlation heatmap to visually represent the correlation matrix among numerical features. 
This provides insights into the strength and direction of relationships between variables.
"""


# Create a heatmap using seaborn
plt.figure(figsize=(10, 6))
sns.heatmap(telco_corr, annot=True, cmap='coolwarm', linewidths=.5)
plt.title('Correlation Heatmap for Telco Dataset')
plt.show()

"""
2. Pairplot for Numerical Variables:
Generate a pairplot to visualize relationships between numerical variables. This provides a quick overview of how 
features interact and whether certain patterns emerge based on the 'Churn' status.
"""

# Select the specified columns from the DataFrame
telco_pnv = telco_df[selected_columns + ['Churn']]

# Pairplot for a quick overview of relationships between numerical variables
sns.pairplot(telco_pnv, hue='Churn', diag_kind='kde')
plt.suptitle('Pairplot for Telco Dataset', y=1.02)
plt.show()














