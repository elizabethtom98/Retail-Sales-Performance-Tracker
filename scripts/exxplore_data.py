import pandas as pd

# Load the dataset by creating data frame
df = pd.read_csv("../data/Superstore.csv" , encoding="ISO-8859-1")   # adjust path if needed
print("Rows × Columns:", df.shape)
print("\nFirst five rows:\n", df.head())
sales_by_region= df.groupby("Region")[["Sales","Profit"]].sum()
print (sales_by_region)
region_sales = df.groupby("Region")[["Sales", "Profit"]].sum().reset_index()
region_sales.to_csv("../data/region_summary.csv", index=False)
