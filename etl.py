import pandas as pd
import os

# List files in the current directory
print("Files in the current directory:")
print(os.listdir())  # Confirm what files are available

# Specify the full paths to the CSV files
bakery_sales_path = r"C:\Users\presetup\Documents\Personal Projects\ETL test\Kaggel Bakary Data\archive\bakery sales.csv"
bakery_price_path = r"C:\Users\presetup\Documents\Personal Projects\ETL test\Kaggel Bakary Data\archive\bakery price.csv"

# Read the CSV files
try:
    bakery_sales = pd.read_csv(bakery_sales_path)  # Read bakery sales data
    bakery_price = pd.read_csv(bakery_price_path)  # Read bakery price data
except FileNotFoundError as e:
    print(f"Error: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

# Fill missing values with 0
bakery_sales.fillna(0, inplace=True)
bakery_price.fillna(0, inplace=True)

# Remove duplicates
bakery_sales.drop_duplicates(inplace=True)
bakery_price.drop_duplicates(inplace=True)

# Melt bakery_sales
item_columns = [
    'angbutter', 'plain bread', 'jam', 'americano', 'croissant', 
    'caffe latte', 'tiramisu croissant', 'cacao deep', 
    'pain au chocolat', 'almond croissant', 'croque monsieur', 
    'mad garlic', 'milk tea', 'gateau chocolat', 'pandoro', 
    'cheese cake', 'lemon ade', 'orange pound', 'wiener', 
    'vanila latte', 'berry ade', 'tiramisu', 'merinque cookies'
]

sales_long = bakery_sales.melt(id_vars=['datetime', 'day of week', 'total', 'place'], 
                                value_vars=item_columns, 
                                var_name='item_name', 
                                value_name='quantity_sold')

# Rename the column in bakery_price
bakery_price.rename(columns={'Name': 'item_name'}, inplace=True)

# Merge sales data with price data
merged_data = pd.merge(sales_long, bakery_price, on='item_name', how='left')

# Convert price and quantity_sold to numeric, forcing errors to NaN
merged_data['price'] = pd.to_numeric(merged_data['price'], errors='coerce')
merged_data['quantity_sold'] = pd.to_numeric(merged_data['quantity_sold'], errors='coerce')

# Drop rows where price or quantity_sold is NaN
merged_data.dropna(subset=['price', 'quantity_sold'], inplace=True)

# Calculate total sales for each item
merged_data['total_sales'] = merged_data['quantity_sold'] * merged_data['price']

# Check for invalid entries in 'datetime' column
# Replace non-date values with NaT
invalid_datetime_entries = merged_data[~merged_data['datetime'].str.match(r'^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}$', na=False)]
print("Invalid datetime entries:\n", invalid_datetime_entries)

# Coerce invalid datetime entries to NaT
merged_data['datetime'] = pd.to_datetime(merged_data['datetime'], dayfirst=True, errors='coerce')

# Drop rows where 'datetime' is NaT
merged_data.dropna(subset=['datetime'], inplace=True)

# Extract only the date from the datetime column
merged_data['date'] = merged_data['datetime'].dt.date

# Select the relevant columns for analysis
final_data = merged_data[['item_name', 'price', 'quantity_sold', 'total_sales', 'date']]

# Rename columns for clarity
final_data.rename(columns={'price': 'price_per_item'}, inplace=True)

# Check the final DataFrame
print(final_data.head())
#print(final_data[9000:9050])

# Get the number of rows and columns
# num_rows, num_columns = final_data.shape
# print(f"Number of rows: {num_rows}")
