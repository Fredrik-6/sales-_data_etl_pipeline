import pandas as pd
import os

# List files in the current directory
print("Files in the current directory:")
print(os.listdir())  # This will help you confirm what files are available

# Specify the full paths to the CSV files
bakery_sales_path = r"C:\Users\presetup\Documents\Personal Projects\ETL test\Kaggel Bakary Data\archive\bakery sales.csv"
bakery_price_path = r"C:\Users\presetup\Documents\Personal Projects\ETL test\Kaggel Bakary Data\archive\bakery price.csv"

# Read the CSV files
try:
    bakery_sales = pd.read_csv(bakery_sales_path)  # Read bakery sales data
    bakery_price = pd.read_csv(bakery_price_path)  # Read bakery price data
    
    # Display the first few rows of each dataset
    print("\nBakery Sales Data:")
    print(bakery_sales.head())
    
    print("\nBakery Price Data:")
    print(bakery_price.head())

except FileNotFoundError as e:
    print(f"Error: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
