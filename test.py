import pandas as pd
import os

# bakery_sales = pd.read_csv('Bakery Sales.csv')  
# bakery_price = pd.read_csv('Bakery Price.csv') 
# print(bakery_sales.head())
# print(bakery_price.head())

bakery_sales_path = r"C:\Users\presetup\Documents\Personal Projects\ETL test\Kaggel Bakary Data\archive\bakery sales.csv"
bakery_price_path = r"C:\Users\presetup\Documents\Personal Projects\ETL test\Kaggel Bakary Data\archive\bakery price.csv"

try:
    bakery_sales = pd.read_csv(bakery_sales_path)
    bakery_price = pd.read_csv(bakery_price_path)
    
    print("\nBakery Sales Data:")
    print(bakery_sales.head())
    
    print("\nBakery Price Data:")
    print(bakery_price.head())

except FileNotFoundError as e:
    print(f"Error: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

