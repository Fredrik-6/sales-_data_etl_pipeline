import pandas as pd
import os

def load_data(sales_path, price_path):
    """Load sales and price data from CSV files."""
    try:
        bakery_sales = pd.read_csv(sales_path)
        bakery_price = pd.read_csv(price_path)
        return bakery_sales, bakery_price
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return None, None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None, None

def test_data_integrity(df, expected_shape):
    """Test for data integrity."""
    assert df is not None, "DataFrame is None."
    assert df.shape == expected_shape, f"Expected shape {expected_shape}, but got {df.shape}."
    assert df.isnull().sum().sum() == 0, "Data contains missing values."
    assert all(df.dtypes != 'object'), "All columns should be numeric."

def main():
    # File paths
    bakery_sales_path = r"C:\Users\presetup\Documents\Personal Projects\ETL test\Kaggel Bakary Data\archive\bakery sales.csv"
    bakery_price_path = r"C:\Users\presetup\Documents\Personal Projects\ETL test\Kaggel Bakary Data\archive\bakery price.csv"
    
    # Load data
    bakery_sales, bakery_price = load_data(bakery_sales_path, bakery_price_path)
    
    # Display the first few rows of each dataset
    if bakery_sales is not None and bakery_price is not None:
        print("\nBakery Sales Data:")
        print(bakery_sales.head())
        
        print("\nBakery Price Data:")
        print(bakery_price.head())
        
        # Test data integrity
        test_data_integrity(bakery_sales, (100, 5))  # Update with expected shape
        test_data_integrity(bakery_price, (50, 2))   # Update with expected shape

if __name__ == "__main__":
    main()
