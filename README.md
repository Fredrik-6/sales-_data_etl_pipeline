# sales-_data_etl_pipeline

ETL Project: Korean Bakery Sales Data

Project Overview
This project involves an Extract, Transform, Load (ETL) process applied to sales data from a small Korean bakery, sourced from Kaggle. The goal is to clean, transform, and load the data into a MySQL database for further analysis.

Files Used
bakery sales.csv: Contains the sales records of various bakery items.
bakery price.csv: Contains the price details of the bakery items.

ETL Process
Extract:
Read the CSV files containing sales and price data using Pandas.

Transform:
Fill missing values with zeros.
Remove duplicate entries.
Melt the sales data to have a long-format DataFrame.
Merge sales data with price data.
Convert data types for price and quantity sold.
Remove entries with zero quantity sold or total sales.
Validate and clean datetime entries.
Extract date from the datetime column.

Load:
Establish a connection to a MySQL database.
Delete existing records in the target table to ensure fresh data.
Load the cleaned data into the MySQL database.

Requirements:
Python 3.x
Pandas
SQLAlchemy
MySQL Connector

How to Run:
Ensure you have the required packages installed.
Update the database connection parameters in the script.
Run the Python script to perform the ETL process.

Conclusion:
The ETL process successfully loads the cleaned sales data into a MySQL database, allowing for further analysis and insights into the bakery's sales performance.
