Multinational-Retail-Data-Centralisation
======

The project, set by Ai Core as part of the data science career accelerator, focuses on building skills in data extraction, cleaning, and analysis using Python and PostgreSQL. The objective is to create a centralized system for storing and analyzing sales data from multiple sources. The project involves the following steps:

Data Extraction and Cleaning:
------
- Identify the various data sources where the sales data is stored.
- Extract the data from these sources using appropriate methods (e.g., APIs, web scraping, CSV/Excel files, etc.).
- Perform necessary data cleaning operations, such as removing duplicates, handling missing values, standardizing formats, etc., to ensure data quality.

Uploading Data to a PostgreSQL Database:
------
- Install PostgreSQL and set up a centralized PostgreSQL database.
- Design the database schema using the star schema, which typically consists of a central fact table and dimension tables.
- Create tables in the database to match the schema design.
- Transform and load the cleaned data into the appropriate tables using SQL or Python libraries like psycopg2.

Querying the Database for Data-driven Insights:
------
- Utilize SQL queries in PostgreSQL to extract relevant information and metrics from the database.
- Analyze the data to derive insights into sales performance, such as top-selling products, regional sales trends, profitability, and other key metrics.
- Generate reports or visualizations to present the findings in a clear and actionable manner.

The repository comprises several files:
======

1. database_utils.py: 
- Contains the DataConnector class responsible for handling database-related operations.
- read_db_creds: Reads the database credentials from a YAML file and returns them as a dictionary.
- init_db_engine: Initializes a connection to the AWS RDS database using the retrieved credentials and returns the database engine.
- list_db_tables: Retrieves the table names from the AWS RDS database using the initialized engine and prints them.
- upload_to_db: Uploads cleaned data to a local PostgreSQL database using the database credentials from the YAML file.

2. data_extraction.py:
- Defines the DataExtractor class, which handles data extraction tasks.
- extract_rds_table: Connects to the AWS RDS database using DataConnector, retrieves a table, and saves it as a pandas dataframe.
- retrieve_pdf_data: Converts PDF data to a CSV file using the tabula module (commented out), then reads it into a pandas dataframe.
- list_number_of_stores: Retrieves the number of stores covered by fetching data through an API (451 stores).
- retrieve_stores_data: Retrieves store data through an API (commented out) and saves it as a CSV file, then reads it into a pandas dataframe.
- extract_from_s3: Extracts data from an AWS S3 bucket in CSV format and returns it as a pandas dataframe.
- extract_json_data: Extracts data from JSON files and returns it as a pandas dataframe.

3. data_cleaning.py:
- Contains the DataClean class responsible for cleaning the data.
- Provides methods to clean specific dataframes (users, cards, stores, products, orders, dates), handling tasks such as handling null values, data format verification, converting data types, removing redundant columns, removing rows with NaN values, removing duplicates, and resetting the index.
- Additionally, there is a method to clean weight data specifically in the products database.

4. data_main.py:
- Executes the code from the other files.
- Instantiates the necessary classes (DataExtractor, DataClean, DataConnector) to perform data extraction, cleaning, and database connectivity tasks.

By following this project workflow, the organization can consolidate its sales data into a centralized PostgreSQL database, allowing for easier access, analysis, and reporting. The star schema design facilitates efficient querying and analysis of the sales data. The use of PostgreSQL enables powerful SQL capabilities to extract insights and support data-driven decision-making within the company.

This project provides valuable hands-on experience in data extraction, cleaning, database management, and SQL querying, all of which are essential skills for a data science career.
