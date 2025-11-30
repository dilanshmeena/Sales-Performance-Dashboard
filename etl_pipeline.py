import pandas as pd
import sqlite3
import numpy as np

# ==========================================
# STEP 1: EXTRACTION (Load Raw Data)
# ==========================================
def load_data(file_path):
    print(f"Loading data from {file_path}...")
    # Try reading with different encodings if standard utf-8 fails
    try:
        df = pd.read_csv(file_path, encoding='windows-1252')
    except:
        df = pd.read_csv(file_path, encoding='utf-8')
    return df

# ==========================================
# STEP 2: TRANSFORMATION (Cleaning via Pandas)
# ==========================================
def clean_data(df):
    print("Starting data cleaning...")
    
    # 1. Remove Duplicates (Improving Data Accuracy)
    initial_count = len(df)
    df = df.drop_duplicates()
    print(f"Removed {initial_count - len(df)} duplicate rows.")

    # 2. Handle Missing Values
    # Filling missing 'Region' with 'Unknown' ensuring no nulls in critical grouping columns
    if 'Region' in df.columns:
        df['Region'] = df['Region'].fillna('Unknown')

    # 3. Standardize Date Format
    # This is crucial for Power BI Time Intelligence functions
    if 'Order Date' in df.columns:
        df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')

    # 4. Feature Engineering (Creating new metrics)
    # Calculating 'Profit Margin' to enable deeper analysis in the Dashboard
    if 'Profit' in df.columns and 'Sales' in df.columns:
        df['Profit Margin'] = np.where(df['Sales'] != 0, df['Profit'] / df['Sales'], 0)
        df['Profit Margin'] = df['Profit Margin'].round(2)

    print("Data cleaning completed.")
    return df

# ==========================================
# STEP 3: LOADING (SQL & CSV Export)
# ==========================================
def export_data(df, db_name="sales_data.db"):
    print("Exporting data...")
    
    # 1. Load into SQL (SQLite)
    # This matches your resume claim of using "SQL" for storage/querying
    conn = sqlite3.connect(db_name)
    df.to_sql('sales_transactions', conn, if_exists='replace', index=False)
    
    # 2. Verify with a SQL Query
    query = """
    SELECT Region, SUM(Sales) as Total_Revenue 
    FROM sales_transactions 
    GROUP BY Region 
    ORDER BY Total_Revenue DESC
    """
    print("\n--- SQL Validation Query Output ---")
    print(pd.read_sql(query, conn))
    
    conn.close()
    
    # 3. Save as CSV for Power BI
    df.to_csv('cleaned_superstore_data.csv', index=False)
    print(f"\nSuccess! Data exported to {db_name} and cleaned_superstore_data.csv")

# ==========================================
# MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    # Create a dummy CSV if you don't have one yet, just to test the script
    # (In reality, point this to your downloaded Kaggle CSV)
    data = {
        'Order ID': ['CA-2023-1', 'CA-2023-2', 'CA-2023-1'], # Duplicate ID to test cleaning
        'Order Date': ['11/08/2023', '11/09/2023', '11/08/2023'],
        'Region': ['South', np.nan, 'South'], # Missing value to test cleaning
        'Sales': [261.96, 731.94, 261.96],
        'Profit': [41.91, 219.58, 41.91]
    }
    
    # Create a dummy raw file for demonstration
    dummy_df = pd.DataFrame(data)
    dummy_df.to_csv('raw_sales_data.csv', index=False)

    # RUN THE PIPELINE
    raw_df = load_data('raw_sales_data.csv')
    cleaned_df = clean_data(raw_df)
    export_data(cleaned_df)