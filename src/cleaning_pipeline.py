import pandas as pd
import numpy as np
import os
from datetime import datetime

# --- CONFIGURATION ---
# We use '..' to go up one level from 'src/' to the project root
RAW_DATA_PATH = os.path.join("..", "data", "raw", "dirty_data.csv")
PROCESSED_DATA_PATH = os.path.join("..", "data", "processed", "clean_data.csv")
LOG_PATH = os.path.join("..", "logs", "audit_log.txt")

def log_step(message):
    """Writes a message to the log file and prints it to console."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Ensure log directory exists
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    
    with open(LOG_PATH, 'a') as f:
        f.write(f"[{timestamp}] {message}\n")
    print(f"[{timestamp}] {message}")

def run_pipeline():
    # 1. Load Data
    if not os.path.exists(RAW_DATA_PATH):
        print(f"ERROR: File not found at {RAW_DATA_PATH}")
        return
    
    df = pd.read_csv(RAW_DATA_PATH)
    initial_rows = len(df)
    log_step(f"PIPELINE STARTED. Loaded {initial_rows} rows.")

    # 2. Remove Duplicates
    duplicates = df.duplicated().sum()
    if duplicates > 0:
        df = df.drop_duplicates()
        log_step(f"Removed {duplicates} duplicate rows.")

    # 3. Standardize Column Names
    df.columns = [x.lower().strip().replace(" ", "_").replace("/", "_") for x in df.columns]
    log_step("Standardized column names to snake_case.")

    # 4. Handle Missing Values (Promotion)
    if 'promotion' in df.columns:
        missing_promos = df['promotion'].isnull().sum()
        if missing_promos > 0:
            df['promotion'] = df['promotion'].fillna("None")
            log_step(f"Imputed {missing_promos} missing 'promotion' values with 'None'.")

    # 5. Data Type Conversion (Date)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        log_step("Converted 'date' column to datetime objects.")

    # 6. Text Cleaning (Product)
    # Removes [' '] brackets and quotes to make it a clean string
    if 'product' in df.columns:
        df['product'] = df['product'].astype(str).str.replace(r"[\[\]']", "", regex=True)
        log_step("Cleaned special characters from 'product' column.")

    # 7. Save Data
    # Ensure processed directory exists
    os.makedirs(os.path.dirname(PROCESSED_DATA_PATH), exist_ok=True)
    
    df.to_csv(PROCESSED_DATA_PATH, index=False)
    final_rows = len(df)
    
    log_step(f"SUCCESS: Saved clean data to {PROCESSED_DATA_PATH}")
    log_step(f"Final Row Count: {final_rows}")
    log_step("-" * 30)

if __name__ == "__main__":
    run_pipeline()