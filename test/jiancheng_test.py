from datetime import datetime, timedelta
import os
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def get_db_connection():
    password = urllib.parse.quote_plus(os.environ['DB_PASSWORD'])
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    return create_engine(DATABASE_URL)


# Configuration: set the date range here
start_date = datetime.strptime("2025-06-05", "%Y-%m-%d").date()
end_date = datetime.strptime("2025-06-10", "%Y-%m-%d").date()
experiment_name = "app_bpr_recall_exp"

# Connect to the data warehouse
engine = get_db_connection()

# Collect results
results = []

current_date = start_date
while current_date <= end_date:
    date_str = current_date.strftime('%Y-%m-%d')
    sql = f"""
   
    """
    # Execute and fetch into DataFrame
    df = pd.read_sql(sql, engine)

    # Print result for the day
    print(f"\n📅 Date: {date_str}")
    print(df)

    results.append(df)
    current_date += timedelta(days=1)

# Save all results
final_df = pd.concat(results, ignore_index=True)
final_df.to_csv(f"{experiment_name}.csv", index=False)
print(f"\n✅ All results saved to {experiment_name}.csv")
