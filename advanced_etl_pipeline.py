from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random
import os

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 14),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Define DAG
dag = DAG(
    "simplified_etl_pipeline",
    default_args=default_args,
    schedule_interval="*/5 * * * *",  # Every 5 minutes
    catchup=False,
)

# Set paths
DATA_DIR = "/home/azureuser/airflow/tmp"
os.makedirs(DATA_DIR, exist_ok=True)  # Ensure directory exists

# Step 1: Generate Random Data
def generate_data():
    df = pd.DataFrame({
        "id": range(1, 101),
        "value": [random.randint(1, 100) for _ in range(100)],
        "timestamp": pd.date_range(start="2025-03-14", periods=100, freq="T"),
    })
    df.to_csv(f"{DATA_DIR}/random_data.csv", index=False)
    print("✅ Data generated!")

generate_data_task = PythonOperator(
    task_id="generate_data",
    python_callable=generate_data,
    dag=dag,
)

# Step 2: Process Data (Parallel)
def process_data(chunk_num):
    df = pd.read_csv(f"{DATA_DIR}/random_data.csv")
    chunk_size = len(df) // 2
    chunk = df.iloc[chunk_num * chunk_size: (chunk_num + 1) * chunk_size]
    chunk.to_csv(f"{DATA_DIR}/processed_chunk_{chunk_num}.csv", index=False)
    print(f"✅ Processed chunk {chunk_num}")

process_chunk_1 = PythonOperator(
    task_id="process_chunk_1",
    python_callable=lambda: process_data(0),
    dag=dag,
)

process_chunk_2 = PythonOperator(
    task_id="process_chunk_2",
    python_callable=lambda: process_data(1),
    dag=dag,
)

# Step 3: Merge Processed Data (Sequential)
def merge_data():
    df1 = pd.read_csv(f"{DATA_DIR}/processed_chunk_0.csv")
    df2 = pd.read_csv(f"{DATA_DIR}/processed_chunk_1.csv")
    merged_df = pd.concat([df1, df2])
    merged_df.to_csv(f"{DATA_DIR}/final_data.csv", index=False)
    print("✅ Data merged!")

merge_data_task = PythonOperator(
    task_id="merge_data",
    python_callable=merge_data,
    dag=dag,
)

# Step 4: Validate Output
def validate_output():
    if os.path.exists(f"{DATA_DIR}/final_data.csv"):
        print("✅ Final data file exists!")
    else:
        print("❌ Error: Final data file not found!")

validate_output_task = PythonOperator(
    task_id="validate_output",
    python_callable=validate_output,
    dag=dag,
)

# Define Dependencies
generate_data_task >> [process_chunk_1, process_chunk_2]  # Parallel processing
[process_chunk_1, process_chunk_2] >> merge_data_task  # Merge step
merge_data_task >> validate_output_task  # Final validation
