from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
import pandas as pd
import os

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 14),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    "advanced_etl_pipeline",
    default_args=default_args,
    schedule_interval="*/5 * * * *",  # Runs every 5 minutes
    catchup=False,
)

# 1️⃣ Step 1: Fetch latest DAGs from GitHub
git_pull_task = BashOperator(
    task_id="git_pull_latest_dags",
    bash_command="""
        cd /home/azureuser/airflow/dags &&
        git reset --hard &&
        git pull origin main
    """,
    dag=dag,
)

# 2️⃣ Step 2: Generate random data
def generate_data():
    df = pd.DataFrame({
        "id": range(1, 101),
        "value": [random.randint(1, 100) for _ in range(100)],
        "timestamp": pd.date_range(start="2025-03-14", periods=100, freq="T"),
    })
    df.to_csv("/home/azureuser/airflow/tmp/random_data.csv", index=False)
    print("✅ Data generated!")

generate_data_task = PythonOperator(
    task_id="generate_random_data",
    python_callable=generate_data,
    dag=dag,
)

# 3️⃣ Step 3: Parallel Processing (Splitting the dataset)
def process_chunk(chunk_num):
    df = pd.read_csv("/home/azureuser/airflow/tmp/random_data.csv")
    chunk_size = len(df) // 2
    chunk = df.iloc[chunk_num * chunk_size: (chunk_num + 1) * chunk_size]
    chunk.to_csv(f"/home/azureuser/airflow/tmp/processed_chunk_{chunk_num}.csv", index=False)
    print(f"✅ Processed chunk {chunk_num}")

process_chunk_1 = PythonOperator(
    task_id="process_chunk_1",
    python_callable=lambda: process_chunk(0),
    dag=dag,
)

process_chunk_2 = PythonOperator(
    task_id="process_chunk_2",
    python_callable=lambda: process_chunk(1),
    dag=dag,
)

# 4️⃣ Step 4: Merge Processed Data
def merge_data():
    df1 = pd.read_csv("/home/azureuser/airflow/tmp/processed_chunk_0.csv")
    df2 = pd.read_csv("/home/azureuser/airflow/tmp/processed_chunk_1.csv")
    merged_df = pd.concat([df1, df2])
    merged_df.to_csv("/home/azureuser/airflow/tmp/final_data.csv", index=False)
    print("✅ Data merged successfully!")

merge_data_task = PythonOperator(
    task_id="merge_processed_data",
    python_callable=merge_data,
    dag=dag,
)

# 5️⃣ Step 5: Load Data (Simulated)
def load_data():
    if os.path.exists("/home/azureuser/airflow/tmp/final_data.csv"):
        print("✅ Data loaded into storage!")
    else:
        print("❌ Error: Final data file not found!")

load_data_task = PythonOperator(
    task_id="load_data",
    python_callable=load_data,
    dag=dag,
)

# Define DAG dependencies
git_pull_task >> generate_data_task  # Fetch DAGs before processing
generate_data_task >> [process_chunk_1, process_chunk_2]  # Parallel execution
[process_chunk_1, process_chunk_2] >> merge_data_task  # Wait for both to finish
merge_data_task >> load_data_task  # Final step
