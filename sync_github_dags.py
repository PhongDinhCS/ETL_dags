from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 14),  # Adjust as needed
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    "sync_github_dags",
    default_args=default_args,
    schedule_interval="* * * * *",  # Runs every 1 minutes
    catchup=False,
)

# Bash command to update the DAGs folder from GitHub
git_pull_task = BashOperator(
    task_id="git_pull_latest_dags",
    bash_command="""
        cd /home/azureuser/airflow/dags &&
        git reset --hard &&
        git pull origin main
    """,
    dag=dag,
)

git_pull_task
