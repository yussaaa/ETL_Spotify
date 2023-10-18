from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import XCom

from packages.spotify_ETL import (
    extract_recent_played_using_spotipy,
    json_to_df,
    load_to_sqlite,
)

default_args = {
    "owner": "airflow",
    # "depends_on_past": False,
    "start_date": datetime(2023, 10, 18),
    "email": ["yusa.li@hotmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "spotify_ETL_dag",
    default_args=default_args,
    description="Our first Spotify ETL DAG!",
    schedule=timedelta(days=1),
)

run_extract = PythonOperator(
    task_id="task_1_extract",
    python_callable=extract_recent_played_using_spotipy,
    dag=dag,
)
run_transform = PythonOperator(
    task_id="task_2_transform",
    python_callable=json_to_df,
    dag=dag,
)
run_load = PythonOperator(
    task_id="task_3_load",
    python_callable=load_to_sqlite,
    dag=dag,
)

run_extract >> run_transform >> run_load

if __name__ == "__main__":
    dag.test()
