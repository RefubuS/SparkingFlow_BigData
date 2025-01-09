from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import airflow.utils.dates

dag = DAG(
    dag_id="silver_processing_DAG",
    default_args={
        "owner": "Filfawel",
        "start_date": airflow.utils.dates.days_ago(1),
    },
    schedule_interval="@yearly",
)

start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs started"),
    dag=dag,
)

place_processing = SparkSubmitOperator(
    task_id="process_place",
    conn_id="spark-conn",
    application="jobs/python/process_bronze_to_silver.py",
    application_args=[
        "--input_path", "/opt/data/bronze/partition_id=0/*.csv",
        "--output_base_path", "/opt/data/silver",
        "--table", "place",
    ],
    dag=dag,
)

weather_processing = SparkSubmitOperator(
    task_id="process_weather",
    conn_id="spark-conn",
    application="jobs/python/process_bronze_to_silver.py",
    application_args=[
        "--input_path", "/opt/data/bronze/partition_id=0/*.csv",
        "--output_base_path", "/opt/data/silver",
        "--table", "weather",
    ],
    dag=dag,
)

obstacles_processing = SparkSubmitOperator(
    task_id="process_obstacles",
    conn_id="spark-conn",
    application="jobs/python/process_bronze_to_silver.py",
    application_args=[
        "--input_path", "/opt/data/bronze/partition_id=0/*.csv",
        "--output_base_path", "/opt/data/silver",
        "--table", "obstacles",
    ],
    dag=dag,
)

accident_processing = SparkSubmitOperator(
    task_id="process_accident",
    conn_id="spark-conn",
    application="jobs/python/process_bronze_to_silver.py",
    application_args=[
        "--input_path", "/opt/data/bronze/partition_id=0/*.csv",
        "--output_base_path", "/opt/data/silver",
        "--table", "accident",
    ],
    dag=dag,
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag,
)

start >> [place_processing, weather_processing, obstacles_processing, accident_processing] >> end
