from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import airflow.utils.dates

dag = DAG(
    dag_id="gold_processing_DAG",
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

state_statistics_processing = SparkSubmitOperator(
    task_id="process_state_statistics",
    conn_id="spark-conn",
    application="jobs/python/process_silver_to_gold.py",
    application_args=[
        "--input_path", "/opt/data/silver",
        "--output_path", "/opt/data/gold/state_statistics",
        "--process", "state_statistics",
    ],
    dag=dag,
)

crossing_ranking_processing = SparkSubmitOperator(
    task_id="process_crossing_ranking",
    conn_id="spark-conn",
    application="jobs/python/process_silver_to_gold.py",
    application_args=[
        "--input_path", "/opt/data/silver",
        "--output_path", "/opt/data/gold/crossing_ranking",
        "--process", "crossing_ranking",
    ],
    dag=dag,
)

junction_ranking_processing = SparkSubmitOperator(
    task_id="process_junction_ranking",
    conn_id="spark-conn",
    application="jobs/python/process_silver_to_gold.py",
    application_args=[
        "--input_path", "/opt/data/silver",
        "--output_path", "/opt/data/gold/junction_ranking",
        "--process", "junction_ranking",
    ],
    dag=dag,
)

weather_severity_ranking_processing = SparkSubmitOperator(
    task_id="process_weather_severity_ranking",
    conn_id="spark-conn",
    application="jobs/python/process_silver_to_gold.py",
    application_args=[
        "--input_path", "/opt/data/silver",
        "--output_path", "/opt/data/gold/weather_severity_ranking",
        "--process", "weather_severity_ranking",
    ],
    dag=dag,
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag,
)

start >> [state_statistics_processing, crossing_ranking_processing, junction_ranking_processing, weather_severity_ranking_processing] >> end
