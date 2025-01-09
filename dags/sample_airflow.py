import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id = "sample_airflow_BD",
    default_args = {
        "owner": "Filfawel",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "@yearly"
)

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

partition_csv_job = SparkSubmitOperator(
    task_id="partition_csv_job",
    conn_id="spark-conn",
    application="jobs/python/partition_csv.py",
    application_args=[
        "--input_csv", "/opt/data/source/US_Accidents_March23.csv",
        "--output_dir", "/opt/data/bronze",
        "--chunk_size", "800000"
    ],
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> partition_csv_job >> end