from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from docker.types import Mount

default_args = {
    'owner': 'data_engineer',
    'start_date': days_ago(1),
    'retries': 0,
}

NETWORK_NAME = 'livekart-streaming_livekart_net'

with DAG(
    dag_id='livekart_lambda_batch',
    default_args=default_args,
    schedule_interval='*/30 * * * *',
    catchup=False
) as dag:

    start = BashOperator(task_id='start_batch', bash_command='echo "Starting Batch Processing"')

    compact_task = BashOperator(
        task_id='compact_delta_files',
        bash_command='echo "Vacuuming Delta Lake tables..."'
    )

    profit_calc_task = DockerOperator(
        task_id='calculate_profit_margin',
        image='apache/spark:3.5.0', # Correct Image
        command='/opt/spark/bin/spark-submit --packages io.delta:delta-spark_2.12:3.0.0,org.postgresql:postgresql:42.6.0 /opt/airflow/scripts/batch_profit_calc.py',
        network_mode=NETWORK_NAME,
        mounts=[
            Mount(source="D:/livekart-streaming/scripts", target="/opt/airflow/scripts", type="bind"),
            Mount(source="D:/livekart-streaming/data", target="/opt/airflow/data", type="bind")
        ],
        auto_remove=True,
        user='root'
    )

    end = BashOperator(task_id='end_batch', bash_command='echo "Batch Complete"')

    start >> compact_task >> profit_calc_task >> end