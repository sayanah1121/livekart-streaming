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
SPARK_IMAGE = 'apache/spark:3.5.0'

def create_spark_task(task_id, script_name):
    return DockerOperator(
        task_id=task_id,
        image=SPARK_IMAGE,
        command=(
            "/opt/spark/bin/spark-submit "
            "--packages io.delta:delta-spark_2.12:3.0.0,org.postgresql:postgresql:42.6.0 "
            "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
            "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
            # INCREASED MEMORY TO 1GB (Safe because we run sequentially)
            "--driver-memory 1024m "
            "--executor-memory 1024m "
            f"/opt/airflow/scripts/{script_name}"
        ),
        network_mode=NETWORK_NAME,
        # CRITICAL FIX FOR WINDOWS DOCKER
        mount_tmp_dir=False,
        mounts=[
            Mount(source="D:/livekart-streaming/scripts", target="/opt/airflow/scripts", type="bind"),
            Mount(source="D:/livekart-streaming/data", target="/opt/airflow/data", type="bind")
        ],
        auto_remove=True,
        user='root'
    )

with DAG(
    dag_id='livekart_analytics_sequential_v3', # <--- V3 to force reload
    default_args=default_args,
    schedule_interval='*/30 * * * *',
    catchup=False
) as dag:

    start = BashOperator(task_id='start_batch', bash_command='echo "Starting Batch V3"')
    
    compact = BashOperator(
        task_id='compact_delta',
        bash_command='echo "Compacting..."'
    )

    t1 = create_spark_task('calc_profit', 'batch_profit_calc.py')
    t2 = create_spark_task('calc_users', 'batch_user_segmentation.py')
    t3 = create_spark_task('calc_supply', 'batch_supplier_perf.py')
    t4 = create_spark_task('calc_share', 'batch_category_share.py')
    t5 = create_spark_task('calc_velocity', 'batch_hourly_velocity.py')
    t6 = create_spark_task('calc_top', 'batch_top_products.py')

    end = BashOperator(task_id='end_batch', bash_command='echo "Done"')

    # Sequential Chain
    start >> compact >> t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> end