from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'carlos_roque',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'retailnova_lakehouse_pipeline',
    default_args=default_args,
    description='Pipeline con Gatekeeper de Calidad (GX)',
    schedule_interval=None,
    catchup=False,
    tags=['retail', 'lakehouse', 'quality'],
) as dag:

    t1 = BashOperator(
        task_id='load_bronze',
        bash_command='docker exec spark-delta python3 /opt/scripts/bronze_load.py',
    )

    t2 = BashOperator(
        task_id='transform_silver',
        bash_command='docker exec spark-delta python3 /opt/scripts/bronze_to_silver.py',
    )

    # NUEVO PASO: Validación de Calidad como Gatekeeper
    t3 = BashOperator(
        task_id='quality_check_gx',
        bash_command='docker exec spark-delta python3 /opt/scripts/quality_check_silver.py',
    )

    t4 = BashOperator(
        task_id='aggregate_gold',
        bash_command='docker exec spark-delta python3 /opt/scripts/silver_to_gold.py',
    )

    t5 = BashOperator(
        task_id='cleanup_gdpr',
        bash_command='docker exec spark-delta python3 /opt/scripts/delete_gdpr.py',
    )

    t1 >> t2 >> t3 >> t4 >> t5
