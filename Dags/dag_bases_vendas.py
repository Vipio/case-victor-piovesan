from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Default arguments for the DAG
default_args = {
    'owner': 'Victor Piovesan',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'cloud_storage_to_bigquery',
    default_args=default_args,
    description='DAG para verificar arqs novos no storage, triggar function e rodar proc bq',
    schedule_interval=timedelta(days=1), 
    start_date=datetime(2024, 4, 18)
)

# Verifica chegada de arqs novos
gcs_sensor_task = GoogleCloudStorageObjectSensor(
    task_id='gcs_sensor_task',
    bucket='case-boti',
    object='entrada/*',
    mode='poke',
    timeout=600,
    poke_interval=60,
    dag=dag,
)

# 
pubsub_task = PubSubPublishMessageOperator(
        task_id='pubsub_task',
        project_id='case-boti-420516',
        topic='bases_vendas',
        messages=[{"data": b"start_function"}],
        dag=dag)


clean_table_task = BigQueryExecuteQueryOperator(
        task_id=f'clean_table_task',
        sql=f'call boticario.proc_bases_vendas()', #escrever a proc aqui
        use_legacy_sql=False,
        location='us-central1',
        dag=dag,
    )

dummy_task = DummyOperator(task_id='dummy_task', 
                           dag=dag)





# Define task dependencies
gcs_sensor_task >> pubsub_task >>  clean_table_task >> dummy_task 

procs = ['proc_con_ano_mes',
         'proc_con_lin_ano_mes', 
         'proc_con_mar_ano_mes', 
         'proc_con_mar_lin']

for i in procs:

    var_name = f"procedure_{i}_task"
    
    globals()[var_name] = BigQueryExecuteQueryOperator(
        task_id=f'bigquery_{i}_task',
        sql=f'call boticario.{i}()', #escrever a proc aqui
        use_legacy_sql=False,
        location='us-central1',
        dag=dag,
    )

    dummy_task >> [globals()[var_name]]