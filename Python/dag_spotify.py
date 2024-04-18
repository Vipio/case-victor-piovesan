from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta, datetime
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator

project_id = 'case-boti-420516'

# Argumentos padrao do airflow
default_args = {
            "owner": "Victor Piovesan",
            "depends_on_past": False,
            "start_date": datetime(2024,4,18), # Data de início da DAG
            "retries": 0,
        }

# Nome da dag, descricao, e frequencia de execucao
dag = DAG(
    dag_id="pipeline_spotify",
    default_args = default_args,
    description = "Trigga pub/sub da api Spotify",
    schedule_interval = None #'0 0 * * *' para rodar todo dia meia noite.
    )

def pubsub(task, topic_id):
    send_message = PubSubPublishMessageOperator(
        task_id=task,
        project_id=project_id,
        topic=topic_id,
        messages=[{"data": b"start_function"}],
        dag=dag)
        
    return send_message


apis_spotify = pubsub('apis_spotify', 'api-spotify')

apis_spotify



