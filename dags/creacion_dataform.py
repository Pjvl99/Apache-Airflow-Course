from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataform import DataformCreateRepositoryOperator, DataformCreateWorkspaceOperator
from airflow.models import Variable
from airflow.decorators import task
import os

default_args = {
    'owner': 'pablo',
    'depends_on_past': False,
    'start_date': datetime(2025,1,1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='creacion_de_dataform',
    description='Vamos a crear nuestro espacio de dataform automáticamente',
    default_args=default_args,
    schedule_interval='0 12 * * *', #Corra diariamente al medio dia
    catchup=False,
    max_active_runs=1,
    tags=['dataform']
) as dag:
    
    crear_repo = DataformCreateRepositoryOperator(
        task_id='hacer_repo',
        project_id=Variable.get("proyecto"),
        region=Variable.get("region"),
        repository_id=Variable.get("repositorio")
    )

    espacio_trabajo = DataformCreateWorkspaceOperator(
        task_id='crear_espacio_de_trabajo',
        project_id=Variable.get("proyecto"),
        region=Variable.get("region"),
        repository_id=Variable.get("repositorio"),
        workspace_id="pablo"
    )

    crear_repo >> espacio_trabajo
