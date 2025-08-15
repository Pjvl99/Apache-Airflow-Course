from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataform import DataformCreateWorkflowInvocationOperator
from ayudas.slack import slack_notificaciones
import os

default_args = {
    'owner': 'pablo',
    'depends_on_past': False,
    'start_date': datetime(2025,1,1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

nombre_de_flujo_de_trabajo = 'consistencia_por_equipo'

proyecto = f'projects/{os.environ.get("proyecto")}'
localizacion = f'locations/{os.environ.get("region")}'
repositorio = f'repositories/{os.environ.get("repositorio")}'
tarea = f'workflowConfigs/{nombre_de_flujo_de_trabajo}'

with DAG(
    dag_id='oro.consistencia_por_equipo',
    description='Creacion o actualizacion de la tabla de plata con las estadisticas de la liga',
    default_args=default_args,
    schedule_interval='0 12 * * *', #Corra diariamente al medio dia
    catchup=False,
    max_active_runs=1,
    tags=['dataform', 'oro'],
    on_failure_callback=slack_notificaciones
) as dag:
    
    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id="create-workflow-invocation",
        project_id=os.environ.get("proyecto"),
        region=os.environ.get("region"),
        repository_id=os.environ.get("repositorio"),
        asynchronous=False,
        workflow_invocation={
            "workflow_config": f"{proyecto}/{localizacion}/{repositorio}/{tarea}" #La ubicacion y nombre de tarea
        }
    )
    
    create_workflow_invocation