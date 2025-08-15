from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataform import DataformCreateWorkflowInvocationOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from ayudas.slack import slack_notificaciones
import os

default_args = {
    'owner': 'pablo',
    'depends_on_past': False,
    'start_date': datetime(2025,1,1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

nombre_de_flujo_de_trabajo = 'plata_laliga_estadisticas'

proyecto = f'projects/{os.environ.get("proyecto")}'
localizacion = f'locations/{os.environ.get("region")}'
repositorio = f'repositories/{os.environ.get("repositorio")}'
tarea = f'workflowConfigs/{nombre_de_flujo_de_trabajo}'

with DAG(
    dag_id='plata.estadisticas_de_la_liga',
    description='Creacion o actualizacion de la tabla de plata con las estadisticas de la liga',
    default_args=default_args,
    schedule_interval='0 12 * * *', #Corra diariamente al medio dia
    catchup=False,
    max_active_runs=1,
    tags=['dataform', 'plata'],
    on_failure_callback=slack_notificaciones
) as dag:
    
    crear_canalizacion_dataflow = BeamRunPythonPipelineOperator( #Este no solo valida su conexion en la interfaz de airflow sino que usa gcloud (sistema O) tmb para validar.
        task_id="iniciar_canalizacion_python",
        runner="DataflowRunner",
        py_file=f"{os.environ.get("bucket")}/apache_beam_plata.py",
        pipeline_options={
            "sdk_container_image" : "us-east1-docker.pkg.dev/melodic-subject-467218-g1/dataflow/dataflow_canalizacion:latest"
        },
        py_options=[],
        py_requirements=[
            "apache-beam[gcp]==2.66.0",
            "html5lib==1.1",
            "pandas==2.2.2",
            "pyarrow==16.1.0",
            "numpy==1.26.4"
        ],
        py_interpreter="python3",
        py_system_site_packages=False,
        dataflow_config={
            "location": os.environ.get("region"),
            "job_name": "transformacion_plata"
        }
    )

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
    
    crear_canalizacion_dataflow >> create_workflow_invocation