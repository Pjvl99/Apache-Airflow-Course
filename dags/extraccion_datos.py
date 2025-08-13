'''
DAG:
  1. Extraer datos de wikipedia usando scrapy
  2. Esto va a ser orquestado con airflow
  3. Vamos a conectarnos a google cloud storage
  4. Vamos a almacenar esa data en nuestro balde de datos
'''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.sensors.bash import BashSensor
from airflow.decorators import task
from ayudas.slack import slack_notificaciones
import os

default_args = {
    'owner': 'pablo',
    'depends_on_past': False,
    'start_date': datetime(2025,1,1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

@task #Definiendo una tarea => Reemplazo a pythonOperator
def crear_listado_archivo():
    '''
    Iterar por carpeta y archivo y lo voy a mandar a gcs
    '''
    mapa_archivos = []
    for principal, _, archivos in os.walk('/usr/local/airflow/datos'):
        for archivo in archivos:
            path_local = os.path.join(principal, archivo)
            path_relativo = os.path.relpath(path_local, '/usr/local/airflow/datos') #bronce/clasificacion/.html
            gcs_ubicacion = os.path.join('bronce', path_relativo)
            mapa_archivos.append({'path_local': path_local, 'gcs_ubicacion': gcs_ubicacion})
    return mapa_archivos

with DAG(
    dag_id='extraccion_dato_laliga_wikipedia',
    description='Proceso para extraer datos de wikipedia usando scrapy a gcs',
    default_args=default_args,
    schedule_interval='0 12 * * *', #Corra diariamente al medio dia
    catchup=False,
    max_active_runs=1,
    on_failure_callback=slack_notificaciones,
    tags=['laliga']
) as dag:
    '''
    Ejecucion scrapy
    Validaciones
    Subida de archivos a gcs
    '''
    ejecutar_scrapy = BashOperator(
        task_id='ejecutar_scrapy', 
        bash_command='cd /usr/local/airflow/dags/scraper && python -m scrapy crawl laliga'
    )

    revisar_folderes = BashSensor(
        task_id='revisar_folderes', #Revisar que existan la 4 carpeta (que se ejecuto y encontro toda la data correctamente)
        bash_command='''
            COUNT=$(find /usr/local/airflow/datos/ -mindepth 1 -maxdepth 1 -type d | wc -l)
            if [ "$COUNT" -eq 4 ]; then
              exit 0
            else
              exit 99
            fi
        ''',
        soft_fail=True,
        timeout=60
    ) #Ya no se ejecutaria la siguiente tarea si mi sensor falla

    mapa_archivos = crear_listado_archivo()

    subir_gcs = LocalFilesystemToGCSOperator.partial(
        task_id='subir_gcs',
        bucket='melodic-subject-467218-g1-proyecto-datos'
    ).expand_kwargs(mapa_archivos.map(lambda archivos: {'src': archivos['path_local'], 'dst': archivos['gcs_ubicacion']}))

    ejecutar_scrapy >> revisar_folderes >> mapa_archivos >> subir_gcs
