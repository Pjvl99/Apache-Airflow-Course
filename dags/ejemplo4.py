# Definir el primer DAG del curso

from airflow import DAG #Vamos a definir el DAG
from airflow.operators.python import PythonOperator #Operador el molde para definir una tarea, vamos a interactuar con una función de Python
from datetime import datetime  # Calendarización
from airflow.models import Variable

def generar_datos():
    return {'datos': [1,2,3,4,5]}

def procesar_datos(**context):
    datos = context['ti'].xcom_pull(task_ids='generar_datos') #Crear una comunicacion entre funciones - xcom_pull me permite jalar datos de otra funcion
    print(datos)

with DAG(
    dag_id="xcom_ejemplo",
    start_date=datetime(2025,7,25), #Si la fecha de inicio es mayor a la fecha actual no va a ejecutarse (ni aunque se ejecute manualmente)
    schedule_interval="@daily", #Se va a ejecutar diariamente, es a la medianoche
    catchup=False #Si las tareas dependen del pasado, si yo defini mi tarea para 3 dias en el pasado y la ejecuta ahorita y la calendarizacion diaria, crear 3 ejecuciones.
) as dag:
    
    generacion_de_datos = PythonOperator(
        task_id="generar_datos",
        python_callable=generar_datos
    )

    procesamiento_de_datos = PythonOperator(
        task_id="procesamiento_de_datos",
        python_callable=procesar_datos
    )

    generacion_de_datos >> procesamiento_de_datos #Esto va de izquierda a derecha, donde el valor mas a la izquierda va a ser el primero en ejecutarse