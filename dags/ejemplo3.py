# Definir el primer DAG del curso

from airflow import DAG #Vamos a definir el DAG
from airflow.operators.python import PythonOperator #Operador el molde para definir una tarea, vamos a interactuar con una función de Python
from datetime import datetime  # Calendarización
from airflow.models import Variable
import os

def saludo():
    mensaje = os.environ.get("saludo", default_var="Hola Mundo") #Si esto no existe va a fallar
    print(mensaje)

with DAG(
    dag_id="ejemplo_variable",
    start_date=datetime(2025,7,25), #Si la fecha de inicio es mayor a la fecha actual no va a ejecutarse (ni aunque se ejecute manualmente)
    schedule_interval="@daily", #Se va a ejecutar diariamente, es a la medianoche
    catchup=False #Si las tareas dependen del pasado, si yo defini mi tarea para 3 dias en el pasado y la ejecuta ahorita y la calendarizacion diaria, crear 3 ejecuciones.
) as dag:
    
    tarea1 = PythonOperator( #Me permite llamar funciones de python
        task_id="primer_tarea",
        python_callable=saludo
    )

    tarea1