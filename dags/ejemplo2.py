# Definir el primer DAG del curso

from airflow import DAG #Vamos a definir el DAG
from airflow.operators.python import PythonOperator #Operador el molde para definir una tarea, vamos a interactuar con una función de Python
from datetime import datetime  # Calendarización

#Sin los operadores no van a poder ejecutar nada en airflow, son vitales para cada tarea.

# Estructura
# 1. Importacion de librerias
# 2. Importacion de operadores
# 3. Creacion de la logica de programacion (funcion de python)
# 4. Definicion de nuestro DAG, para esto utilizamos la funcion DAG que importamos
# 5. Crear la tarea, para hacerlo mandamos a llamar un operador que es lo que interactua con airflow al momento de una ejecucion
# 6. Mandamos a definir el orden en el que queramos que se ejecuten nuestras tareas (utilizamos las variables de las tareas)

def saludo():
    print("Hola Mundo")

with DAG(
    dag_id="ejemplo_catchup",
    start_date=datetime(2025,7,25), #Si la fecha de inicio es mayor a la fecha actual no va a ejecutarse (ni aunque se ejecute manualmente)
    schedule_interval="@daily", #Se va a ejecutar diariamente, es a la medianoche
    catchup=True #Si las tareas dependen del pasado, si yo defini mi tarea para 3 dias en el pasado y la ejecuta ahorita y la calendarizacion diaria, crear 3 ejecuciones.
) as dag:
    
    tarea1 = PythonOperator( #Me permite llamar funciones de python
        task_id="primer_tarea",
        python_callable=saludo
    )

    tarea1