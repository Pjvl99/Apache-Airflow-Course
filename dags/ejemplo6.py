# Definir el primer DAG del curso

from airflow import DAG #Vamos a definir el DAG
from airflow.operators.python import PythonOperator #Operador el molde para definir una tarea, vamos a interactuar con una función de Python
from airflow.sensors.python import PythonSensor
from datetime import datetime  # Calendarización
from airflow.models import Variable
from airflow.providers.slack.notifications.slack import SlackNotifier
from ayudas.slack import slack_notificaciones

# Ejemplo #6
# Voy a monitorear mi dag en una herramienta externa
# Voy a encontrar la manera de enviarme alertas cada vez que este dag sea exitoso/falle


def verificar_datos():
    import random
    datos = random.randint(1,6)
    print("El numero es:", datos)
    if datos in [1,2,3]:
        return True
    else:
        raise Exception("Los datos no son validos")
    
def procesar_datos():
    print("Procesando datos")

with DAG(
    dag_id="monitoreo_ejemplo",
    on_failure_callback=slack_notificaciones,
    start_date=datetime(2025,7,25), #Si la fecha de inicio es mayor a la fecha actual no va a ejecutarse (ni aunque se ejecute manualmente)
    schedule_interval="@daily", #Se va a ejecutar diariamente, es a la medianoche
    catchup=False #Si las tareas dependen del pasado, si yo defini mi tarea para 3 dias en el pasado y la ejecuta ahorita y la calendarizacion diaria, crear 3 ejecuciones.
) as dag:

    validando_datos = PythonSensor( #Validar una condicion, si la condicion es verdadera se procede a la siguiente tarea de lo contrario se puede omitir o recalendarizar
        task_id="validando_datos",
        python_callable=verificar_datos,
        mode="reschedule",
        poke_interval=10
    )

    procesamiento_de_datos = PythonOperator(
        task_id="procesamiento_de_datos",
        python_callable=procesar_datos
    )

    validando_datos >> procesamiento_de_datos #Esto va de izquierda a derecha, donde el valor mas a la izquierda va a ser el primero en ejecutarse