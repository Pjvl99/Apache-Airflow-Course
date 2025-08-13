from datetime import datetime, timedelta 
from airflow import DAG 
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.weekday import WeekDay
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'pablo',
    'depends_on_past': False,
    'start_date': datetime(2025,1,1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def ver_dia_de_la_semana(**context):
    fecha_de_ejecucion = context['execution_date']

    if fecha_de_ejecucion.weekday() == WeekDay.SATURDAY or fecha_de_ejecucion.weekday() == WeekDay.SUNDAY:
        return 'fin_de_semana'
    elif fecha_de_ejecucion.day == 1:
        return 'inicio_de_mes'
    else:
        return 'dia_de_la_semana'
    
def tarea_de_la_semana():
    print("Es un dia de la semana")
    return 'dia_de_la_semana'

def tarea_de_fin_de_semana():
    print("Es un fin de semana")
    return 'fin_de_semana'

def tarea_de_inicio_de_mes():
    print("Es el primer dia del mes")
    return 'inicio_de_mes'

def reporte(**context):
    ti = context['ti']
    resultados = []
    for tarea in ['dia_de_la_semana', 'fin_de_semana', 'inicio_de_mes']:
        resultado = ti.xcom_pull(task_ids=tarea)
        if resultado:
            resultados.append(resultado)

    print("Resultados de mi reporte final:", resultados)

with DAG(
    dag_id='multi_rama_ejemplo',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['sesion3']
) as dag:
    inicio = DummyOperator(task_id='inicio')

    rama_condicional = BranchPythonOperator(
        task_id='ver_dia_de_la_semana',
        python_callable=ver_dia_de_la_semana,
        provide_context=True
    )

    rama_dia_de_la_semana = PythonOperator(
        task_id='dia_de_la_semana',
        python_callable=tarea_de_la_semana
    )

    rama_fin_de_semana = PythonOperator(
        task_id='fin_de_semana',
        python_callable=tarea_de_fin_de_semana
    )

    rama_inicio_de_mes = PythonOperator(
        task_id='inicio_de_mes',
        python_callable=tarea_de_inicio_de_mes
    )

    reporte_final = PythonOperator(
        task_id='reporte_final',
        python_callable=reporte,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED
    )

    final = DummyOperator(task_id='final')

    inicio >> rama_condicional
    rama_condicional >> [rama_fin_de_semana, rama_dia_de_la_semana, rama_inicio_de_mes]

    rama_inicio_de_mes >> reporte_final
    rama_fin_de_semana >> reporte_final
    rama_dia_de_la_semana >> reporte_final

    reporte_final >> final