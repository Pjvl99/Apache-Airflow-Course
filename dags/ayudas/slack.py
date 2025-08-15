from slack_sdk import WebClient
import os

def slack_notificaciones(context):
    '''
    Mediante un web request va a enviar mensajes a slack
    cada vez que falle un DAG con la info del mismo.
    '''
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    fecha_ejecucion = context.get("execution_date")
    log_url = context.get("task_instance").log_url

    token = os.environ.get("slack")

    cliente = WebClient(token=token)

    mensaje = (
        ":red_circle: *Hubo una falla!*\n"
        f"*DAG*: `{dag_id}`\n"
        f"*Tarea*: `{task_id}`\n"
        f"*Fecha de ejecucion*: `{fecha_ejecucion}`\n"
        f"*Logs*: {log_url}"
    )

    cliente.chat_postMessage(
        channel="airflow-notificaciones",
        text=mensaje
    )