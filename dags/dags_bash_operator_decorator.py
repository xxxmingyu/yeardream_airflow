from airflow.decorators import dag
import pendulum
from airflow.operators.empty import EmptyOperator

@dag(start_date=pendulum.datetime(2024, 5, 1, tz="Asia/Seoul"), schedule="0 13 * * 5#2", catchup=False, tags=["homework"])
def generate_dag(task_id):
    EmptyOperator(task_id)

dag = generate_dag("decorator_t1")