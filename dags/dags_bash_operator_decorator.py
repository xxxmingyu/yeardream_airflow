from airflow.decorators import dag
import pendulum
from airflow.operators.empty import EmptyOperator

@dag(start_date=pendulum.datetime(2024, 5, 1, tz="Asia/Seoul"), schedule="0 13 * * 5#2", tags=["homework"])
def generate_dag():
    EmptyOperator(task_id="decorator_t1")

generate_dag()