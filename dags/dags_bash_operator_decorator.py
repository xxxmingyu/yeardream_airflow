from airflow.decorators import dag
import pendulum
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

@dag(dag_id='dags_bash_operator_decorator', 
     start_date=pendulum.datetime(2024, 5, 1, tz="Asia/Seoul"), 
     schedule="0 13 * * 5#2", 
     catchup=False, 
     tags=["homework"])
def dags_bash_operator_decorator():
    bash_t1 = BashOperator(
        task_id="decorator_t1",
        bash_command="echo whoami"
    )
    bash_t2 = BashOperator(
        task_id="decorator_t2",
        bash_command="echo $HOSTNAME"
    )
    bash_t1 >> bash_t2

dags_bash_operator_decorator()