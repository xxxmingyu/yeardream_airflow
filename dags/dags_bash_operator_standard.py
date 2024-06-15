from airflow import DAG
import pendulum
from airflow.operators.empy import EmptyOperator

my_dag = DAG(
    dag_id="dags_bash_operator_standard",
    schedule = "0 9 * * 1,5",
    start_date=pendulum.datetime(2024, 6, 1, tz="Asia/Seoul"),
    catchup = False,
    tags=["homework"]
)
EmptyOperator(task_id="standart_t1", dag=my_dag)