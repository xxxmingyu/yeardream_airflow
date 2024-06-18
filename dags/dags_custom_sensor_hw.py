from sensors.seoul_api_date_sensor import SeoulApiDateSensor
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_custom_sensor_hw',
    start_date=pendulum.datetime(2024,6,14, tz='Asia/Seoul'),
    schedule='0 9 * * *',
    catchup=False
) as dag:
    sensor__tb_cycle_rent_use_day_info = SeoulApiDateSensor(
        task_id='sensor__List_air_quality_by_district_service',
        dataset_nm='/xml/ListAirQualityByDistrictService',
        check_date='{{data_interval_start.in_timezone("Asia/Seoul") | ds_nodash }}'
    )
