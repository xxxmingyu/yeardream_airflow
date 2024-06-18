from airflow.sensors.base import BaseSensorOperator
from airflow.hooks.base import BaseHook
'''
서울시 공공데이터 API 추출시 특정 날짜로 업데이트 되었는지 확인하는 센서 
{dataset}/1/5/{yyyymmdd} 형태로 조회하는 데이터셋만 적용 가능
response 필드에 yyyymmdd 속성이 존재하는 데이터셋만 적용 가능
'''

class SeoulApiDateSensorHw(BaseSensorOperator):
    template_fields = ('endpoint','check_date')

    def __init__(self, dataset_nm, check_date=None, **kwargs):
        '''
        dataset_nm: 서울시 공공데이터 포털에서 센싱하고자 하는 데이터셋 명
        base_dt_col: 센싱 기준 컬럼 (yyyy.mm.dd... or yyyy/mm/dd... 형태만 가능)
        day_off: 배치일 기준 생성여부를 확인하고자 하는 날짜 차이를 입력 (기본값: 0)
        '''
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json' + dataset_nm + '/1/5'  # 5건만 추출
        self.check_date = check_date
        self.subcol_nm = kwargs.get('subcol_nm')
        self.column_nm = kwargs.get('column_nm')

    def poke(self, context):
        import requests
        import json
        connection = BaseHook.get_connection(self.http_conn_id)
        if self.check_date == None:
            url = f'http://{connection.host}:{connection.port}/{self.endpoint}'
        else:
            url = f'http://{connection.host}:{connection.port}/{self.endpoint}/{self.check_date}'
        self.log.info(f'url: {url}')
        response = requests.get(url)
        contents = json.loads(response.text)
        self.log.info(f'response: {contents}')
        code = contents.get('CODE')
        keys = contents.get('ListAirQualityByDistrictService').get('row')[0].get('MSRDATE')
        self.log.info(f'keys: {keys}')
        self.log.info(self.subcol_nm)
        self.log.info(self.column_nm)