from airflow.sensors.base import BaseSensorOperator
from airflow.hooks.base import BaseHook
'''
서울시 공공데이터 API 추출시 특정 날짜로 업데이트 되었는지 확인하는 센서 
response 필드에 yyyymmdd 속성이 존재하는 데이터셋만 적용 가능
'''

class SeoulApiDateSensorHw(BaseSensorOperator):
    template_fields = ('endpoint')

    def __init__(self, dataset_nm, subcol_nm, column_nm, **kwargs):
        '''
        dataset_nm: 서울시 공공데이터 포털에서 센싱하고자 하는 데이터셋 명
        base_dt_col: 센싱 기준 컬럼 (yyyy.mm.dd... or yyyy/mm/dd... 형태만 가능)
        day_off: 배치일 기준 생성여부를 확인하고자 하는 날짜 차이를 입력 (기본값: 0)
        '''
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json' + dataset_nm  # 5건만 추출
        self.subcol_nm = subcol_nm
        self.column_nm = column_nm

    def poke(self, context):
        import requests
        import json
        import datetime
        connection = BaseHook.get_connection(self.http_conn_id)
        start_row = 1
        end_row = 1000

        while True:
            if start_row == 1000001:
                break
            url = f'http://{connection.host}:{connection.port}/{self.endpoint}/{start_row}/{end_row}'
            self.log.info(f'url: {url}')
            response = requests.get(url)
            contents = json.loads(response.text)
            self.log.info(f'response: {contents}')
            for i in self.subcol_nm:
                contents = contents.get(i)
            for i in range(start_row, end_row):
                self.log.info(type(contents[i].get(self.column_nm)))
                key = datetime.datetime.strptime(contents[i].get(self.column_nm), '%Y%m%d')
                targetday = datetime(2024, 6, 18)

                if key == targetday:
                    self.log.info('지정된 날에 저장된 값이 있습니다')
                    self.log.info(contents)
                    return True

                if key < targetday:
                    self.log.info('지정된 날에는 저장된 값이 없습니다')
                    return False
                
            start_row = end_row + 1
            end_row = end_row + 1000