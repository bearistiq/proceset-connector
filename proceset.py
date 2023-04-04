import requests
import io
import pandas


class Proceset:
    """Класс для представления коннектора к Proceset"""

    def __init__(self, base_url: str, api_key: str) -> None:
        """Устанавливает все необходимые атрибуты коннектора"""
        self.base_url = base_url + 'graphql'
        self.api_key = api_key
        self.conn_guid = self.get_guid()

    def send_request(self, query: str) -> dict:
        """Возвращает результат выполнения GraphQL-запроса"""
        response = requests.post(self.base_url, params={'api_key': self.api_key}, json={'query': query})
        return response.json()

    def get_guid(self) -> str:
        """Извлекает GUID системного подключения к ClickHouse"""
        query = 'query{connection{connections(sorting_column:TYPE, sorting_direction:DESC)' \
                '{items{element{connection{guid}}}}}}'
        response = self.send_request(query)
        conn_guid = response['data']['connection']['connections']['items'][0]['element']['connection']['guid']
        return conn_guid

    def extract_data(self, sql_script: str) -> pandas.DataFrame:
        """Выполняет запрос на извлечение данных из ClickHouse"""
        query = f'query{{clickhouse{{extract_data(connection_guid:"{self.conn_guid}", sql_script:"{sql_script}")}}}}'
        response = requests.post(self.base_url, params={'api_key': self.api_key}, json={'query': query})
        return pandas.read_csv(io.StringIO(response.text), sep=';')

    def upload_data(self, database_name: str, table_name: str, dataframe: pandas.DataFrame) -> dict:
        """Загружает DataFrame в существующую таблицу Proceset"""
        query = f'mutation{{clickhouse{{upload_data_table(connection_guid: "{self.conn_guid}", ' \
                f'database_name: "{database_name}", table_name: "{table_name}")}}}}'
        csv_table = dataframe.to_csv(sep=';', index=False, date_format='%Y-%m-%d %H:%M:%S.%f')
        files = {'query': (None, query, 'application/json'), 'file': ('table.csv', csv_table, 'text/csv')}
        response = requests.post(self.base_url, params={'api_key': self.api_key}, files=files)
        return response.json()

    def execute_script(self, script_id: int) -> dict:
        """Запускает скрипт автоматизации в Proceset"""
        query = f'mutation{{automation{{script{{execute(id: {script_id}){{id, is_running}}}}}}}}'
        return self.send_request(query)
