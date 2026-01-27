from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='simple_taskflow_dag',
    default_args=default_args,
    description='Простой DAG с TaskFlow API',
    schedule='@daily',
    start_date=datetime(2026, 1, 27),
    catchup=False,
)
def my_simple_dag():
    
    @task
    def extract():
        print("Извлекаем данные")
        return {'data': [1, 2, 3, 4, 5]}
    
    @task
    def transform(data: dict):
        print(f"Трансформируем данные: {data}")
        transformed = [x * 2 for x in data['data']]
        return {'transformed': transformed}
    
    @task
    def load(data: dict):
        print(f"Загружаем данные: {data}")
        return "Данные успешно загружены"
    
    # Определяем поток данных
    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load(transformed_data)

# Создаем экземпляр DAG
dag_instance = my_simple_dag()

