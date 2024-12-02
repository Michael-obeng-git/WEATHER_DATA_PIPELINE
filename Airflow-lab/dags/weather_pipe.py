from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.hooks.postgres_hook import PostgresHook
import requests
from datetime import datetime, timedelta
from pytz import timezone, utc

API_CONN_ID = 'openweather_api'
POSTGRES_CONN_ID = 'postgres_conn_id'

# Define the default arguments
default_args = {
    'owner': 'michael',
    'email': ['michael.fordah@amalitech.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'retries': 2
}

# postgres_conn_id = 'postgres'
city_name = 'Portland'
api_key = '91fe39ec5835cdbc33fef282d33ea256'

# Create the DAG
with DAG(
    'open_weather',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 11, 28),
    catchup=False,
    tags=['open_weather']
) as dag:
    
    # Task 1
    check_weather_api_sensor = HttpSensor(
        task_id='check_weather_api_sensor',
        http_conn_id='openweather_api',
        endpoint=f'2.5/weather?q={city_name}&appid={api_key}',
        response_check=lambda response: response.status_code == 200,
        poke_interval=5,
        timeout=20
    )

    # Task 2
    def fetch_weather():
        url = f'https://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}'
        response = requests.get(url)
        if response.status_code == 200:
            weather = response.json()
            return weather
        else:
            raise Exception(f"Failed to fetch weather. Status code: {response.status_code}")

    fetch_weather_task = PythonOperator(
        task_id='fetch_weather',
        python_callable=fetch_weather
    )

    # Task 3
    def transform_weather(ti):
        weather = ti.xcom_pull(task_ids='fetch_weather')
        temp_kelvin = weather['main']['temp']
        temp_fahrenheit = (temp_kelvin - 273.15) * 9/5 + 32
        pressure = weather['main']['pressure']
        humidity = weather['main']['humidity']
        timestamp = datetime.fromtimestamp(weather['dt'], tz=utc)
        # timezone.utc
        city = weather['name']
        return {
            'temp_fahrenheit': temp_fahrenheit,
            'pressure': pressure,
            'humidity': humidity,
            'timestamp': timestamp,
            'city': city
        }

    transform_weather_task = PythonOperator(
        task_id='transform_weather',
        python_callable=transform_weather
    )

    # Task 4
    def load_weather_data(ti):
        transformed_weather = ti.xcom_pull(task_ids='transform_weather')
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        
        try:
            
            cursor = conn.cursor()
            cursor.execute('CREATE SCHEMA IF NOT EXISTS OpenWeather;')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS OpenWeather.weather(
                    temp_fahrenheit float,
                    pressure int,
                    humidity int,
                    timestamp timestamp,
                    city varchar(50)
                );
            ''')
            cursor.execute('''
                INSERT INTO OpenWeather.weather (temp_fahrenheit, pressure, humidity, timestamp, city)
                VALUES (%s, %s, %s, %s, %s);
            ''', (transformed_weather['temp_fahrenheit'], transformed_weather['pressure'],
                  transformed_weather['humidity'], transformed_weather['timestamp'], transformed_weather['city']))
            
            conn.commit()
        except Exception as e:
            print(f"An error occured: {e}")
            conn.rollback()
        finally: 
            cursor.close()

    load_weather_data_task = PythonOperator(
        task_id='load_weather_data',
        python_callable=load_weather_data
    )

    # Set task dependencies
    check_weather_api_sensor >> fetch_weather_task >> transform_weather_task >> load_weather_data_task

