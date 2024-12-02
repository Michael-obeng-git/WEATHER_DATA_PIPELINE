# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.hooks.base_hook import BaseHook
# from airflow.hooks.postgres_hook import PostgresHook
# from airflow.sensors.http_sensor import HttpSensor
# import requests
# # import psycopg2
# from datetime import datetime, timedelta
# from pytz import timezone

# API_CONN_ID = 'openweather_api'
# POSTGRES_CONN_ID = 'postgres_conn_id'

# #Define the defaut argument
# default_args = {
#     'owner': 'michael',
#     'email':['michael.fordah@amalitech.com'],
#     'email_on_failure': False,
#     'email-on_retry': False,
#     'retry_delay': timedelta(minutes=5),
#     'retries':1
# }

# # weather_connection = BaseHook.get_connection(API_CONN_ID)
# # api_key = weather_connection.extra_dejson.get('api_key')

# postgres_conn_id = 'postgres'
# city_name = 'Portland'
# api_key = '91fe39ec5835cdbc33fef282d33ea256'

# #Create the DAG
# with DAG(
#     'open_weather',
#     default_args = default_args,
#     schedule_interval='@daily',
#     start_date= datetime(2024,11,28),
#     catchup = False,
#     tags=['open_weather']
# ) as dag:
    
#     # Task 1
#     check_weather_api_sensor = HttpSensor(
#         task_id='check_weather_api_sensor',
#         http_conn_id='openweather_api',
#         endpoint=f'2.5/weather?1={city_name}&APPID={api_key}',
#         response_check=lambda response: response.status_code == 200,
#         poke_interval =5,
#         timeout=20
#     )


# def fetch_weather():
#     url = f'https://api.openweathermap.org/data/2.5/weather?1={city_name}&APPID={api_key}'
#     response = requests.get(url)
#     if response.status_code == 200:
#         weather = response.json()
#         return weather
#     else:
#         raise Exception(f"Failed to fetch weather. Status code: {response.status_code}")

# # Task 2
# fetch_weather = PythonOperator(
#     task_id = 'fetch_weather',
#     python_callable = fetch_weather
# )

# #Transform weather data
# def transform_weather(ti):
#     weather = ti.xcom_pull(task_ids='fetch_weather')
#     # Converting temperature from kelvin to Fahrenheit
#     temp_kelvin = weather['main']['temp']
#     temp_fahrenheit = (temp_kelvin - 273.15) * 9/5 + 32
#     pressure = weather['main']['pressure']
#     humidity = weather['main']['humidity']
#     timestamp = datetime.fromtimestamp(weather['dt'], timezone.utc)
#     city = weather['name']
    
# # Task 3
# transform_weather = PythonOperator(
#     task_id = 'transform_weather',
#     python_callable = transform_weather
# )


# # Uploading data to postgres
# def load_weather_data(ti):
#     transform_weather = ti.xcom_pull(task_ids='transform_weather')
#     pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
#     conn = pg_hook.get_conn()
#     cursor = conn.cursor()
#     # Create schema
#     cursor.execute('CREATE SCHEMA IF NOT EXISTS OpenWeather;')
#     cursor.execute('''
#                    CREATE TABLE IF NOT EXISTS OpenWeather.weather(
#                        temp_fahrenheit float,
#                        pressure int,
#                        humidity int,
#                        timestamp timestamp,
#                        city varchar(50)
#                    );
#                    ''')
    
#     cursor.execute('''
#                    insert into OpenWeather.weather (temp_fahrenheit, pressure, humidity, timestamp,city)
#                    values ( %s,%s,%s,%s,%s);
#                    ''',transform_weather['temp_fahrenheit'], transform_weather['pressure'],transform_weather['humidity'],transform_weather['timestamp'],transform_weather['city'])
#     conn.commit()
#     cursor.close()
    
#     load_weather_data = PythonOperator(
#         task_id = 'load_weather_data',
#         python_callable = load_weather_data
#     )
    
    
    
# check_weather_api_sensor >> fetch_weather >> transform_weather >> load_weather_data



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





# def check_api_status(api_url):
#     response = requests.get(api_url)
#     return response.status_code == 200

# def fetch_weather_data(api_url,api_key,city):
#     response = requests.get(f"{api_url}?q={city}&appid={api_key}&units=metric")
#     if response.status_code == 200:
#         weather = response.json()
#     else: 
#         raise Exception(f"Failed to fetch weather data. Status code: {response.status_code}")

    # https://api.openweathermap.org/data/2.5/weather
    
# def transform_data(weather_data):
#     temp_fahrenheit = weather_data['main']['temp'] * 9/5 + 32
#     local_time = datetime.fromtimestamp(weather_data['dt'], timezone('Ghana/Accra'))
#     tranformed_data = {
#         'temperature': temp_fahrenheit,
#         'wind_speed': weather_data['wind']['speed'],
#         'humidity': weather_data['main']['humidity'],
#         'timestamp': local_time.strftime('%Y-%m-%d %H:%M:%S')
#     }
#     return transform_data

# def load_to_postgres (tranformed_data, postgres_conn_id):
#     conn = BaseHook.get_connection(postgres_conn_id)
#     connection = psycopg2.connect(
#         dbname = conn.schema,
#         user=conn.login,
#         passeord=conn.password,
#         host=conn.host,port=conn.port
#     )
#     cursor = connection.cursor()
#     insert_query = """
#         INSERT INTO weather_data (temperature, wind_speed, humidity, timestamp)
#         VALUES (%s, %s, %s, %s)
#     """
#     cursor.execute(insert_query, (tranformed_data['temperature'], tranformed_data['wind_speed'], tranformed_data['humidity'], tranformed_data['timestamp']))
#     connection.commit()
#     cursor.close()
#     connection.close()
    
#   #Definition of the constraints
# api_url = "https://api.openweathermap.org/data/2.5/weather"
# api_key = "91fe39ec5835cdbc33fef282d33ea256"
# city = "Portland"
# postgres_conn_id = API_CONN_ID

# # Tasks to check API status
# check_api_status = PythonOperator(
#     task_id = "check_api_status",
#     python_callable = check_api_status,
#     op_args=[api_url],
#     dag=dag
# )    

# # Task to fetch weather data
# fetch_weather_data= PythonOperator(
#     task_id = 'fetch_weather_data',
#     python_callable = fetch_weather_data,
#     op_args = [api_url,api_key,city],
#     provide_context = True,
#     dag=dag
# )

# # Task to transform the data
# transform_data_task = PythonOperator(
#     task_id = 'transform_data',
#     python_callable=transform_data,
#     provide_context = True,
#     dag=dag
# )

# # Task to Load data into PostgreSQL
# load_to_postgres = PythonOperator(
#     task_id = 'load_to_postgres',
#     python_callable=load_to_postgres,
#     provide_context = True,
#     dag=dag
# )


# check_api_status >> fetch_weather_data
# >> transform_data >> load_to_postgres
    