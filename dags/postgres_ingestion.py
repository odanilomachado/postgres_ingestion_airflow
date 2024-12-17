import requests
import pandas as pd
from configparser import ConfigParser
from sqlalchemy import create_engine
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

TMP_DIR = '/tmp/'

def _request_api(ti):
    response = requests.get('https://randomuser.me/api/?results=15')
    create_dataframe(response.json(), ti)

def create_dataframe(response_json, ti):
    users_info_columns = ['first_name','last_name','gender','email','date_of_birth','cell_phone','phone']
    users_info_data=[]

    for user in response_json['results']:
        first_name = user['name']['first']
        last_name = user['name']['last']
        gender = user['gender']
        email = user['email']
        date_of_birth = user['dob']['date']
        cell_phone = user['cell']
        phone = user['phone']
        users_info_data.append([first_name, last_name, gender, email, date_of_birth, cell_phone, phone])

    users_df = pd.DataFrame(data=users_info_data, columns=users_info_columns)

    datetime_str = datetime.now().strftime('%Y%m%d_%H%M%S')

    filename = f'{TMP_DIR}users_info_{datetime_str}'

    users_df.to_csv(f'{filename}.csv', na_rep='NULL', index=False) # na_rep adiciona NULL aos valores faltantes.

    ti.xcom_push(key='filename', value=filename)

def database_connection():
    config = ConfigParser()
    config_file_dir = '/opt/airflow/config/DB_Postgres.cfg'
    config.read_file(open(config_file_dir))

    db_host = config.get('DB_Postgres','db_host')
    db_schema = config.get('DB_Postgres','db_schema')
    db_user = config.get('DB_Postgres','db_user')
    db_password = config.get('DB_Postgres','db_password')

    try:
        engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}/{db_schema}')
        return engine    
    except Exception as e:
        raise Exception(f'Conecção mal sucedida! Erro: {e}')
    
def _load_to_db(ti):
    filename = ti.xcom_pull(key='filename')

    users_df = pd.read_csv(f'{filename}.csv', header=0)

    users_df.to_sql(schema='public', name='users', con=database_connection(), index=False, if_exists='append')

with DAG(
    'ingest_postgre',
    start_date=datetime(2024, 12, 17),
    tags=['airflow_project'],
    schedule_interval='* * * * *',
    catchup=False
) as dag:
    
    request_api = PythonOperator(
        task_id='request_api',
        python_callable=_request_api
    )

    load_to_db = PythonOperator(
        task_id='load_to_db',
        python_callable=_load_to_db
    )

    request_api >> load_to_db