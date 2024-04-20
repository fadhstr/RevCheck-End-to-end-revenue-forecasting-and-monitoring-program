from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine #koneksi ke postgres
import pandas as pd
    

def load_raw():
    database = "airflow_final_project"
    username = "airflow_final_project"
    password = "airflow_final_project"
    host = "postgres"

    # Membuat URL koneksi PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Gunakan URL ini saat membuat koneksi SQLAlchemy
    engine = create_engine(postgres_url)
    # engine= create_engine("postgresql+psycopg2://airflow:airflow@postgres/airflow")
    conn = engine.connect()

    df = pd.read_csv('dags/sales_raw.csv')
    #df.to_sql(nama_table_db, conn, index=False, if_exists='replace')
    df.to_sql('sales_raw', conn, index=False, if_exists='replace')  # M

def fetch_data():

    # fetch data
    database = "airflow_final_project"
    username = "airflow_final_project"
    password = "airflow_final_project"
    host = "postgres"

    # Membuat URL koneksi PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Gunakan URL ini saat membuat koneksi SQLAlchemy
    engine = create_engine(postgres_url)
    conn = engine.connect()

    df = pd.read_sql_query('select * from sales_raw', conn)
    df.to_csv('/opt/airflow/dags/sales_raw.csv', sep=',', index=False)
    
def preprocessing(): 
    
    data = pd.read_csv("/opt/airflow/dags/sales_raw.csv")

    data.dropna(inplace=True)
    data.drop_duplicates(inplace=True)

    data = data.drop(columns=['Day', 'Month', 'Year'])

    data['Date'] = pd.to_datetime(data['Date'])

    data.to_csv('/opt/airflow/dags/sales_clean.csv', index=False)
    
def load_csv_to_postgres():
    database = "airflow_final_project"
    username = "airflow_final_project"
    password = "airflow_final_project"
    host = "postgres"

    # Membuat URL koneksi PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Gunakan URL ini saat membuat koneksi SQLAlchemy
    engine = create_engine(postgres_url)
    # engine= create_engine("postgresql+psycopg2://airflow:airflow@postgres/airflow")
    conn = engine.connect()

    df = pd.read_csv('/opt/airflow/dags/sales_clean.csv')
    #df.to_sql(nama_table_db, conn, index=False, if_exists='replace')
    df.to_sql('sales_clean', conn, index=False, if_exists='replace')  # M
        
        
default_args = {
    'owner': 'dewa_almatin', 
    'start_date': datetime(2024, 3, 29, 0, 0)
}

with DAG(
    "final_project_DAG", #atur sesuai nama project kalian
    description='final_project',
    schedule_interval='30 6 * * *', #atur schedule untuk menjalankan airflow pada 06:30.
    default_args=default_args, 
    catchup=False
) as dag:
    load_pertama = PythonOperator(
        task_id='load_raw',
        python_callable=load_raw)

    #task: 1
    fetch_data_pg = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data)
    
    # Task: 2
    '''  Fungsi ini ditujukan untuk menjalankan pembersihan data.'''
    clean_data = PythonOperator(
        task_id='clean_data',
        python_callable=preprocessing)

    # Task: 3
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_csv_to_postgres)

    #proses untuk menjalankan di airflow
    load_pertama >> fetch_data_pg >> clean_data >> load_data