from sqlalchemy import create_engine #koneksi ke postgres
from airflow.models import DAG
from airflow.operators.python import PythonOperator

import pandas as pd
import numpy as np
import datetime

from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Bidirectional
from tensorflow.keras.layers import LSTM
from tensorflow.keras.preprocessing.sequence import TimeseriesGenerator

from sklearn.preprocessing import MinMaxScaler


def fetch_data():

    database = "airflow_final_project"
    username = "airflow_final_project"
    password = "airflow_final_project"
    host = "postgres"

    # Membuat URL koneksi PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Gunakan URL ini saat membuat koneksi SQLAlchemy
    engine = create_engine(postgres_url)
    conn = engine.connect()

    df = pd.read_sql_query('select * from sales_clean', conn)
    df.to_csv('/opt/airflow/dags/sales_clean.csv', sep=',', index=False)

def generate_date_list(start_date):
    """
    Generate a list of 12 consecutive dates starting from the given start date in the specified format.
    
    Parameters:
        start_date (str): Start date in 'YYYY-MM-DD' format.
    
    Returns:
        pd.DatetimeIndex: DatetimeIndex containing 12 consecutive dates in the specified format.
    """
    # Convert start date to datetime object
    start_date = pd.to_datetime(start_date)
    
    # Generate 12 consecutive dates starting from the start date
    date_list = pd.date_range(start=start_date, periods=12, freq='W-SUN')
    
    return date_list

def train_pred():

    data = pd.read_csv('/opt/airflow/dags/sales_clean.csv')
    data['Date'] = pd.to_datetime(data['Date'])
    data = data.loc[data['Date'] >= '2015-07-01']

    data.columns = map(str.lower, data.columns)

    rev= data.groupby('date')['revenue'].sum().reset_index()
    rev = rev.set_index('date')

    df = rev['revenue'].resample('W').sum()

    # Convert the pandas Series into a NumPy array
    data_array = np.array(df).reshape(-1,1)

    # Define the proportion of data to allocate for training
    train_size = 0.78  # 80% of the data for training, 20% for testing

    # Calculate the index that separates the training and testing sets
    train_index = int(len(data_array) * train_size)

    # Split the data into training and testing sets
    train = data_array[:train_index]
    test = data_array[train_index:]

    #Scale train and test data to [-1, 1]
    scaler = MinMaxScaler()
    scaler.fit(train)
    train = scaler.transform(train)
    test = scaler.transform(test)

    n_input = 12
    # univariate
    n_features = 1
    #TimeseriesGenerator automatically transform a univariate time series dataset into a supervised learning problem.
    generator = TimeseriesGenerator(train, train, length=n_input, batch_size=10)

    model_bi = Sequential()
    model_bi.add(Bidirectional(LSTM(50, activation='relu'), input_shape=(12, 1)))
    model_bi.add(Dense(1))
    model_bi.compile(optimizer='adam', loss='mape')

    model_bi.fit_generator(generator,epochs=800)

    last_week = df[-1:].index
    pred_start = last_week.strftime('%Y-%m-%d').tolist()
    pred_start[0]

    pred_list_b = []
    last_week = df[-1:].index
    pred_start = last_week.strftime('%Y-%m-%d').tolist()

    batch = test.reshape((1, n_input, n_features))

    for i in range(n_input):   
        pred_list_b.append(model_bi.predict(batch)[0]) 
        batch = np.append(batch[:,1:,:],[[pred_list_b[i]]],axis=1)

    df_predict_bi = pd.DataFrame(scaler.inverse_transform(pred_list_b),
                            index=generate_date_list(pred_start[0]), columns=['Prediction'])

    df_predict_bi.to_csv('/opt/airflow/dags/pred.csv', sep=',', index=True)

def load_data():
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

    df = pd.read_csv('/opt/airflow/dags/pred.csv')
    #df.to_sql(nama_table_db, conn, index=False, if_exists='replace')
    df.to_sql('pred', conn, index=False, if_exists='replace')  # M
        
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
    fetch = PythonOperator(
        task_id='fetch',
        python_callable=fetch_data)

    machine_learning = PythonOperator(
        task_id='machine_learning',
        python_callable=train_pred)

    load = PythonOperator(
        task_id='load',
        python_callable=load_data)

    #proses untuk menjalankan di airflow
    fetch >> machine_learning >> load