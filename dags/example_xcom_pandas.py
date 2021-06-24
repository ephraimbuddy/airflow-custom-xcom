from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np

def generate_pd(ti):
    df = pd.DataFrame(np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),
                   columns=['a', 'b', 'c'])
    ti.xcom_push(key="df_data", value=df)

def xcom_not_df(ti):
    ti.xcom_pull(task_ids='push_xcom_df')
    return {'data': [1,2,3,4,5]}

def no_xcom(ti):
    value=ti.xcom_pull(task_ids='push_xcom_not_df')
    print('Pulled xcom: ', value)


with DAG(
    "example_xcom_pandas",
    schedule_interval=None,
    start_date=days_ago(2)
) as dag:

    task1 = PythonOperator(
        task_id="push_xcom_df",
        python_callable=generate_pd
    )
    task2 = PythonOperator(
        task_id="push_xcom_not_df",
        python_callable=xcom_not_df
    )
    task3 = PythonOperator(
        task_id = "no_xcom",
        python_callable=no_xcom
    )

    task1 >> task2 >> task3
