from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

dag =  DAG(
    "example_xcom",
    schedule_interval=None,
    start_date=days_ago(2)
) 

def pushxcom(ti):
    # returning a value pushes xcom
    return [0,2,4,6]

def pushxcom_explicit(ti):
    # Explicitly push an xcom
    ti.xcom_push(key="explicit", value=[6,5,4,3])

def pull_xcom(ti):
    # Pull an xcom
    xcom = ti.xcom_pull(task_ids='push_xcom')
    print("Xcom pulled: ", xcom)

with dag:
    task1 = PythonOperator(
        task_id="push_xcom",
        python_callable=pushxcom
    )
    task2 = PythonOperator(
        task_id="push_xcom_explicit",
        python_callable=pushxcom_explicit
    )
    task3 = PythonOperator(
        task_id="pull_xcom",
        python_callable=pull_xcom
    )

    task1 >> task2 >> task3