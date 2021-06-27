from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task

dag =  DAG(
    "example_taskflow_xcom",
    schedule_interval=None,
    start_date=days_ago(2)
) 

@task(dag=dag)
def push_xcom():
    return [0, 2, 4, 6]  # Pushes XCOM


@task(dag=dag)
def pull_xcom(data):  # Pulls the pushed XCOM
    # If we use return statement, we push an xcom
    print(data)


mydata = push_xcom()
pull_xcom(mydata) # The pull