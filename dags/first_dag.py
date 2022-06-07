from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def first_execution(**context):
    print("FIRST EXECUTION")
    context["ti"].xcom_push(key="mkey", value="first_function_execution")
    # return "FIRST EXECUTION"


def another_execution(**context):
    variable = context.get("ti").xcom_pull(key="mkey")
    print(f"Second Function Execution with value :{variable}")
    return "Hello World: " + variable


with DAG(
    dag_id="first_dag",
    schedule_interval="@hourly",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "start_date": datetime(2022, 6, 6),
    },
    catchup=False,
) as f:

    first_execution_ = PythonOperator(
        task_id="first_dag",
        python_callable=first_execution,
        provide_context=True,
        op_kwargs={"name": "ciku"},
    )

    second_execution_ = PythonOperator(
        task_id="second_dag",
        python_callable=another_execution,
        provide_context=True,
    )

first_execution_ >> second_execution_
