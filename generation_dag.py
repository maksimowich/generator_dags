from airflow import DAG
from airflow.operators.python import PythonOperator


def generate_data(**context):
    name = context['dag_run'].conf['name']
    rows = context['dag_run'].conf['rows']
    print(f"hello, {name} {type(name)}")
    print(f"row: {rows} {type(rows)}")


args = {
    'owner': 'amaksimovich',
    'depends_on_past': False,
    'retries': 1,
    'schedule_interval': None,
    'provide_context': True
}

dag = DAG(
    dag_id='generation_test',
    default_args=args,
    tags=["generator"],
)

t1 = PythonOperator(
    task_id="generate_data",
    python_callable=generate_data,
    dag=dag,
)
