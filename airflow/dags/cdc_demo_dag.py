from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="postgres_cdc_demo",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "owner": "data-team",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    echo_message = BashOperator(
        task_id="echo_demo_message",
        bash_command="echo 'Airflow is orchestrating the CDC demo workflow'",
    )

    seed_data = BashOperator(
        task_id="seed_demo_data",
        bash_command="docker exec postgres psql -U postgres -d mydb -c \"INSERT INTO public.customers (name, email) VALUES ('Airflow Demo', 'airflow@example.com') ON CONFLICT (email) DO NOTHING;\"",
    )

    echo_message >> seed_data
