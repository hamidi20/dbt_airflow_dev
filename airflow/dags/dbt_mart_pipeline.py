from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dbt_mart_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    dim_customers = BashOperator(
        task_id="dim_customers",
        bash_command="docker exec dbt_runner dbt run --select dim_customers"
    )

    fact_orders = BashOperator(
        task_id="fact_orders",
        bash_command="docker exec dbt_runner dbt run --select fact_orders"
    )

    fact_payments = BashOperator(
        task_id="fact_payments",
        bash_command="docker exec dbt_runner dbt run --select fact_payments"
    )

    fact_sales_summary = BashOperator(
        task_id="fact_sales_summary",
        bash_command="docker exec dbt_runner dbt run --select fact_sales_summary"
    )

    dim_customers >> fact_orders >> fact_payments >> fact_sales_summary