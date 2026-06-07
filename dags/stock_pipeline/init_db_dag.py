import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def run_init_sql():
    """Reads and executes the SQL schema creation file."""
    dag_dir = os.path.dirname(os.path.abspath(__file__))
    sql_path = os.path.join(dag_dir, 'sql', 'init_schema.sql')
    
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"SQL file not found at: {sql_path}")
        
    with open(sql_path, 'r') as f:
        sql = f.read()
        
    print(f"Connecting to database via connection 'postgres_default'...")
    hook = PostgresHook(postgres_conn_id='postgres_default')
    hook.run(sql)
    print("Database schema successfully initialized.")

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='init_stock_dw_schema',
    default_args=default_args,
    schedule=None,  # Run manually
    catchup=False,
    tags=['setup', 'stock_pipeline'],
    doc_md="""
    ### Init Stock DW Schema DAG
    This DAG initializes the raw and mart schemas and tables in the PostgreSQL database.
    It should be run once before starting the stock market ETL pipeline.
    """
) as dag:

    init_db_task = PythonOperator(
        task_id='initialize_database',
        python_callable=run_init_sql
    )
