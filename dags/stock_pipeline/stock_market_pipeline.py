from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

# Import custom modules
from stock_pipeline.extractors.yahoo_finance import fetch_stock_data
from stock_pipeline.validators.data_quality import validate_stock_data
from stock_pipeline.transformers.technical_indicators import calculate_indicators
from stock_pipeline.loaders.postgres_loader import upsert_raw_prices, upsert_mart_metrics

# Configurable list of IDX tickers
TICKERS = ['BBCA.JK', 'BBRI.JK', 'TLKM.JK', 'GOTO.JK', 'ASII.JK']

def extract_task_func(**context):
    """
    Extracts stock data from Yahoo Finance.
    For daily runs, fetches the last 60 days to ensure enough history 
    for calculating rolling technical indicators (like MA30 and RSI14).
    """
    params = context.get('params', {})
    start_date = params.get('start_date')
    end_date = params.get('end_date')
    
    if start_date and end_date:
        # Manual backfill mode
        print(f"Manual backfill requested for range: {start_date} to {end_date}")
    else:
        # Daily incremental mode
        # ds is the execution date (YYYY-MM-DD)
        ds = context['ds']
        run_date = datetime.strptime(ds, '%Y-%m-%d')
        # We need at least 30-40 days of history to compute MA30 and RSI14 correctly.
        # Fetching 60 days is safe, fast, and covers weekends/holidays.
        start_date = (run_date - timedelta(days=60)).strftime('%Y-%m-%d')
        end_date = (run_date + timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"Daily execution. Fetching history from {start_date} to {end_date} to calculate indicators.")
        
    df = fetch_stock_data(TICKERS, start_date, end_date)
    
    if df.empty:
        raise ValueError("No stock data returned from API.")
        
    # Push to XCom as JSON string
    return df.to_json(orient='records')

def validate_task_func(**context):
    """Validates the extracted raw stock prices."""
    ti = context['ti']
    json_data = ti.xcom_pull(task_ids='extract_stock_data')
    
    if not json_data:
        raise ValueError("No data received from extract task.")
        
    df = pd.read_json(json_data)
    # Perform validation
    validate_stock_data(df)
    return json_data

def transform_task_func(**context):
    """Transforms raw prices into technical indicators."""
    ti = context['ti']
    json_data = ti.xcom_pull(task_ids='validate_data')
    
    if not json_data:
        raise ValueError("No validated data received.")
        
    df = pd.read_json(json_data)
    df_transformed = calculate_indicators(df)
    
    # Push transformed data to XCom
    return df_transformed.to_json(orient='records')

def load_task_func(**context):
    """Loads raw prices and metrics into the Data Warehouse."""
    ti = context['ti']
    
    # 1. Pull data
    raw_json = ti.xcom_pull(task_ids='validate_data')
    mart_json = ti.xcom_pull(task_ids='transform_data')
    
    if not raw_json or not mart_json:
        raise ValueError("Missing raw or transformed data for loading.")
        
    df_raw = pd.read_json(raw_json)
    df_mart = pd.read_json(mart_json)
    
    # For daily runs, we can restrict writing to mart only for the execution date (ds),
    # or write the entire calculated history. Writing the calculated window (e.g. 60 days)
    # is fast and ensures any retrospective historical adjustments are captured.
    # We will write the full fetched/calculated window here.
    
    # 2. Upsert raw prices
    upsert_raw_prices(df_raw, conn_id='postgres_default')
    
    # 3. Upsert mart metrics
    upsert_mart_metrics(df_mart, conn_id='postgres_default')
    
    print("ETL pipeline load task completed successfully.")

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='stock_market_etl_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['etl', 'finance', 'stock_pipeline'],
    params={
        "start_date": Param(
            default="",
            type="string",
            description="Start date for backfill (YYYY-MM-DD). Leave empty for daily run."
        ),
        "end_date": Param(
            default="",
            type="string",
            description="End date for backfill (YYYY-MM-DD). Leave empty for daily run."
        ),
    },
    doc_md="""
    ### Stock Market ETL Pipeline
    This pipeline extracts stock market data from Yahoo Finance API for major Indonesian (IDX) tickers,
    validates data quality, calculates rolling technical indicators (MA7, MA30, RSI14, daily returns, volatility),
    and loads them into the PostgreSQL Data Warehouse.
    """
) as dag:

    extract_task = PythonOperator(
        task_id='extract_stock_data',
        python_callable=extract_task_func
    )

    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_task_func
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_task_func
    )

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_task_func
    )

    # DAG Dependency Flow
    extract_task >> validate_task >> transform_task >> load_task
