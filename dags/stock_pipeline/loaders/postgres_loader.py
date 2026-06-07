import pandas as pd
import numpy as np
from airflow.providers.postgres.hooks.postgres import PostgresHook

def _clean_records(df: pd.DataFrame) -> list:
    """
    Cleans DataFrame values, replacing NaN/NaT with None and converting
    numpy types to native Python types for database insertion.
    """
    records = []
    for row in df.itertuples(index=False):
        cleaned_row = []
        for val in row:
            if pd.isna(val):
                cleaned_row.append(None)
            elif isinstance(val, (np.integer, np.int64)):
                cleaned_row.append(int(val))
            elif isinstance(val, (np.floating, np.float64)):
                cleaned_row.append(float(val))
            else:
                cleaned_row.append(val)
        records.append(tuple(cleaned_row))
    return records

def upsert_raw_prices(df: pd.DataFrame, conn_id: str = 'postgres_default'):
    """
    Upserts raw stock prices into the raw.stock_prices table.
    """
    if df is None or df.empty:
        print("No raw stock prices to load.")
        return
        
    records = _clean_records(df)
    
    query = """
    INSERT INTO raw.stock_prices (date, ticker, open, high, low, close, adj_close, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (date, ticker) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        adj_close = EXCLUDED.adj_close,
        volume = EXCLUDED.volume,
        ingested_at = NOW();
    """
    
    print(f"Upserting {len(records)} records into raw.stock_prices...")
    hook = PostgresHook(postgres_conn_id=conn_id)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(query, records)
        conn.commit()
    print("Successfully loaded raw prices.")

def upsert_mart_metrics(df: pd.DataFrame, conn_id: str = 'postgres_default'):
    """
    Upserts stock metrics into the mart.stock_metrics table.
    """
    if df is None or df.empty:
        print("No stock metrics to load.")
        return
        
    records = _clean_records(df)
    
    query = """
    INSERT INTO mart.stock_metrics (date, ticker, close, ma7, ma30, rsi14, daily_return, volatility)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (date, ticker) DO UPDATE SET
        close = EXCLUDED.close,
        ma7 = EXCLUDED.ma7,
        ma30 = EXCLUDED.ma30,
        rsi14 = EXCLUDED.rsi14,
        daily_return = EXCLUDED.daily_return,
        volatility = EXCLUDED.volatility,
        processed_at = NOW();
    """
    
    print(f"Upserting {len(records)} records into mart.stock_metrics...")
    hook = PostgresHook(postgres_conn_id=conn_id)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(query, records)
        conn.commit()
    print("Successfully loaded mart metrics.")
