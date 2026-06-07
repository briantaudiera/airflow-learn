import pandas as pd

def validate_stock_data(df: pd.DataFrame) -> bool:
    """
    Performs data quality checks on the fetched stock data.
    Raises ValueError if data quality criteria are not met.
    
    :param df: pandas.DataFrame containing stock data
    :return: True if validation passes
    """
    if df is None or df.empty:
        raise ValueError("Data validation failed: DataFrame is empty or None.")
        
    # 1. Check for required columns
    required_cols = ['date', 'ticker', 'open', 'high', 'low', 'close', 'adj_close', 'volume']
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Data validation failed: Missing required column '{col}'")
            
    # 2. Check for nulls in critical columns
    critical_cols = ['date', 'ticker', 'close']
    for col in critical_cols:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            raise ValueError(f"Data validation failed: Column '{col}' contains {null_count} null value(s)")
            
    # 3. Check for duplicate records (same date and ticker)
    duplicates = df.duplicated(subset=['date', 'ticker']).sum()
    if duplicates > 0:
        raise ValueError(f"Data validation failed: Found {duplicates} duplicate record(s) for (date, ticker)")
        
    # 4. Check for logic issues (e.g. negative prices or negative volume)
    negative_close = (df['close'] < 0).sum()
    if negative_close > 0:
        raise ValueError(f"Data validation failed: Found {negative_close} row(s) with negative close price")
        
    negative_volume = (df['volume'] < 0).sum()
    if negative_volume > 0:
        raise ValueError(f"Data validation failed: Found {negative_volume} row(s) with negative volume")
        
    print(f"Data quality checks passed. Total validated records: {len(df)}")
    return True
