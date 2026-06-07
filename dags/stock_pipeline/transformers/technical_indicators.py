import pandas as pd
import pandas_ta as ta

def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates technical indicators for stock prices grouped by ticker.
    
    :param df: pandas.DataFrame containing raw stock prices (date, ticker, close)
    :return: pandas.DataFrame containing stock metrics for the data warehouse mart
    """
    if df is None or df.empty:
        print("Empty DataFrame received for transformation.")
        return pd.DataFrame()
        
    # Ensure correct sorting for rolling metrics
    df_sorted = df.sort_values(by=['ticker', 'date']).copy()
    
    transformed_dfs = []
    
    for ticker, group in df_sorted.groupby('ticker'):
        # Create a clean copy to avoid SettingWithCopyWarning
        group = group.copy()
        
        # Ensure 'close' is numeric
        group['close'] = pd.to_numeric(group['close'], errors='coerce')
        
        # Calculate Moving Averages (min_periods=1 ensures we get values even for short histories)
        group['ma7'] = group['close'].rolling(window=7, min_periods=1).mean()
        group['ma30'] = group['close'].rolling(window=30, min_periods=1).mean()
        
        # Calculate Daily Returns
        group['daily_return'] = group['close'].pct_change()
        
        # Calculate Volatility (standard deviation of daily returns over a 30-day window)
        group['volatility'] = group['daily_return'].rolling(window=30, min_periods=1).std()
        
        # Calculate RSI 14 (Relative Strength Index)
        try:
            # pandas_ta requires at least 14 records to compute RSI
            if len(group) >= 14:
                rsi_series = ta.rsi(group['close'], length=14)
                group['rsi14'] = rsi_series
            else:
                group['rsi14'] = None
        except Exception as e:
            print(f"Warning: Failed to calculate RSI for {ticker}. Error: {str(e)}")
            group['rsi14'] = None
            
        transformed_dfs.append(group)
        
    # Combine back all groups
    combined_df = pd.concat(transformed_dfs, ignore_index=True)
    
    # Filter only relevant columns for the mart table
    mart_df = combined_df[['date', 'ticker', 'close', 'ma7', 'ma30', 'rsi14', 'daily_return', 'volatility']]
    
    return mart_df
