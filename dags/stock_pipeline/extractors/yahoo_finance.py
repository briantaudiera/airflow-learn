import pandas as pd
import yfinance as yf
from datetime import datetime

def fetch_stock_data(tickers, start_date, end_date):
    """
    Fetches historical stock data from Yahoo Finance for a list of tickers.
    
    :param tickers: List of ticker strings (e.g. ['BBCA.JK', 'TLKM.JK'])
    :param start_date: Start date string (YYYY-MM-DD)
    :param end_date: End date string (YYYY-MM-DD)
    :return: pandas.DataFrame containing stock data
    """
    all_data = []
    
    for ticker in tickers:
        try:
            print(f"Fetching data for {ticker} from {start_date} to {end_date}...")
            # yf.download is standard for retrieving historical data
            df = yf.download(ticker, start=start_date, end=end_date)
            
            if df.empty:
                print(f"Warning: No data found for ticker {ticker} in range {start_date} to {end_date}")
                continue
                
            # Reset index to turn 'Date' into a column
            df = df.reset_index()
            
            # Clean multi-index columns if they occur
            df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
            
            # Standardize columns to lowercase with underscores
            df.columns = [str(col).strip().lower().replace(' ', '_') for col in df.columns]
            
            # Add ticker column
            df['ticker'] = ticker
            
            # Ensure critical columns exist
            required_cols = ['date', 'ticker', 'open', 'high', 'low', 'close', 'adj_close', 'volume']
            for col in required_cols:
                if col not in df.columns:
                    # If adj_close doesn't exist, fallback to close
                    if col == 'adj_close' and 'close' in df.columns:
                        df['adj_close'] = df['close']
                    else:
                        df[col] = None
                        
            # Filter and order columns
            df = df[required_cols]
            all_data.append(df)
            
        except Exception as e:
            print(f"Error fetching data for ticker {ticker}: {str(e)}")
            
    if not all_data:
        print("No stock data was fetched successfully.")
        return pd.DataFrame()
        
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # Convert 'date' to YYYY-MM-DD string format
    combined_df['date'] = pd.to_datetime(combined_df['date']).dt.strftime('%Y-%m-%d')
    
    # Ensure numeric columns are properly converted and NaN values are handled
    numeric_cols = ['open', 'high', 'low', 'close', 'adj_close', 'volume']
    for col in numeric_cols:
        combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')
        
    return combined_df
