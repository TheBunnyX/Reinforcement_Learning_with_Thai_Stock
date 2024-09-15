from settrade_v2 import MarketRep
import datetime
import pandas as pd

# Initialize Investor
marketrep = MarketRep(
    app_id="",
    app_secret="",
    broker_id="SANDBOX",
    app_code="SANDBOX",
    is_auto_queue=False
)

# Initialize MarketData
market_data = marketrep.MarketData()  # Assuming MarketData needs Investor instance

# List of stock symbols
symbols = ["EA", "BBL", "KTB", "SCB", "KBANK", "BDMS", "PTT", "PTTEP", "ADVANC", "CPALL",
           "GULF", "DELTA", "TTB", "IVL", "INTUCH", "VGI", "GPSC", "JMT", "AOT", "BTS",
           "TOP", "CPF", "BGRIM", "TISCO", "MTC", "BANPU", "KTC", "BCP", "KKP", "HMPRO",
           "TIDLOR","AJA"]

# Initialize list to hold DataFrames
dfs = []

# Loop through each symbol and get data
for symbol in symbols:
    try:
        res = market_data.get_candlestick(
            symbol=symbol,
            interval="1d",
            start="2021-08-01T00:00:00",
            end="2024-08-09T23:59:00",
            normalized=True
        )
        
        # Extract time, open, high, low, close, and volume data
        time_data = []
        open_data = []
        high_data = []
        low_data = []
        close_data = []
        volume_data = []

        if res:
            for k, v in res.items():
                if k == 'time':
                    time_data = [datetime.datetime.fromtimestamp(int(n)).strftime('%Y-%m-%d %H:%M:%S') for n in v]
                elif k == 'open':
                    open_data = v
                elif k == 'high':
                    high_data = v
                elif k == 'low':
                    low_data = v
                elif k == 'close':
                    close_data = v
                elif k == 'volume':
                    volume_data = v

        data_length = min(len(time_data), len(open_data), len(high_data), len(low_data), len(close_data), len(volume_data))
        time_data = time_data[:data_length]
        open_data = open_data[:data_length]
        high_data = high_data[:data_length]
        low_data = low_data[:data_length]
        close_data = close_data[:data_length]
        volume_data = volume_data[:data_length]

        # Create DataFrame for current symbol
        df = pd.DataFrame({
            'date': time_data,
            'open': open_data,
            'high': high_data,
            'low': low_data,
            'close': close_data,
            'volume': volume_data
        })

        df['date'] = pd.to_datetime(df['date'])
        df['tic'] = symbol
        df['day'] = df['date'].dt.dayofweek
        df['day'] = df['day'].apply(lambda x: x if x < 5 else None)

        dfs.append(df)
        print(symbol," is finished")

    except Exception as e:
        print(f"An error occurred for symbol {symbol}: {e}")

# Combine all DataFrames into one
combined_df = pd.concat(dfs, ignore_index=False)

# Sort by 'time' and 'tic' columns
combined_df = combined_df.sort_values(by=['date', 'tic'])

# Save combined DataFrame to CSV
combined_df.to_csv('combined_stocks_data.csv', index=False)

# Split data
total_rows = len(combined_df)

# Calculate split index
split_index = total_rows // 2

# Split DataFrame into two parts
df1 = combined_df.iloc[:split_index]
df2 = combined_df.iloc[split_index:]

# Save split DataFrames to separate CSV files
df1.to_csv('stocks_data_train.csv', index=False)
df2.to_csv('stocks_data_test.csv', index=False)

print("Data saved to stocks_data_train.csv and stocks_data_test.csv")
