import akshare as ak
import pandas as pd
import os
from datetime import datetime

# Use relative path
FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'liauto_southbound.xlsx')

def run():
    print(f"[{datetime.now()}] Starting scraper...")
    
    try:
        # Define date range
        # Always fetch from start date to today to ensure we catch any corrections or late updates
        start_date = '2025-10-24'
        end_date = datetime.now().strftime('%Y-%m-%d')
        
        # 1. Fetch Southbound Holdings Data
        print("Fetching Southbound Holdings...")
        df_hsgt = ak.stock_hsgt_individual_em(symbol="02015")
        df_hsgt['持股日期'] = pd.to_datetime(df_hsgt['持股日期'])
        
        # 2. Fetch HK Stock Daily Data
        print("Fetching HK Daily Data...")
        df_hk = ak.stock_hk_daily(symbol="02015", adjust="")
        df_hk['date'] = pd.to_datetime(df_hk['date'])
        
        # Filter date range
        mask_hk = (df_hk['date'] >= start_date) & (df_hk['date'] <= end_date)
        df_hk_filtered = df_hk.loc[mask_hk].copy()
        
        mask_hsgt = (df_hsgt['持股日期'] >= start_date) & (df_hsgt['持股日期'] <= end_date)
        df_hsgt_filtered = df_hsgt.loc[mask_hsgt].copy()
        
        # 3. Merge Data (Outer Join to capture updates from either source)
        # Requirement 1: If both missing (no date in either), no row is created.
        merged_df = pd.merge(
            df_hk_filtered[['date', 'open', 'high', 'low', 'close']], 
            df_hsgt_filtered[['持股日期', '持股数量']], 
            left_on='date', 
            right_on='持股日期', 
            how='outer'
        )
        
        # Coalesce dates (fill 'date' with '持股日期' if 'date' is NaN)
        merged_df['date'] = merged_df['date'].fillna(merged_df['持股日期'])
        
        # 4. Process Data
        merged_df = merged_df.sort_values('date')
        
        # Calculate daily change percentage: (close - prev_close) / prev_close * 100
        merged_df['prev_close'] = merged_df['close'].shift(1)
        merged_df['当日涨跌幅'] = ((merged_df['close'] - merged_df['prev_close']) / merged_df['prev_close'] * 100).round(2)
        # Note: If Price is missing (NaN), change percent will be NaN, which is correct (Req 3)
        
        # Requirement 2: If Stock Price exists but Southbound missing -> ffill Holdings
        # Requirement 3: If Price missing but Southbound exists -> Keep Price NaN
        merged_df['持股数量'] = merged_df['持股数量'].ffill()
        
        # Calculate Net Increase
        merged_df['diff'] = merged_df['持股数量'].diff()
        merged_df['diff'] = merged_df['diff'].fillna(0) # First day diff is 0
        
        merged_df['南向当日净增持：万股'] = merged_df['diff'] / 10000
        merged_df['南向总持有：亿股'] = merged_df['持股数量'] / 100000000
        
        # Format columns
        merged_df['日期'] = merged_df['date'].dt.strftime('%Y-%m-%d')
        merged_df['开盘价'] = merged_df['open']
        merged_df['最高价'] = merged_df['high']
        merged_df['最低价'] = merged_df['low']
        merged_df['收盘价'] = merged_df['close']
        
        # Select final columns
        final_df = merged_df[[
            '日期', 
            '南向当日净增持：万股', 
            '南向总持有：亿股',
            '开盘价',
            '最高价',
            '最低价',
            '收盘价',
            '当日涨跌幅'
        ]].copy()
        
        # 5. Save (Overwrite to ensure consistency)
        final_df.to_excel(FILE_PATH, index=False)
        print(f"Excel updated successfully. Total rows: {len(final_df)}")
        print(f"Date range: {final_df['日期'].min()} to {final_df['日期'].max()}")
        print(final_df.tail())
            
    except Exception as e:
        print(f"Error in scraper: {e}")

if __name__ == "__main__":
    run()
