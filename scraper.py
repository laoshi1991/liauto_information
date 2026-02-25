import akshare as ak
import pandas as pd
import os
from datetime import datetime

# Use relative path
FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'liauto_southbound.xlsx')

def run():
    print(f"[{datetime.now()}] Starting scraper...")
    
    try:
        # 1. Fetch Southbound Holdings Data (Connect Days only)
        print("Fetching Southbound Holdings...")
        df_hsgt = ak.stock_hsgt_individual_em(symbol="02015")
        df_hsgt['持股日期'] = pd.to_datetime(df_hsgt['持股日期'])
        
        # 2. Fetch HK Stock Daily Data (HK Trading Days)
        print("Fetching HK Daily Data...")
        df_hk = ak.stock_hk_daily(symbol="02015", adjust="")
        # df_hk columns: date, open, close, etc.
        # Ensure date is datetime
        df_hk['date'] = pd.to_datetime(df_hk['date'])
        
        # 3. Merge to align with HK Trading Days
        # Use df_hk as base (Left Join) to keep all HK trading days
        merged_df = pd.merge(df_hk[['date']], df_hsgt[['持股日期', '持股数量']], left_on='date', right_on='持股日期', how='left')
        
        # 4. Process Data
        # Sort by date
        merged_df = merged_df.sort_values('date')
        
        # Forward Fill '持股数量' for days where Connect was closed but HK was open
        merged_df['持股数量'] = merged_df['持股数量'].ffill()
        
        # Calculate Net Increase
        merged_df['diff'] = merged_df['持股数量'].diff()
        merged_df['南向当日净增持：万股'] = merged_df['diff'] / 10000
        merged_df['南向总持有：亿股'] = merged_df['持股数量'] / 100000000
        merged_df['日期'] = merged_df['date'].dt.strftime('%Y-%m-%d')
        
        # Select relevant columns
        final_df = merged_df[['日期', '南向当日净增持：万股', '南向总持有：亿股']].copy()
        
        # Remove rows where holdings are still NaN (e.g. before Southbound inception if any)
        final_df = final_df.dropna(subset=['南向总持有：亿股'])
        
        # 5. Save (Overwrite to ensure full history is aligned with HK calendar)
        # Check if we should append or overwrite. 
        # Since logic changed to "HK trading days", it's safer to overwrite to fill past gaps.
        final_df.to_excel(FILE_PATH, index=False)
        print(f"Excel updated successfully. Total rows: {len(final_df)}")
        print(f"Latest date: {final_df['日期'].iloc[-1]}")
            
    except Exception as e:
        print(f"Error in scraper: {e}")


if __name__ == "__main__":
    run()
