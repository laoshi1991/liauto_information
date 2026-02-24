import akshare as ak
import pandas as pd
import os
from datetime import datetime

FILE_PATH = '/Users/caoshijie/Desktop/coding/理想web/理想南向持有.xlsx'

def run():
    print(f"[{datetime.now()}] Starting scraper...")
    
    # 1. Fetch data from Akshare
    try:
        df_ak = ak.stock_hsgt_individual_em(symbol="02015")
        # df_ak columns: ['持股日期', '当日收盘价', '当日涨跌幅', '持股数量', '持股市值', '持股数量占A股百分比', '持股市值变化-1日', '持股市值变化-5日', '持股市值变化-10日']
    except Exception as e:
        print(f"Error fetching data from Akshare: {e}")
        return

    # 2. Process Data
    # Sort by date
    df_ak['持股日期'] = pd.to_datetime(df_ak['持股日期'])
    df_ak = df_ak.sort_values('持股日期')
    
    # Calculate Net Increase (万股) and Total Holdings (亿股)
    # Net Increase = (Today - Yesterday) / 10000
    df_ak['diff'] = df_ak['持股数量'].diff()
    # For the first row, diff is NaN. We might leave it or fill it if we have context.
    # But since we are appending, we care about recent data.
    
    df_ak['南向当日净增持：万股'] = df_ak['diff'] / 10000
    df_ak['南向总持有：亿股'] = df_ak['持股数量'] / 100000000
    df_ak['日期'] = df_ak['持股日期'].dt.strftime('%Y-%m-%d')
    
    # Select relevant columns
    new_data = df_ak[['日期', '南向当日净增持：万股', '南向总持有：亿股']].copy()
    
    # 3. Read existing Excel to find last date
    if os.path.exists(FILE_PATH):
        try:
            existing_df = pd.read_excel(FILE_PATH)
            # Ensure Date column is datetime
            existing_df['日期'] = pd.to_datetime(existing_df['日期'])
            last_date = existing_df['日期'].max()
            print(f"Last date in Excel: {last_date}")
            
            # Filter new rows
            # new_data['日期'] is string, convert to datetime for comparison
            new_data['dt'] = pd.to_datetime(new_data['日期'])
            rows_to_add = new_data[new_data['dt'] > last_date].copy()
            
            if rows_to_add.empty:
                print("No new data to append.")
                return

            print(f"Found {len(rows_to_add)} new rows.")
            
            # Recalculate the first row's Net Increase if needed?
            # actually, df_ak['diff'] uses previous row in df_ak. 
            # If df_ak covers the transition from last_date to new_date, it's correct.
            # Assuming df_ak history overlaps with Excel history.
            
            # Format for Excel
            # Drop temp column
            rows_to_add = rows_to_add.drop(columns=['dt'])
            
            # Append
            # We need to write back to Excel. 
            # To preserve existing format, we concat and write.
            # existing_df needs to be converted back to match format if needed, but pandas handles it.
            existing_df['日期'] = existing_df['日期'].dt.strftime('%Y-%m-%d')
            
            updated_df = pd.concat([existing_df, rows_to_add], ignore_index=True)
            
            # Save
            updated_df.to_excel(FILE_PATH, index=False)
            print("Excel updated successfully.")
            
        except Exception as e:
            print(f"Error reading/writing Excel: {e}")
    else:
        print("Excel file not found. Creating new one.")
        new_data.to_excel(FILE_PATH, index=False)

if __name__ == "__main__":
    run()
