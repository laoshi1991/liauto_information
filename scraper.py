import akshare as ak
import pandas as pd
import os
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Use relative path
FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'liauto_southbound.xlsx')

def send_email(new_data_df):
    sender_email = os.environ.get('MAIL_USERNAME')
    sender_password = os.environ.get('MAIL_PASSWORD')
    recipient_email = 'iamterrencecao@gmail.com'

    if not sender_email or not sender_password:
        print("Email credentials not found. Skipping email.")
        return

    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        
        # Use date from the first row of new_data_df (which is just one row here)
        if not new_data_df.empty:
            report_date = new_data_df.iloc[0]['日期']
            msg['Subject'] = f"理想汽车数据日报 - {report_date}"
        else:
             msg['Subject'] = f"理想汽车数据日报 - 无数据 - {datetime.now().strftime('%Y-%m-%d')}"

        # Build HTML content
        html_content = """
        <html>
        <head>
            <style>
                table { border-collapse: collapse; width: 100%; }
                th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
                th { background-color: #f2f2f2; }
                tr:nth-child(even) { background-color: #f9f9f9; }
            </style>
        </head>
        <body>
            <h2>最新一天的南向持股数据</h2>
            <table>
                <tr>
                    <th>日期</th>
                    <th>南向当日净增持(万股)</th>
                    <th>南向总持有(亿股)</th>
                    <th>收盘价(HKD)</th>
                    <th>当日涨跌幅(%)</th>
                </tr>
        """
        
        for _, row in new_data_df.iterrows():
            html_content += f"""
                <tr>
                    <td>{row['日期']}</td>
                    <td>{row['南向当日净增持：万股']:.2f}</td>
                    <td>{row['南向总持有：亿股']:.4f}</td>
                    <td>{row['收盘价'] if pd.notnull(row['收盘价']) else '-'}</td>
                    <td>{row['当日涨跌幅'] if pd.notnull(row['当日涨跌幅']) else '-'}</td>
                </tr>
            """
            
        html_content += """
            </table>
            <p>更多详情请访问: <a href="https://liauto.personaltools.fun/">理想汽车数据看板</a></p>
        </body>
        </html>
        """

        msg.attach(MIMEText(html_content, 'html'))

        # Send email
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()
        print(f"Email sent successfully to {recipient_email}")

    except Exception as e:
        print(f"Failed to send email: {e}")

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
        
        # Always send email with the latest row (reporting mode)
        if not final_df.empty:
            latest_row = final_df.iloc[[-1]] # Keep as DataFrame
            print("Sending email with latest data...")
            send_email(latest_row)
        else:
            print("No data available to report.")

        # 5. Save (Overwrite to ensure consistency)
        final_df.to_excel(FILE_PATH, index=False)
        print(f"Excel updated successfully. Total rows: {len(final_df)}")
        print(f"Date range: {final_df['日期'].min()} to {final_df['日期'].max()}")
        print(final_df.tail())
            
    except Exception as e:
        print(f"Error in scraper: {e}")

if __name__ == "__main__":
    run()
