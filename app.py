from flask import Flask, render_template, jsonify, send_file
import pandas as pd
import os
from apscheduler.schedulers.background import BackgroundScheduler
import datetime
import scraper

app = Flask(__name__)
# Use relative path for Vercel/Cloud compatibility
FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'liauto_southbound.xlsx')

def read_data():
    if not os.path.exists(FILE_PATH):
        return []
    try:
        df = pd.read_excel(FILE_PATH)
        # Handle datetime objects
        if pd.api.types.is_datetime64_any_dtype(df['日期']):
            df['日期'] = df['日期'].dt.strftime('%Y-%m-%d')
        else:
            df['日期'] = df['日期'].astype(str)
            
        # Convert NaN to None or 0
        df = df.where(pd.notnull(df), None)
        return df.to_dict(orient='records')
    except Exception as e:
        print(f"Error reading data: {e}")
        return []

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def get_data():
    data = read_data()
    return jsonify(data)

@app.route('/api/debug')
def debug_info():
    """Endpoint to check file system and environment on Vercel"""
    import platform
    info = {
        'cwd': os.getcwd(),
        'files_in_cwd': os.listdir('.'),
        'file_path': FILE_PATH,
        'file_exists': os.path.exists(FILE_PATH),
        'python_version': platform.python_version(),
        'platform': platform.platform(),
        'read_test': 'Not attempted',
        'read_error': None
    }
    
    # Try reading the file
    try:
        if os.path.exists(FILE_PATH):
            df = pd.read_excel(FILE_PATH)
            info['read_test'] = 'Success'
            info['columns'] = list(df.columns)
            info['row_count'] = len(df)
            info['sample_data'] = df.head(2).to_dict(orient='records')
        else:
            info['read_test'] = 'File not found'
    except Exception as e:
        info['read_test'] = 'Failed'
        info['read_error'] = str(e)
        import traceback
        info['traceback'] = traceback.format_exc()

    return jsonify(info)

@app.route('/api/export')
def export_data():
    if not os.path.exists(FILE_PATH):
        return "Data file not found", 404
    return send_file(FILE_PATH, as_attachment=True, download_name='liauto_southbound.xlsx')

def scheduled_job():
    print(f"Job run at {datetime.datetime.now()}")
    try:
        scraper.run()
    except Exception as e:
        print(f"Scraper failed: {e}")

if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    # Schedule to run at 20:30 every trading day (Mon-Fri)
    # Note: timezone might be an issue. Assuming system time is local.
    scheduler.add_job(scheduled_job, 'cron', day_of_week='mon-fri', hour=20, minute=30)
    scheduler.start()
    
    app.run(host='0.0.0.0', port=5001, debug=True, use_reloader=False) 
    # use_reloader=False to avoid running scheduler twice
