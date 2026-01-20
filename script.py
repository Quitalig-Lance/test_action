import pandas as pd
import requests
import zipfile
import io
import os
import re
import logging
import gc
import shutil
from sqlalchemy import create_engine, text

# --- STEP 1: ROBUST LOGGING SETUP ---
log_format = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    handlers=[
        logging.FileHandler("sync_history.log", encoding='utf-8'),
        logging.StreamHandler() # Also prints to console
    ]
)

# --- CONFIGURATION ---
DB_URL = os.getenv('DB_URL', '')
NTA_URL = "https://www.houjin-bangou.nta.go.jp/download/zenken/index.html"
GBIZ_URL = "https://info.gbiz.go.jp/hojin/Download"

print(DB_URL)

TABLE_MAP = {
    'nta_full': 'corporate_basic_information_nta',
    'gbiz_7':   'corporate_basic_information_gbizinfo',
    'gbiz_8':   'notification_certification_information_gbizinfo',
    'gbiz_9':   'award_information_gbizinfo',
    'gbiz_10':  'subsidy_information_gbizinfo',
    'gbiz_11':  'procurement_information_gbizinfo',
    'gbiz_12':  'patent_information_gbizinfo',
    'gbiz_13':  'financial_information_gbizinfo',
    'gbiz_14':  'workplace_information_gbizinfo'
}

UNIQUE_TABLES = [
    'corporate_basic_information_nta', 
    'corporate_basic_information_gbizinfo', 
    'workplace_information_gbizinfo'
]

sync_results = {}
session = requests.Session()

def process_sync(chunk_iterator, table_name, engine):
    """Memory-safe chunk processor."""
    first_chunk = True
    total_rows = 0
    
    for df in chunk_iterator:
        df.columns = [c.lower().strip() for c in df.columns]
        df = df.rename(columns={'法人番号': 'corporate_number', '更新年月日': 'update_date', 'sequence': 'sequencenumber'})
        df['corporate_number'] = df['corporate_number'].astype(str)

        mode = 'replace' if first_chunk else 'append'
        df.to_sql('temp_import', engine, if_exists=mode, index=False, method='multi', chunksize=5000)
        
        first_chunk = False
        total_rows += len(df)
        print(f"   -> Buffered {total_rows} rows...")
        del df
        gc.collect()

    # Final SQL Move
    with engine.begin() as conn:
        cols_query = conn.execute(text("SELECT * FROM temp_import LIMIT 0"))
        col_str = ", ".join([f'"{c}"' for c in cols_query.keys()])
        
        if table_name in UNIQUE_TABLES:
            query = f"""
                INSERT INTO {table_name} ({col_str}) 
                SELECT {col_str} FROM temp_import 
                ON CONFLICT (corporate_number) 
                DO UPDATE SET update_date = EXCLUDED.update_date;
            """
        else:
            query = f"""
                INSERT INTO {table_name} ({col_str})
                SELECT {col_str} FROM temp_import t
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table_name} m
                    WHERE m.corporate_number = t.corporate_number
                    AND m.update_date = t.update_date
                );
            """
        res = conn.execute(text(query))
        sync_results[table_name] = f"SUCCESS ({res.rowcount} updated/added)"
        conn.execute(text("DROP TABLE IF EXISTS temp_import;"))

def run_nta_sync(engine):
    print("\n[1/2] Starting NTA Sync...")
    temp_zip = "nta_temp.zip"
    try:
        # Get Token
        res = session.get(NTA_URL, timeout=30)
        token = re.search(r'value="([^"]+)"', res.text).group(1)
        
        # Stream Download to Disk (CRITICAL FOR 8GB RAM)
        print("   -> Downloading NTA Zip to disk (streaming)...")
        payload = {'jp.go.nta.houjin_bangou.framework.web.common.CNSFWTokenProcessor.request.token': token, 'event': 'download', 'selDlFileNo': '26067'}
        
        with session.post(NTA_URL, data=payload, stream=True) as r:
            with open(temp_zip, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        
        print("   -> Download complete. Processing CSV in chunks...")
        nta_columns = ["sequence", "corporate_number", "process", "correct", "update_date", "change_date", "name", "name_image_id", "kind", "prefecture_name", "city_name", "street_number", "address_image_id", "prefecture_code", "city_code", "post_code", "address_outside", "address_outside_image_id", "close_date", "close_cause", "successor_corporate_number", "change_cause", "assignment_date", "latest", "en_name", "en_prefecture_name", "en_city_name", "en_address_outside", "furigana", "hi_notation"]

        with zipfile.ZipFile(temp_zip) as z:
            csv_name = [n for n in z.namelist() if n.endswith('.csv')][0]
            with z.open(csv_name) as f:
                reader = pd.read_csv(f, encoding='shift_jis', header=None, names=nta_columns, dtype={"corporate_number": str}, chunksize=30000)
                process_sync(reader, TABLE_MAP['nta_full'], engine)

    except Exception as e:
        logging.error(f"NTA Sync Failed: {e}")
    finally:
        if os.path.exists(temp_zip):
            os.remove(temp_zip)

def run_gbiz_sync(engine):
    print("\n[2/2] Starting gBizInfo Sync...")
    for fid in range(7, 15):
        t_name = TABLE_MAP[f'gbiz_{fid}']
        print(f"   -> Processing File ID: {fid} ({t_name})")
        try:
            res = session.post(GBIZ_URL, data={'downfile': str(fid), 'downtype': 'zip', 'downenc': 'SJIS'}, stream=True)
            with zipfile.ZipFile(io.BytesIO(res.content)) as z:
                for filename in z.namelist():
                    with z.open(filename) as f:
                        reader = pd.read_csv(f, encoding='shift_jis', chunksize=50000)
                        process_sync(reader, t_name, engine)
            gc.collect()
        except Exception as e:
            logging.error(f"gBiz {fid} Failed: {e}")

def main():
    print("--- CORPORATE DATA SYNC START ---")
    try:
        engine = create_engine(DB_URL)
        # Check connection immediately
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("DB Connection: OK")
        
        run_nta_sync(engine)
        run_gbiz_sync(engine)
        
    except Exception as e:
        print(f"FATAL ERROR: Could not connect to DB or initialize: {e}")
    
    print("\n" + "="*50)
    print("FINAL SUMMARY REPORT")
    for table, status in sync_results.items():
        print(f"{table:<40} | {status}")
    print("="*50)

if __name__ == "__main__":
    main()