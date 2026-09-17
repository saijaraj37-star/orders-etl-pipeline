from __future__ import annotations
from datetime import datetime,timedelta
import pandas as pd
from sqlalchemy import create_engine 
from airflow.decorators import dag,task
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from google.oauth2 import service_account
from airflow.models import Variable
import io

SERVICE_ACCOUNT_FILE = "/opt/airflow/config/credentials/service-account.json"
DRIVE_FOLDER_ID = "1anKRcBB9r5gIYaF0WAEeeWdGjJG6qotm"
SOURCE_FILE_NAME = "raw_orders.csv"            # Drive mein rakhi CSV ka exact naam
SCOPES = ["https://www.googleapis.com/auth/drive"]
LOCAL_RAW_PATH = "/tmp/data_raw.csv"
RAW_CSV_PATH = "/opt/airflow/dags/data/raw_orders.csv"
# POSTGRES_CONN = "postgresql+psycopg2://airflow:airflow@postgres/airflow"
POSTGRES_CONN = "postgresql://postgres.uqmomapfsbcaqisevpvc:EWWNXS2nB8wKwXur@aws-0-ap-northeast-2.pooler.supabase.com:5432/postgres"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"
# SUPABASE_CONN = "postgresql://postgres:EWWNXS2nB8wKwXur@db.uqmomapfsbcaqisevpvc.supabase.co:5432/postgres"

TABLE_NAME = "orders"

def get_drive_service():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds)


def find_file(service, name, folder_id):
    query = f"name = '{name}' and '{folder_id}' in parents and trashed = false"
    results = (
        service.files()
        .list(q=query, fields="files(id, name, modifiedTime)")
        .execute()
    )
    files = results.get("files", [])
    return files[0] if files else None


def getEngine():
    return create_engine(POSTGRES_CONN)

# def getCloudEngine():
#     return create_engine(SUPABASE_CONN)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

@dag(
    dag_id="orders_bronze_silver_gold",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "medallion", "postgres"],
)
def orders_medallion_pipeline():
    @task
    def bronze() -> str:
        print("Bronze layer se raw data load ho raha hai...")
        
        service = get_drive_service()
        file_meta = find_file(service, SOURCE_FILE_NAME, DRIVE_FOLDER_ID)

        if not file_meta:
            raise FileNotFoundError(
                f"'{SOURCE_FILE_NAME}' Drive folder mein nahi mili. Naam check karo."
            )

        last_modified = Variable.get("csv_last_modified", default_var=None)
        current_modified = file_meta["modifiedTime"]

        if last_modified == current_modified:
            print("File update nahi hui hai — is baar skip kar rahe hain.")
            return "SKIP"

        request = service.files().get_media(fileId=file_meta["id"])
        with io.FileIO(LOCAL_RAW_PATH, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()

        Variable.set("csv_last_modified", current_modified)
        print(f"Downloaded: {file_meta['name']} (modified: {current_modified})")

        engine = getEngine()

        try:
            with engine.connect() as conn:
                conn.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_BRONZE};")
                conn.commit()
        except Exception as e:
            print(f"Schema already exists ya create hone mein race condition: {e}")
            
        df = pd.read_csv( LOCAL_RAW_PATH, dtype=str )

        # df = pd.read_csv(RAW_CSV_PATH, dtype=str)

        print(f"Raw rows read: {len(df)}")
        print(f"Raw columns: {list(df.columns)}")

        df.to_sql(
            TABLE_NAME,
            engine,
            schema=SCHEMA_BRONZE,
            if_exists="replace",
            index=False,
        )

        print(f"Bronze table '{SCHEMA_BRONZE}.{TABLE_NAME}' mein {len(df)} rows likhi.")
        return "PROCESS"
    
    @task
    def silver(signal: str) -> str:
        print("Silver layer mein cleaning shuru...")
        
        if signal == "SKIP":
            print("Kuch process nahi karna — file same hai.")
            return "SKIP"
        
        engine = getEngine()
        
        try:
            with engine.connect() as conn:
                conn.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_SILVER};")
                conn.commit()
        except Exception as e:
            print(f"Schema already exists ya create hone mein race condition: {e}")

        df = pd.read_sql_table(TABLE_NAME, engine, schema=SCHEMA_BRONZE)

        print(f"Bronze se padhi rows: {len(df)}")
        
        text_cols = ["customer_name", "city", "product", "category","payment_method", "status"]
        
        for col in text_cols:
            df[col] = df[col].astype(str).str.strip()
            
        junk_values = ["NULL", "NaN", "None", "n/a", "N/A", "nan", ""]
        df = df.replace(junk_values, pd.NA)
        
        df["email"] = df["email"].astype(str).str.lower().str.strip()
        
        df["email"] = df["email"].str.replace("gmial.com", "gmail.com", regex=False)
        df.loc[~df["email"].str.contains("@", na=False), "email"] = pd.NA
        
        word_to_num = {"one": "1", "two": "2", "three": "3", "four": "4", "five": "5"}
        df["quantity"] = df["quantity"].astype(str).str.lower().replace(word_to_num)
        
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
        df.loc[df["quantity"] < 0, "quantity"] = pd.NA
        
        df["unit_price"] = (
            df["unit_price"]
            .astype(str)
            .str.replace("$", "", regex=False)
            .str.replace(",", "", regex=False)
            .str.strip()
        )
        
        df["unit_price"] = df["unit_price"].replace("free", "0")
        df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
        df.loc[df["unit_price"] < 0, "unit_price"] = pd.NA
        
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
        
        df["category"] = df["category"].str.title()
        df["category"] = df["category"].fillna("Unknown")
        df["category"] = df["category"].replace({"Home And Kitchen": "Home & Kitchen"})

        df["status"] = df["status"].str.title()
        df["status"] = df["status"].fillna("Unknown")
        
        df["city"] = df["city"].str.title()
        
        before = len(df)
        df = df.drop_duplicates(subset=["order_id"])
        removed = before - len(df)
        print(f"Duplicate order_id rows removed: {removed}")
        
        df["processed_at"] = pd.Timestamp.utcnow()
        
        df.to_sql(
            TABLE_NAME,
            engine,
            schema=SCHEMA_SILVER,
            if_exists="replace",
            index=False,
        )

        print(f"Silver table '{SCHEMA_SILVER}.{TABLE_NAME}' mein {len(df)} rows likhi.")
        return "PROCESS"
    
    @task
    def gold(signal: str):
        print("Gold layer mein final table taiyar ho raha hai...")
        
        if signal == "SKIP":
            print("Kuch process nahi karna — file same hai.")
            return "SKIP"

        engine = getEngine()
        df = pd.read_sql_table(TABLE_NAME, engine, schema=SCHEMA_SILVER)

        print(f"Silver se padhi rows: {len(df)}")

        df["revenue"] = df["quantity"] * df["unit_price"]

        try:
            with engine.connect() as conn:
                conn.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_GOLD};")
                conn.exec_driver_sql(f"TRUNCATE TABLE {SCHEMA_GOLD}.{TABLE_NAME};" )
                conn.commit()
        except Exception as e:
            print(f"Schema already exists ya create hone mein race condition: {e}")
            
        
                    
        df.to_sql(
            TABLE_NAME,
            engine,
            schema=SCHEMA_GOLD,
            if_exists="append",
            index=False,
        )

        print(f"Gold table '{SCHEMA_GOLD}.{TABLE_NAME}' mein {len(df)} rows likhi.")
    
    b = bronze()
    s = silver(b)
    gold(s)

orders_medallion_pipeline()