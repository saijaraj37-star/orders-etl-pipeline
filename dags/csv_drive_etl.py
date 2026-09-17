# @dag(dag_id="mera_naya_pipeline", schedule="...", ...)
# def mera_pipeline():
    
#     @task
#     def extract():
#         # source से data लाओ (API, database, file, वगैरह)
#         return data

#     @task
#     def transform(data):
#         # clean करो, process करो
#         return processed_data

#     @task
#     def load(processed_data):
#         # destination पर भेजो (database, warehouse, Drive, S3 वगैरह)
#         pass

#     d = extract()
#     p = transform(d)
#     load(p)

# mera_pipeline()

# from __future__ import annotations

# import io
# from datetime import datetime, timedelta

# import pandas as pd
# from googleapiclient.discovery import build
# from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
# from google.oauth2 import service_account

# from airflow.decorators import dag, task
# from airflow.models import Variable

# # ---------------- Config — apni zaroorat ke hisaab se badlo ----------------
# SERVICE_ACCOUNT_FILE = "/opt/airflow/config/credentials/service-account.json"
# DRIVE_FOLDER_ID = "1anKRcBB9r5gIYaF0WAEeeWdGjJG6qotm"
# SOURCE_FILE_NAME = "data.csv"            # Drive mein rakhi CSV ka exact naam
# PROCESSED_FILE_NAME = "data_processed.csv"
# SCOPES = ["https://www.googleapis.com/auth/drive"]

# LOCAL_RAW_PATH = "/tmp/data_raw.csv"
# LOCAL_PROCESSED_PATH = "/tmp/data_processed.csv"
# # -----------------------------------------------------------------------------


# def get_drive_service():
#     creds = service_account.Credentials.from_service_account_file(
#         SERVICE_ACCOUNT_FILE, scopes=SCOPES
#     )
#     return build("drive", "v3", credentials=creds)


# def find_file(service, name, folder_id):
#     query = f"name = '{name}' and '{folder_id}' in parents and trashed = false"
#     results = (
#         service.files()
#         .list(q=query, fields="files(id, name, modifiedTime)")
#         .execute()
#     )
#     files = results.get("files", [])
#     return files[0] if files else None


# default_args = {
#     "owner": "airflow",
#     "retries": 1,
#     "retry_delay": timedelta(minutes=2),
# }


# @dag(
#     dag_id="csv_drive_etl_pipeline",
#     schedule="*/10 * * * *",
#     start_date=datetime(2026, 1, 1),
#     catchup=False,
#     default_args=default_args,
#     tags=["etl", "google-drive"],
# )
# def csv_drive_etl():
#     """
#     Har 10 minute mein Google Drive se CSV check karta hai,
#     agar update hui hai to process karke wapas Drive par daal deta hai.
#     """

#     @task
#     def extract() -> str:
#         """Drive se file check karo, sirf update hone par download karo."""
#         service = get_drive_service()
#         file_meta = find_file(service, SOURCE_FILE_NAME, DRIVE_FOLDER_ID)

#         if not file_meta:
#             raise FileNotFoundError(
#                 f"'{SOURCE_FILE_NAME}' Drive folder mein nahi mili. Naam check karo."
#             )

#         last_modified = Variable.get("csv_last_modified", default_var=None)
#         current_modified = file_meta["modifiedTime"]

#         if last_modified == current_modified:
#             print("File update nahi hui hai — is baar skip kar rahe hain.")
#             return "SKIP"

#         request = service.files().get_media(fileId=file_meta["id"])
#         with io.FileIO(LOCAL_RAW_PATH, "wb") as fh:
#             downloader = MediaIoBaseDownload(fh, request)
#             done = False
#             while not done:
#                 _, done = downloader.next_chunk()

#         Variable.set("csv_last_modified", current_modified)
#         print(f"Downloaded: {file_meta['name']} (modified: {current_modified})")
#         return "PROCESS"

#     @task
#     def transform(signal: str) -> str:
#         """CSV ko clean aur process karo."""
#         if signal == "SKIP":
#             print("Kuch process nahi karna — file same hai.")
#             return "SKIP"

#         df = pd.read_csv(LOCAL_RAW_PATH)

#         # ---- Example transformations — apni zaroorat ke hisaab se badlo ----
#         df = df.drop_duplicates()
#         df.columns = [c.strip() for c in df.columns]
#         df["processed_at"] = datetime.utcnow().isoformat()
#         # ----------------------------------------------------------------------

#         df.to_csv(LOCAL_PROCESSED_PATH, index=False)
#         print(f"Processed {len(df)} rows, {len(df.columns)} columns.")
#         return "PROCESS"

#     @task
#     def load(signal: str):
#         """Processed file ko Drive par upload ya update karo."""
#         if signal == "SKIP":
#             print("Upload skip — kuch naya nahi hai.")
#             return

#         service = get_drive_service()
#         existing = find_file(service, PROCESSED_FILE_NAME, DRIVE_FOLDER_ID)
#         media = MediaIoBaseUpload(
#             io.FileIO(LOCAL_PROCESSED_PATH, "rb"), mimetype="text/csv"
#         )

#         if existing:
#             service.files().update(fileId=existing["id"], media_body=media).execute()
#             print(f"Updated existing file on Drive: {PROCESSED_FILE_NAME}")
#         else:
#             file_metadata = {"name": PROCESSED_FILE_NAME, "parents": [DRIVE_FOLDER_ID]}
#             service.files().create(body=file_metadata, media_body=media).execute()
#             print(f"Created new file on Drive: {PROCESSED_FILE_NAME}")

#     raw_signal = extract()
#     processed_signal = transform(raw_signal)
#     load(processed_signal)


# csv_drive_etl()

from __future__ import annotations

import io
from datetime import datetime, timedelta, timezone

import pandas as pd
import openpyxl
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

from airflow.decorators import dag, task
from airflow.sdk import Variable


# ============================================================
# CONFIG
# ============================================================

SERVICE_ACCOUNT_FILE = (
    "/opt/airflow/config/credentials/service-account.json"
)

DRIVE_FOLDER_ID = "1anKRcBB9r5gIYaF0WAEeeWdGjJG6qotm"

SOURCE_FILE_NAME = "data.xlsx"
PROCESSED_FILE_NAME = "data_processed.xlsx"

LOCAL_RAW_PATH = "/tmp/data_raw.xlsx"
LOCAL_PROCESSED_PATH = "/tmp/data_processed.xlsx"

SCOPES = [
    "https://www.googleapis.com/auth/drive"
]

EXCEL_MIME_TYPE = (
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)


# ============================================================
# GOOGLE DRIVE SERVICE
# ============================================================

def get_drive_service():
    credentials = (
        service_account.Credentials
        .from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=SCOPES,
        )
    )

    return build(
        "drive",
        "v3",
        credentials=credentials,
    )


# ============================================================
# FIND FILE
# ============================================================

def find_file(
    service,
    name,
    folder_id,
    mime_type=None,
):
    query = (
        f"'{folder_id}' in parents "
        f"and name = '{name}' "
        f"and trashed = false"
    )

    if mime_type:
        query += f" and mimeType = '{mime_type}'"

    response = (
        service.files()
        .list(
            q=query,
            fields=(
                "files("
                "id,"
                "name,"
                "mimeType,"
                "modifiedTime"
                ")"
            ),
            orderBy="modifiedTime desc",
            pageSize=100,
        )
        .execute()
    )

    files = response.get("files", [])

    print("=" * 60)
    print(f"Searching for: {name}")
    print(f"Folder ID: {folder_id}")
    print(f"Files found: {len(files)}")
    print("=" * 60)

    for file in files:
        print(
            f"Name: {file.get('name')} | "
            f"ID: {file.get('id')} | "
            f"MIME: {file.get('mimeType')} | "
            f"Modified: {file.get('modifiedTime')}"
        )

    if not files:
        return None

    # Newest file is returned because of orderBy
    return files[0]


# ============================================================
# DEFAULT ARGS
# ============================================================

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# ============================================================
# DAG
# ============================================================

@dag(
    dag_id="xlsx_drive_etl_pipeline",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=[
        "etl",
        "google-drive",
        "xlsx",
    ],
)
def xlsx_drive_etl():

    # ========================================================
    # EXTRACT
    # ========================================================

    @task
    def extract() -> str:

        print("=" * 60)
        print("STARTING EXTRACT")
        print("=" * 60)

        service = get_drive_service()

        # Find the raw .xlsx file (uploaded directly, not a Google Sheet)
        source_file = find_file(
            service=service,
            name=SOURCE_FILE_NAME,
            folder_id=DRIVE_FOLDER_ID,
            mime_type=EXCEL_MIME_TYPE,
        )

        if not source_file:
            raise FileNotFoundError(
                f"Excel file '{SOURCE_FILE_NAME}' "
                f"was not found in Drive folder "
                f"{DRIVE_FOLDER_ID}"
            )

        file_id = source_file["id"]
        current_modified = source_file["modifiedTime"]

        print(f"Source file: {source_file['name']}")
        print(f"File ID: {file_id}")
        print(f"Modified: {current_modified}")

        # Get previous modification time
        last_modified = Variable.get(
            "xlsx_last_modified",
            default=None,
        )

        print(
            f"Last processed modification: "
            f"{last_modified}"
        )

        # Skip if unchanged
        if last_modified == current_modified:
            print(
                "File has not changed. "
                "Skipping processing."
            )
            return "SKIP"

        # ----------------------------------------------------
        # DOWNLOAD RAW XLSX (already in Excel format, no conversion)
        # ----------------------------------------------------

        print("Downloading raw .xlsx file...")

        request = service.files().get_media(
            fileId=file_id,
        )

        with io.FileIO(
            LOCAL_RAW_PATH,
            "wb",
        ) as file_handle:

            downloader = MediaIoBaseDownload(
                file_handle,
                request,
            )

            done = False

            while not done:

                status, done = (
                    downloader.next_chunk()
                )

                if status:
                    progress = int(
                        status.progress() * 100
                    )

                    print(
                        f"Download progress: "
                        f"{progress}%"
                    )

        # Save modification time
        Variable.set(
            "xlsx_last_modified",
            current_modified,
        )

        print(
            f"Excel file saved to: "
            f"{LOCAL_RAW_PATH}"
        )

        print("=" * 60)
        print("EXTRACT COMPLETED")
        print("=" * 60)

        return "PROCESS"

    # ========================================================
    # TRANSFORM
    # ========================================================

    @task
    def transform(signal: str) -> str:

        print("=" * 60)
        print("STARTING TRANSFORM")
        print("=" * 60)

        if signal == "SKIP":
            print(
                "No changes detected. "
                "Transform skipped."
            )
            return "SKIP"

        # ----------------------------------------------------
        # READ EXCEL
        # ----------------------------------------------------

        print(
            f"Reading: {LOCAL_RAW_PATH}"
        )

        df = pd.read_excel(
            LOCAL_RAW_PATH,
            engine="openpyxl",
        )

        print(
            f"Rows before processing: "
            f"{len(df)}"
        )

        print(
            f"Columns before processing: "
            f"{len(df.columns)}"
        )

        # ----------------------------------------------------
        # REMOVE DUPLICATES
        # ----------------------------------------------------

        before = len(df)

        df = df.drop_duplicates()

        removed = before - len(df)

        print(
            f"Duplicate rows removed: "
            f"{removed}"
        )

        # ----------------------------------------------------
        # CLEAN COLUMN NAMES
        # ----------------------------------------------------

        df.columns = [
            str(column).strip()
            for column in df.columns
        ]

        print(
            f"Columns: {list(df.columns)}"
        )

        # ----------------------------------------------------
        # ADD PROCESSING TIMESTAMP
        # ----------------------------------------------------

        df["processed_at"] = (
            datetime.now(
                timezone.utc
            ).isoformat()
        )

        # ----------------------------------------------------
        # SAVE EXCEL
        # ----------------------------------------------------

        df.to_excel(
            LOCAL_PROCESSED_PATH,
            index=False,
            engine="openpyxl",
        )

        print(
            f"Processed rows: {len(df)}"
        )

        print(
            f"Processed columns: {len(df.columns)}"
        )

        print(
            f"Processed file: "
            f"{LOCAL_PROCESSED_PATH}"
        )

        print("=" * 60)
        print("TRANSFORM COMPLETED")
        print("=" * 60)

        return "PROCESS"

    # ========================================================
    # LOAD
    # ========================================================

    @task
    def load(signal: str):

        print("=" * 60)
        print("STARTING LOAD")
        print("=" * 60)

        if signal == "SKIP":
            print(
                "No changes detected. "
                "Upload skipped."
            )
            return

        service = get_drive_service()

        # Find existing processed file
        existing_file = find_file(
            service=service,
            name=PROCESSED_FILE_NAME,
            folder_id=DRIVE_FOLDER_ID,
        )

        # ----------------------------------------------------
        # PREPARE FILE
        # ----------------------------------------------------

        media = MediaIoBaseUpload(
            io.FileIO(
                LOCAL_PROCESSED_PATH,
                "rb",
            ),
            mimetype=EXCEL_MIME_TYPE,
        )

        # ----------------------------------------------------
        # UPDATE EXISTING FILE
        # ----------------------------------------------------

        if existing_file:

            print(
                f"Updating existing file: "
                f"{existing_file['id']}"
            )

            service.files().update(
                fileId=existing_file["id"],
                media_body=media,
            ).execute()

            print(
                f"Updated: "
                f"{PROCESSED_FILE_NAME}"
            )

        # ----------------------------------------------------
        # CREATE NEW FILE
        # ----------------------------------------------------

        else:

            print(
                "Processed file does not exist."
            )

            print(
                "Creating new Excel file..."
            )

            file_metadata = {
    "name": PROCESSED_FILE_NAME,
    "parents": [DRIVE_FOLDER_ID],
    "mimeType": "application/vnd.google-apps.spreadsheet",  # यह force करता है Sheet बनाना
}

            service.files().create(
                body=file_metadata,
                media_body=media,
            ).execute()

            print(
                f"Created: "
                f"{PROCESSED_FILE_NAME}"
            )

        print("=" * 60)
        print("LOAD COMPLETED")
        print("=" * 60)

    # ========================================================
    # TASK FLOW
    # ========================================================

    raw_signal = extract()

    processed_signal = transform(
        raw_signal
    )

    load(
        processed_signal
    )


# ============================================================
# REGISTER DAG
# ============================================================

xlsx_drive_etl()
