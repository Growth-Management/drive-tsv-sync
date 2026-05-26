import io
import json
import os
import re
from typing import Dict, Any

from flask import Flask, jsonify
from google.cloud import storage
from google.cloud import secretmanager
from google.cloud import bigquery
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

app = Flask(__name__)

PROJECT_ID = os.environ["PROJECT_ID"]

FOLDER_ID = "1o5OHFPUxxzjSLg3JkDPHK_SbA6Q2Dl6q"

GCS_BUCKET = "drive-tsv"
GCS_PREFIX = "drive-tsv/top_banner_tsv/"
STATE_BLOB = "state/state.json"

SECRET_CLIENT_ID = "drive-oauth-client-id"
SECRET_CLIENT_SECRET = "drive-oauth-client-secret"
SECRET_REFRESH_TOKEN = "drive-oauth-refresh-token"

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

BQ_DATASET = "ice_magapocke_source"
BQ_TABLE = "top_banner_tsv"
BQ_STAGING_TABLE = "top_banner_tsv_stg"

BQ_TABLE_ID = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
BQ_STAGING_TABLE_ID = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_STAGING_TABLE}"

BQ_SCHEMA = [
    bigquery.SchemaField("command", "STRING"),
    bigquery.SchemaField("start_time", "STRING"),
    bigquery.SchemaField("finish_time", "STRING"),
    bigquery.SchemaField("note", "STRING"),
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("type", "STRING"),
    bigquery.SchemaField("origin_image", "STRING"),
    bigquery.SchemaField("url_scheme", "STRING"),
    bigquery.SchemaField("alt_text", "STRING"),
    bigquery.SchemaField("weight", "STRING"),
    bigquery.SchemaField("platform", "STRING"),
    bigquery.SchemaField("subscriber", "STRING"),
    bigquery.SchemaField("target_user", "STRING"),
    bigquery.SchemaField("target_account_ids_csv", "STRING"),
    bigquery.SchemaField("purchaser", "STRING"),
    bigquery.SchemaField("is_unpurchased_point", "STRING"),
    bigquery.SchemaField("unpurchased_point_days", "STRING"),
    bigquery.SchemaField("point_inequality", "STRING"),
    bigquery.SchemaField("point", "STRING"),
]

MERGE_KEYS = ["id"]


def get_secret(secret_id: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8").strip()


def get_drive_service():
    client_id = get_secret(SECRET_CLIENT_ID)
    client_secret = get_secret(SECRET_CLIENT_SECRET)
    refresh_token = get_secret(SECRET_REFRESH_TOKEN)

    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=SCOPES,
    )

    return build("drive", "v3", credentials=creds, cache_discovery=False)


def get_storage_bucket():
    client = storage.Client(project=PROJECT_ID)
    return client.bucket(GCS_BUCKET)


def load_state(bucket) -> Dict[str, Any]:
    blob = bucket.blob(STATE_BLOB)

    if not blob.exists():
        return {}

    return json.loads(blob.download_as_text(encoding="utf-8"))


def save_state(bucket, state: Dict[str, Any]) -> None:
    blob = bucket.blob(STATE_BLOB)
    blob.upload_from_string(
        json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True),
        content_type="application/json",
    )


def list_tsv_files(drive_service):
    query = f"'{FOLDER_ID}' in parents and name contains '.tsv' and trashed = false"

    files = []
    page_token = None

    while True:
        response = (
            drive_service.files()
            .list(
                q=query,
                corpora="allDrives",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                fields=(
                    "nextPageToken, "
                    "files(id, name, mimeType, modifiedTime, md5Checksum, size)"
                ),
                pageToken=page_token,
            )
            .execute()
        )

        files.extend(response.get("files", []))
        page_token = response.get("nextPageToken")

        if not page_token:
            break

    return files


def should_transfer(file_meta: Dict[str, Any], state: Dict[str, Any]) -> bool:
    file_id = file_meta["id"]
    previous = state.get(file_id)

    if previous is None:
        return True

    current_md5 = file_meta.get("md5Checksum")
    previous_md5 = previous.get("md5Checksum")

    if current_md5 and previous_md5:
        return current_md5 != previous_md5

    return (
        file_meta.get("modifiedTime") != previous.get("modifiedTime")
        or file_meta.get("size") != previous.get("size")
    )


def download_drive_file(drive_service, file_id: str) -> bytes:
    request = drive_service.files().get_media(
        fileId=file_id,
        supportsAllDrives=True,
    )

    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    buffer.seek(0)
    return buffer.read()


def upload_to_gcs(bucket, file_name: str, data: bytes) -> str:
    blob_path = f"{GCS_PREFIX}{file_name}"
    blob = bucket.blob(blob_path)

    blob.upload_from_string(
        data,
        content_type="text/tab-separated-values",
    )

    return blob_path


def extract_file_timestamp(blob_name: str) -> str | None:
    match = re.search(r"_(\d{14})\.tsv$", blob_name)
    return match.group(1) if match else None


def find_latest_tsv_blob(bucket):
    blobs = list(bucket.list_blobs(prefix=GCS_PREFIX))

    candidates = []
    for blob in blobs:
        ts = extract_file_timestamp(blob.name)
        if ts:
            candidates.append((ts, blob))

    if not candidates:
        return None

    return max(candidates, key=lambda x: x[0])[1]


def ensure_bq_tables(bq_client: bigquery.Client) -> None:
    schema = BQ_SCHEMA

    for table_id in [BQ_TABLE_ID, BQ_STAGING_TABLE_ID]:
        table = bigquery.Table(table_id, schema=schema)
        try:
            bq_client.get_table(table_id)
        except Exception:
            bq_client.create_table(table)


def load_latest_tsv_to_bq(bucket) -> Dict[str, Any]:
    latest_blob = find_latest_tsv_blob(bucket)

    if latest_blob is None:
        return {
            "loaded": False,
            "reason": "No TSV files found in GCS prefix.",
        }

    raw_bytes = latest_blob.download_as_bytes()

    try:
        text = raw_bytes.decode("utf-16")
    except UnicodeDecodeError:
        text = raw_bytes.decode("utf-8-sig")

    utf8_bytes = text.encode("utf-8")

    bq_client = bigquery.Client(project=PROJECT_ID)
    ensure_bq_tables(bq_client)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter="\t",
        skip_leading_rows=3,
        schema=BQ_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        allow_quoted_newlines=True,
        encoding="UTF-8",
        quote_character='"',
        max_bad_records=0,
    )

    load_job = bq_client.load_table_from_file(
        io.BytesIO(utf8_bytes),
        BQ_STAGING_TABLE_ID,
        job_config=job_config,
        rewind=True,
    )
    load_job.result()

    merge_to_main_table(bq_client)

    return {
        "loaded": True,
        "source_gcs": f"gs://{GCS_BUCKET}/{latest_blob.name}",
        "staging_table": BQ_STAGING_TABLE_ID,
        "target_table": BQ_TABLE_ID,
    }


def merge_to_main_table(bq_client: bigquery.Client) -> None:
    columns = [field.name for field in BQ_SCHEMA]

    on_condition = " and ".join([
        f"target.{key} = source.{key}"
        for key in MERGE_KEYS
    ])

    update_columns = [
        col for col in columns
        if col not in MERGE_KEYS
    ]

    update_set = "\n        , ".join([
        f"{col} = source.{col}"
        for col in update_columns
    ])

    insert_columns = "\n        , ".join(columns)
    insert_values = "\n        , ".join([
        f"source.{col}"
        for col in columns
    ])

    sql = f"""
merge
    `{BQ_TABLE_ID}` as target
using
    `{BQ_STAGING_TABLE_ID}` as source
on
    {on_condition}
when matched then
    update set
        {update_set}
when not matched then
    insert (
        {insert_columns}
    )
    values (
        {insert_values}
    )
"""

    query_job = bq_client.query(sql)
    query_job.result()


@app.route("/", methods=["GET", "POST"])
def run_sync():
    drive_service = get_drive_service()
    bucket = get_storage_bucket()

    state = load_state(bucket)
    files = list_tsv_files(drive_service)

    transferred = []
    skipped = []
    failed = []

    for file_meta in files:
        file_id = file_meta["id"]
        file_name = file_meta["name"]

        try:
            if not should_transfer(file_meta, state):
                skipped.append(file_name)
                continue

            data = download_drive_file(drive_service, file_id)
            gcs_path = upload_to_gcs(bucket, file_name, data)

            state[file_id] = {
                "name": file_name,
                "md5Checksum": file_meta.get("md5Checksum"),
                "modifiedTime": file_meta.get("modifiedTime"),
                "size": file_meta.get("size"),
                "gcsPath": f"gs://{GCS_BUCKET}/{gcs_path}",
            }

            transferred.append(file_name)

        except Exception as e:
            failed.append({
                "name": file_name,
                "id": file_id,
                "error": str(e),
            })

    if transferred:
        save_state(bucket, state)

    if transferred:
        bq_result = load_latest_tsv_to_bq(bucket)
    else:
        bq_result = {
            "loaded": False,
            "reason": "No transferred files. BigQuery load skipped.",
        }

    result = {
        "detected": len(files),
        "transferred": len(transferred),
        "skipped": len(skipped),
        "failed": len(failed),
        "transferred_files": transferred,
        "failed_files": failed,
        "bigquery": bq_result,
    }

    status_code = 500 if failed else 200
    return jsonify(result), status_code