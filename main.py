import io
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

from flask import Flask, jsonify
from google.cloud import storage
from google.cloud import secretmanager
from google.cloud import bigquery
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

app = Flask(__name__)

PROJECT_ID = os.environ["PROJECT_ID"]
CONFIG_PATH = Path(os.environ.get("SYNC_TARGETS_CONFIG", "config/sync_targets.json"))

GCS_BUCKET = "drive-tsv"

SECRET_CLIENT_ID = "drive-oauth-client-id"
SECRET_CLIENT_SECRET = "drive-oauth-client-secret"
SECRET_REFRESH_TOKEN = "drive-oauth-refresh-token"

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

REQUIRED_TARGET_FIELDS = {
    "folder_id",
    "gcs_prefix",
    "state_blob",
    "bq_dataset",
    "bq_table",
    "bq_staging_table",
    "merge_keys",
}


@dataclass(frozen=True)
class SyncTarget:
    name: str
    folder_id: str
    gcs_prefix: str
    state_blob: str
    bq_dataset: str
    bq_table: str
    bq_staging_table: str
    merge_keys: List[str]
    bq_schema: List[bigquery.SchemaField]

    @property
    def bq_table_id(self) -> str:
        return f"{PROJECT_ID}.{self.bq_dataset}.{self.bq_table}"

    @property
    def bq_staging_table_id(self) -> str:
        return f"{PROJECT_ID}.{self.bq_dataset}.{self.bq_staging_table}"


def schema_field_from_config(field_config: Dict[str, Any]) -> bigquery.SchemaField:
    field_name = field_config["name"]
    field_type = field_config.get("type") or field_config.get("field_type")

    if not field_type:
        raise ValueError(f"BigQuery schema field '{field_name}' is missing type.")

    return bigquery.SchemaField(
        field_name,
        field_type,
        mode=field_config.get("mode", "NULLABLE"),
        description=field_config.get("description"),
    )


def normalize_gcs_prefix(gcs_prefix: str) -> str:
    prefix = gcs_prefix.strip("/")
    return f"{prefix}/" if prefix else ""


def load_sync_targets() -> List[SyncTarget]:
    with CONFIG_PATH.open(encoding="utf-8") as config_file:
        config = json.load(config_file)

    target_configs = config.get("targets")
    if not isinstance(target_configs, list) or not target_configs:
        raise ValueError(f"{CONFIG_PATH} must contain a non-empty targets list.")

    targets = []
    for index, target_config in enumerate(target_configs):
        if not isinstance(target_config, dict):
            raise ValueError(f"Target at index {index} must be an object.")

        missing = REQUIRED_TARGET_FIELDS - target_config.keys()
        if missing:
            missing_fields = ", ".join(sorted(missing))
            raise ValueError(f"Target at index {index} is missing: {missing_fields}")

        schema_config = target_config.get("bq_schema")
        if not isinstance(schema_config, list) or not schema_config:
            raise ValueError(
                f"Target '{target_config.get('name', target_config['bq_table'])}' "
                "must define a non-empty bq_schema."
            )

        merge_keys = target_config["merge_keys"]
        if not isinstance(merge_keys, list) or not merge_keys:
            raise ValueError(
                f"Target '{target_config.get('name', target_config['bq_table'])}' "
                "must define a non-empty merge_keys list."
            )

        schema = [schema_field_from_config(field) for field in schema_config]
        schema_columns = {field.name for field in schema}
        missing_merge_keys = set(merge_keys) - schema_columns
        if missing_merge_keys:
            missing_keys = ", ".join(sorted(missing_merge_keys))
            raise ValueError(
                f"Target '{target_config.get('name', target_config['bq_table'])}' "
                f"has merge_keys not present in bq_schema: {missing_keys}"
            )

        targets.append(
            SyncTarget(
                name=target_config.get("name", target_config["bq_table"]),
                folder_id=target_config["folder_id"],
                gcs_prefix=normalize_gcs_prefix(target_config["gcs_prefix"]),
                state_blob=target_config["state_blob"],
                bq_dataset=target_config["bq_dataset"],
                bq_table=target_config["bq_table"],
                bq_staging_table=target_config["bq_staging_table"],
                merge_keys=merge_keys,
                bq_schema=schema,
            )
        )

    return targets


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


def load_state(bucket, state_blob: str) -> Dict[str, Any]:
    blob = bucket.blob(state_blob)

    if not blob.exists():
        return {}

    return json.loads(blob.download_as_text(encoding="utf-8"))


def save_state(bucket, state_blob: str, state: Dict[str, Any]) -> None:
    blob = bucket.blob(state_blob)
    blob.upload_from_string(
        json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True),
        content_type="application/json",
    )


def list_tsv_files(drive_service, folder_id: str):
    query = f"'{folder_id}' in parents and name contains '.tsv' and trashed = false"

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


def gcs_blob_path(target: SyncTarget, file_name: str) -> str:
    return f"{target.gcs_prefix}{file_name}" if target.gcs_prefix else file_name


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


def upload_to_gcs(bucket, target: SyncTarget, file_name: str, data: bytes) -> str:
    blob_path = gcs_blob_path(target, file_name)
    blob = bucket.blob(blob_path)

    blob.upload_from_string(
        data,
        content_type="text/tab-separated-values",
    )

    return blob_path


def extract_file_timestamp(blob_name: str) -> str | None:
    match = re.search(r"_(\d{14})\.tsv$", blob_name)
    return match.group(1) if match else None


def find_latest_tsv_blob(bucket, target: SyncTarget):
    blobs = list(bucket.list_blobs(prefix=target.gcs_prefix))

    candidates = []
    for blob in blobs:
        ts = extract_file_timestamp(blob.name)
        if ts:
            candidates.append((ts, blob))

    if not candidates:
        return None

    return max(candidates, key=lambda x: x[0])[1]


def ensure_bq_tables(bq_client: bigquery.Client, target: SyncTarget) -> None:
    for table_id in [target.bq_table_id, target.bq_staging_table_id]:
        table = bigquery.Table(table_id, schema=target.bq_schema)
        try:
            bq_client.get_table(table_id)
        except Exception:
            bq_client.create_table(table)


def load_latest_tsv_to_bq(bucket, target: SyncTarget) -> Dict[str, Any]:
    latest_blob = find_latest_tsv_blob(bucket, target)

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
    ensure_bq_tables(bq_client, target)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter="\t",
        skip_leading_rows=3,
        schema=target.bq_schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        allow_quoted_newlines=True,
        encoding="UTF-8",
        quote_character='"',
        max_bad_records=0,
    )

    load_job = bq_client.load_table_from_file(
        io.BytesIO(utf8_bytes),
        target.bq_staging_table_id,
        job_config=job_config,
        rewind=True,
    )
    load_job.result()

    merge_to_main_table(bq_client, target)

    return {
        "loaded": True,
        "source_gcs": f"gs://{GCS_BUCKET}/{latest_blob.name}",
        "staging_table": target.bq_staging_table_id,
        "target_table": target.bq_table_id,
    }


def bq_column(column_name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column_name):
        raise ValueError(f"Invalid BigQuery column name: {column_name}")
    return f"`{column_name}`"


def merge_to_main_table(bq_client: bigquery.Client, target: SyncTarget) -> None:
    columns = [field.name for field in target.bq_schema]

    on_condition = " and ".join([
        f"target.{bq_column(key)} = source.{bq_column(key)}"
        for key in target.merge_keys
    ])

    update_columns = [
        col for col in columns
        if col not in target.merge_keys
    ]

    update_set = "\n        , ".join([
        f"{bq_column(col)} = source.{bq_column(col)}"
        for col in update_columns
    ])
    matched_clause = ""
    if update_columns:
        matched_clause = f"""
when matched then
    update set
        {update_set}
"""

    insert_columns = "\n        , ".join([bq_column(col) for col in columns])
    insert_values = "\n        , ".join([
        f"source.{bq_column(col)}"
        for col in columns
    ])

    sql = f"""
merge
    `{target.bq_table_id}` as target
using
    `{target.bq_staging_table_id}` as source
on
    {on_condition}
{matched_clause}
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


def sync_target(drive_service, bucket, target: SyncTarget) -> Dict[str, Any]:
    state = load_state(bucket, target.state_blob)
    files = list_tsv_files(drive_service, target.folder_id)

    transferred = []
    skipped = []
    failed = []
    target_error = None

    for file_meta in files:
        file_id = file_meta["id"]
        file_name = file_meta["name"]

        try:
            if not should_transfer(file_meta, state):
                skipped.append(file_name)
                continue

            data = download_drive_file(drive_service, file_id)
            gcs_path = upload_to_gcs(bucket, target, file_name, data)

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
        save_state(bucket, target.state_blob, state)

    if transferred:
        try:
            bq_result = load_latest_tsv_to_bq(bucket, target)
        except Exception as e:
            target_error = f"BigQuery load failed: {e}"
            bq_result = {
                "loaded": False,
                "error": str(e),
            }
    else:
        bq_result = {
            "loaded": False,
            "reason": "No transferred files. BigQuery load skipped.",
        }

    result = {
        "target": target.name,
        "folder_id": target.folder_id,
        "gcs_prefix": target.gcs_prefix,
        "state_blob": target.state_blob,
        "detected": len(files),
        "transferred": len(transferred),
        "skipped": len(skipped),
        "failed": len(failed),
        "transferred_files": transferred,
        "failed_files": failed,
        "bigquery": bq_result,
    }

    if target_error:
        result["error"] = target_error

    return result


@app.route("/", methods=["GET", "POST"])
def run_sync():
    drive_service = get_drive_service()
    bucket = get_storage_bucket()
    targets = load_sync_targets()

    target_results = []

    for target in targets:
        try:
            target_results.append(sync_target(drive_service, bucket, target))
        except Exception as e:
            target_results.append({
                "target": target.name,
                "folder_id": target.folder_id,
                "error": str(e),
            })

    failed_targets = [
        target_result
        for target_result in target_results
        if target_result.get("error") or target_result.get("failed", 0) > 0
    ]

    summary = {
        "detected": sum(target_result.get("detected", 0) for target_result in target_results),
        "transferred": sum(
            target_result.get("transferred", 0)
            for target_result in target_results
        ),
        "skipped": sum(target_result.get("skipped", 0) for target_result in target_results),
        "failed": sum(target_result.get("failed", 0) for target_result in target_results),
    }

    result = {
        **summary,
        "targets": len(targets),
        "failed_targets": len(failed_targets),
        "results": target_results,
    }

    if len(target_results) == 1:
        target_result = target_results[0]
        result["transferred_files"] = target_result.get("transferred_files", [])
        result["failed_files"] = target_result.get("failed_files", [])
        result["bigquery"] = target_result.get("bigquery", {
            "loaded": False,
            "error": target_result.get("error", "Target failed before BigQuery load."),
        })

    status_code = 500 if failed_targets else 200
    return jsonify(result), status_code
