import io
import csv
import json
import logging
import os
import re
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

from flask import Flask, jsonify
import requests
from google.cloud import storage
from google.cloud import secretmanager
from google.cloud import bigquery
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = os.environ["PROJECT_ID"]
CONFIG_PATH = Path(os.environ.get("SYNC_TARGETS_CONFIG", "config/sync_targets.json"))
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")
SLACK_WEBHOOK_SECRET = os.environ.get("SLACK_WEBHOOK_SECRET")

GCS_BUCKET = "drive-tsv"
INVALID_GCS_PREFIX = "invalid"

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
}


@dataclass(frozen=True)
class ValidationConfig:
    mode: str
    header_row_index: int
    notify_on_invalid: bool


@dataclass(frozen=True)
class SyncTarget:
    name: str
    folder_id: str
    file_name_pattern: str | None
    gcs_prefix: str
    state_blob: str
    bq_dataset: str
    bq_table: str
    bq_staging_table: str
    bq_schema: List[bigquery.SchemaField]
    validation: ValidationConfig

    @property
    def bq_table_id(self) -> str:
        return f"{PROJECT_ID}.{self.bq_dataset}.{self.bq_table}"

    @property
    def bq_staging_table_id(self) -> str:
        return f"{PROJECT_ID}.{self.bq_dataset}.{self.bq_staging_table}"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def log_structured(event: str, **payload: Any) -> None:
    logger.info(json.dumps({"event": event, **payload}, ensure_ascii=False, sort_keys=True))


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


def validation_config_from_target(target_config: Dict[str, Any]) -> ValidationConfig:
    validation_config = target_config.get("validation", {})
    if not isinstance(validation_config, dict):
        raise ValueError(
            f"Target '{target_config.get('name', target_config['bq_table'])}' "
            "validation must be an object."
        )

    mode = validation_config.get("mode", "strict")
    if mode not in {"strict", "disabled"}:
        raise ValueError(
            f"Target '{target_config.get('name', target_config['bq_table'])}' "
            f"has unsupported validation mode: {mode}"
        )

    header_row_index = validation_config.get("header_row_index", 1)
    if not isinstance(header_row_index, int) or header_row_index < 0:
        raise ValueError(
            f"Target '{target_config.get('name', target_config['bq_table'])}' "
            "validation.header_row_index must be a non-negative integer."
        )

    return ValidationConfig(
        mode=mode,
        header_row_index=header_row_index,
        notify_on_invalid=validation_config.get("notify_on_invalid", True),
    )


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

        file_name_pattern = target_config.get("file_name_pattern")
        if file_name_pattern is not None:
            re.compile(file_name_pattern)

        schema = [schema_field_from_config(field) for field in schema_config]
        validation = validation_config_from_target(target_config)

        targets.append(
            SyncTarget(
                name=target_config.get("name", target_config["bq_table"]),
                folder_id=target_config["folder_id"],
                file_name_pattern=file_name_pattern,
                gcs_prefix=normalize_gcs_prefix(target_config["gcs_prefix"]),
                state_blob=target_config["state_blob"],
                bq_dataset=target_config["bq_dataset"],
                bq_table=target_config["bq_table"],
                bq_staging_table=target_config["bq_staging_table"],
                bq_schema=schema,
                validation=validation,
            )
        )

    return targets


def get_secret(secret_id: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8").strip()


def get_slack_webhook_url() -> str | None:
    if SLACK_WEBHOOK_URL:
        return SLACK_WEBHOOK_URL

    if not SLACK_WEBHOOK_SECRET:
        return None

    try:
        return get_secret(SLACK_WEBHOOK_SECRET)
    except Exception as e:
        logger.error("SLACK_WEBHOOK_SECRET_ACCESS_FAILED secret=%s error=%s", SLACK_WEBHOOK_SECRET, e)
        return None


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


def matches_target_file(target: SyncTarget, file_name: str) -> bool:
    if not target.file_name_pattern:
        return True

    return re.search(target.file_name_pattern, file_name) is not None


def list_tsv_files(drive_service, target: SyncTarget):
    query = f"'{target.folder_id}' in parents and name contains '.tsv' and trashed = false"

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

    return [
        file_meta
        for file_meta in files
        if matches_target_file(target, file_meta["name"])
    ]


def gcs_blob_path(target: SyncTarget, file_name: str) -> str:
    return f"{target.gcs_prefix}{file_name}" if target.gcs_prefix else file_name


def extract_file_timestamp(file_name: str) -> str | None:
    match = re.search(r"_(\d{14})\.tsv$", file_name)
    return match.group(1) if match else None


def select_latest_drive_file(files: List[Dict[str, Any]]) -> tuple[Dict[str, Any], str] | tuple[None, None]:
    candidates = []
    for file_meta in files:
        file_timestamp = extract_file_timestamp(file_meta["name"])
        if file_timestamp:
            candidates.append((file_timestamp, file_meta))

    if not candidates:
        return None, None

    file_timestamp, file_meta = max(candidates, key=lambda candidate: candidate[0])
    return file_meta, file_timestamp


def last_success_matches(
    file_meta: Dict[str, Any],
    file_timestamp: str,
    state: Dict[str, Any],
) -> bool:
    last_success = state.get("last_success") or {}

    return (
        last_success.get("drive_file_id") == file_meta.get("id")
        and last_success.get("file_name") == file_meta.get("name")
        and last_success.get("file_timestamp") == file_timestamp
        and last_success.get("md5_checksum") == file_meta.get("md5Checksum")
        and last_success.get("modified_time") == file_meta.get("modifiedTime")
        and last_success.get("size") == file_meta.get("size")
    )


def file_state_payload(
    file_meta: Dict[str, Any],
    file_timestamp: str,
    gcs_path: str | None = None,
    loaded_at: str | None = None,
) -> Dict[str, Any]:
    payload = {
        "drive_file_id": file_meta.get("id"),
        "file_name": file_meta.get("name"),
        "file_timestamp": file_timestamp,
        "md5_checksum": file_meta.get("md5Checksum"),
        "modified_time": file_meta.get("modifiedTime"),
        "size": file_meta.get("size"),
    }

    if gcs_path is not None:
        payload["gcs_path"] = gcs_path

    if loaded_at is not None:
        payload["loaded_at"] = loaded_at

    return payload


def attempt_payload(
    status: str,
    file_meta: Dict[str, Any] | None = None,
    file_timestamp: str | None = None,
    **extra: Any,
) -> Dict[str, Any]:
    payload = {
        "status": status,
        "attempted_at": utc_now_iso(),
    }

    if file_meta and file_timestamp:
        payload.update(file_state_payload(file_meta, file_timestamp))

    payload.update(extra)
    return payload


def decode_tsv_bytes(data: bytes) -> tuple[str, str]:
    if data.startswith((b"\xff\xfe", b"\xfe\xff")):
        return data.decode("utf-16"), "utf-16"

    try:
        return data.decode("utf-8-sig"), "utf-8-sig"
    except UnicodeDecodeError:
        return data.decode("utf-16"), "utf-16"


def parse_tsv_row(row_text: str) -> List[str]:
    return next(csv.reader([row_text], delimiter="\t", quotechar='"'))


def schema_column_names(target: SyncTarget) -> List[str]:
    return [field.name for field in target.bq_schema]


def suggested_schema_patch(added_columns: List[str]) -> str | None:
    if not added_columns:
        return None

    patch = [
        {
            "name": column,
            "type": "STRING",
        }
        for column in added_columns
    ]
    return json.dumps(patch, ensure_ascii=False, indent=2)


def classify_schema_drift(
    expected_columns: List[str],
    actual_columns: List[str],
) -> tuple[str, List[str], List[str]]:
    added_columns = [column for column in actual_columns if column not in expected_columns]
    removed_columns = [column for column in expected_columns if column not in actual_columns]

    if added_columns and not removed_columns:
        return "columns_added", added_columns, removed_columns

    if removed_columns and not added_columns:
        return "columns_removed", added_columns, removed_columns

    if not added_columns and not removed_columns:
        return "column_order_changed", added_columns, removed_columns

    return "columns_changed", added_columns, removed_columns


def invalid_tsv_result(
    file_meta: Dict[str, Any],
    reason: str,
    classification: str,
    detail: str,
    expected_columns: List[str] | None = None,
    actual_columns: List[str] | None = None,
    encoding: str | None = None,
) -> Dict[str, Any]:
    expected_columns = expected_columns or []
    actual_columns = actual_columns or []
    added_columns = []
    removed_columns = []
    if actual_columns:
        _, added_columns, removed_columns = classify_schema_drift(
            expected_columns,
            actual_columns,
        )

    return {
        "valid": False,
        "name": file_meta.get("name"),
        "id": file_meta.get("id"),
        "reason": reason,
        "classification": classification,
        "detail": detail,
        "encoding": encoding,
        "expected_column_count": len(expected_columns),
        "actual_column_count": len(actual_columns),
        "added_columns": added_columns,
        "removed_columns": removed_columns,
        "suggested_schema_patch": suggested_schema_patch(added_columns),
    }


def validate_tsv_file(
    target: SyncTarget,
    file_meta: Dict[str, Any],
    data: bytes,
) -> Dict[str, Any]:
    if target.validation.mode == "disabled":
        return {"valid": True}

    expected_columns = schema_column_names(target)

    try:
        text, encoding = decode_tsv_bytes(data)
    except UnicodeDecodeError as e:
        return invalid_tsv_result(
            file_meta=file_meta,
            reason="decode_error",
            classification="decode_error",
            detail=str(e),
            expected_columns=expected_columns,
        )

    rows = text.splitlines()
    header_row_index = target.validation.header_row_index
    if len(rows) <= header_row_index:
        return invalid_tsv_result(
            file_meta=file_meta,
            reason="header_missing",
            classification="header_missing",
            detail=f"Header row index {header_row_index} is outside TSV row range.",
            expected_columns=expected_columns,
            encoding=encoding,
        )

    try:
        actual_columns = parse_tsv_row(rows[header_row_index])
    except csv.Error as e:
        return invalid_tsv_result(
            file_meta=file_meta,
            reason="tsv_parse_error",
            classification="tsv_parse_error",
            detail=str(e),
            expected_columns=expected_columns,
            encoding=encoding,
        )

    if actual_columns != expected_columns:
        classification, added_columns, removed_columns = classify_schema_drift(
            expected_columns,
            actual_columns,
        )
        return {
            "valid": False,
            "name": file_meta.get("name"),
            "id": file_meta.get("id"),
            "reason": "schema_drift",
            "classification": classification,
            "detail": "TSV header columns do not match target bq_schema.",
            "encoding": encoding,
            "expected_column_count": len(expected_columns),
            "actual_column_count": len(actual_columns),
            "added_columns": added_columns,
            "removed_columns": removed_columns,
            "suggested_schema_patch": (
                suggested_schema_patch(added_columns)
                if classification == "columns_added"
                else None
            ),
        }

    return {
        "valid": True,
        "encoding": encoding,
        "actual_column_count": len(actual_columns),
    }


def slack_message_for_invalid_tsv(target: SyncTarget, invalid_file: Dict[str, Any]) -> str:
    lines = [
        "TSV schema validation failed",
        "",
        f"target: {target.name}",
        f"file: {invalid_file.get('name')}",
        f"reason: {invalid_file.get('reason')}",
        f"classification: {invalid_file.get('classification')}",
        f"expected columns: {invalid_file.get('expected_column_count')}",
        f"actual columns: {invalid_file.get('actual_column_count')}",
    ]

    added_columns = invalid_file.get("added_columns") or []
    removed_columns = invalid_file.get("removed_columns") or []

    if added_columns:
        lines.extend([
            "",
            "Added columns:",
            ", ".join(added_columns),
        ])

    if removed_columns:
        lines.extend([
            "",
            "Removed columns:",
            ", ".join(removed_columns),
        ])

    suggested_patch = invalid_file.get("suggested_schema_patch")
    if suggested_patch:
        lines.extend([
            "",
            "Suggested bq_schema addition:",
            f"```{suggested_patch}```",
        ])
    elif removed_columns:
        lines.extend([
            "",
            "Suggested action:",
            "Reject this file by default. Restore the missing TSV columns, or make an explicit schema migration before allowing this format.",
        ])

    return "\n".join(lines)


def notify_invalid_tsv(target: SyncTarget, invalid_file: Dict[str, Any]) -> None:
    log_payload = {
        "event": "TSV_SCHEMA_VALIDATION_FAILED",
        "target": target.name,
        **invalid_file,
    }
    logger.warning(json.dumps(log_payload, ensure_ascii=False, sort_keys=True))

    if not target.validation.notify_on_invalid:
        return

    webhook_url = get_slack_webhook_url()
    if not webhook_url:
        logger.info(
            "SLACK_NOTIFICATION_SKIPPED event=TSV_SCHEMA_VALIDATION_FAILED target=%s file=%s",
            target.name,
            invalid_file.get("name"),
        )
        return

    try:
        response = requests.post(
            webhook_url,
            json={"text": slack_message_for_invalid_tsv(target, invalid_file)},
            timeout=10,
        )
        response.raise_for_status()
        logger.info(
            "SLACK_NOTIFICATION_SENT event=TSV_SCHEMA_VALIDATION_FAILED target=%s file=%s",
            target.name,
            invalid_file.get("name"),
        )
    except Exception as e:
        logger.error(
            "SLACK_NOTIFICATION_FAILED event=TSV_SCHEMA_VALIDATION_FAILED target=%s file=%s error=%s",
            target.name,
            invalid_file.get("name"),
            e,
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

    log_structured(
        "GCS_UPLOAD_SUCCEEDED",
        target=target.name,
        file_name=file_name,
        gcs_path=f"gs://{GCS_BUCKET}/{blob_path}",
    )
    return blob_path


def invalid_gcs_blob_path(target: SyncTarget, file_name: str) -> str:
    return f"{INVALID_GCS_PREFIX}/{target.name}/{file_name}"


def upload_invalid_tsv(
    bucket,
    target: SyncTarget,
    file_name: str,
    data: bytes,
    validation_result: Dict[str, Any],
) -> Dict[str, str]:
    tsv_blob_path = invalid_gcs_blob_path(target, file_name)
    result_blob_path = f"{tsv_blob_path}.validation.json"

    bucket.blob(tsv_blob_path).upload_from_string(
        data,
        content_type="text/tab-separated-values",
    )
    bucket.blob(result_blob_path).upload_from_string(
        json.dumps(validation_result, ensure_ascii=False, indent=2, sort_keys=True),
        content_type="application/json",
    )

    return {
        "invalid_gcs_path": f"gs://{GCS_BUCKET}/{tsv_blob_path}",
        "validation_gcs_path": f"gs://{GCS_BUCKET}/{result_blob_path}",
    }


def ensure_bq_tables(bq_client: bigquery.Client, target: SyncTarget) -> None:
    for table_id in [target.bq_table_id, target.bq_staging_table_id]:
        table = bigquery.Table(table_id, schema=target.bq_schema)
        try:
            bq_client.get_table(table_id)
        except Exception:
            bq_client.create_table(table)


def load_tsv_to_bq(
    target: SyncTarget,
    data: bytes,
    source_gcs_path: str,
) -> Dict[str, Any]:
    text, _ = decode_tsv_bytes(data)
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

    replace_main_table(bq_client, target)

    return {
        "loaded": True,
        "source_gcs": source_gcs_path,
        "staging_table": target.bq_staging_table_id,
        "target_table": target.bq_table_id,
    }


def bq_column(column_name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column_name):
        raise ValueError(f"Invalid BigQuery column name: {column_name}")
    return f"`{column_name}`"


def replace_main_table(bq_client: bigquery.Client, target: SyncTarget) -> None:
    select_columns = "\n        , ".join(
        bq_column(field.name)
        for field in target.bq_schema
    )

    sql = f"""
create or replace table
    `{target.bq_table_id}`
as
select
        {select_columns}
from
    `{target.bq_staging_table_id}`
"""

    query_job = bq_client.query(sql)
    query_job.result()


def sync_target(drive_service, bucket, target: SyncTarget) -> Dict[str, Any]:
    state = load_state(bucket, target.state_blob)
    files = list_tsv_files(drive_service, target)

    selected_file, file_timestamp = select_latest_drive_file(files)
    if selected_file is None:
        state["last_attempt"] = attempt_payload(
            "skipped",
            reason="No matching timestamped TSV files found in Drive folder.",
        )
        save_state(bucket, target.state_blob, state)
        return {
            "target": target.name,
            "folder_id": target.folder_id,
            "file_name_pattern": target.file_name_pattern,
            "gcs_prefix": target.gcs_prefix,
            "state_blob": target.state_blob,
            "detected": len(files),
            "selected_file": None,
            "transferred": 0,
            "skipped": 1,
            "failed": 0,
            "invalid": 0,
            "transferred_files": [],
            "failed_files": [],
            "invalid_files": [],
            "bigquery": {
                "loaded": False,
                "reason": "No matching timestamped TSV files found in Drive folder.",
            },
        }

    file_id = selected_file["id"]
    file_name = selected_file["name"]
    log_structured(
        "DRIVE_FILE_SELECTED",
        target=target.name,
        drive_file_id=file_id,
        file_name=file_name,
        file_timestamp=file_timestamp,
        modified_time=selected_file.get("modifiedTime"),
        size=selected_file.get("size"),
    )

    selected_file_payload = file_state_payload(selected_file, file_timestamp)

    if last_success_matches(selected_file, file_timestamp, state):
        state["last_attempt"] = attempt_payload(
            "skipped",
            selected_file,
            file_timestamp,
            reason="Latest Drive file already loaded successfully.",
        )
        save_state(bucket, target.state_blob, state)
        return {
            "target": target.name,
            "folder_id": target.folder_id,
            "file_name_pattern": target.file_name_pattern,
            "gcs_prefix": target.gcs_prefix,
            "state_blob": target.state_blob,
            "detected": len(files),
            "selected_file": selected_file_payload,
            "transferred": 0,
            "skipped": 1,
            "failed": 0,
            "invalid": 0,
            "transferred_files": [],
            "failed_files": [],
            "invalid_files": [],
            "bigquery": {
                "loaded": False,
                "reason": "Latest Drive file already loaded successfully.",
            },
        }

    source_gcs_path = None

    try:
        data = download_drive_file(drive_service, file_id)
        validation_result = validate_tsv_file(target, selected_file, data)

        if not validation_result["valid"]:
            invalid_paths = upload_invalid_tsv(
                bucket,
                target,
                file_name,
                data,
                validation_result,
            )
            invalid_file = {
                **validation_result,
                **invalid_paths,
            }
            notify_invalid_tsv(target, invalid_file)
            state["last_attempt"] = attempt_payload(
                "invalid",
                selected_file,
                file_timestamp,
                validation_result=validation_result,
                **invalid_paths,
            )
            save_state(bucket, target.state_blob, state)
            return {
                "target": target.name,
                "folder_id": target.folder_id,
                "file_name_pattern": target.file_name_pattern,
                "gcs_prefix": target.gcs_prefix,
                "state_blob": target.state_blob,
                "detected": len(files),
                "selected_file": selected_file_payload,
                "transferred": 0,
                "skipped": 0,
                "failed": 0,
                "invalid": 1,
                "transferred_files": [],
                "failed_files": [],
                "invalid_files": [invalid_file],
                "bigquery": {
                    "loaded": False,
                    "reason": "TSV schema validation failed.",
                },
            }

        gcs_path = upload_to_gcs(bucket, target, file_name, data)
        source_gcs_path = f"gs://{GCS_BUCKET}/{gcs_path}"
        bq_result = load_tsv_to_bq(target, data, source_gcs_path)

        loaded_at = utc_now_iso()
        state["last_success"] = file_state_payload(
            selected_file,
            file_timestamp,
            gcs_path=source_gcs_path,
            loaded_at=loaded_at,
        )
        state["last_attempt"] = attempt_payload(
            "success",
            selected_file,
            file_timestamp,
            gcs_path=source_gcs_path,
            loaded_at=loaded_at,
            bigquery=bq_result,
        )
        save_state(bucket, target.state_blob, state)

        log_structured(
            "SYNC_TARGET_COMPLETED",
            target=target.name,
            drive_file_id=file_id,
            file_name=file_name,
            file_timestamp=file_timestamp,
            gcs_path=source_gcs_path,
            staging_table=target.bq_staging_table_id,
            target_table=target.bq_table_id,
            loaded_at=loaded_at,
        )

        return {
            "target": target.name,
            "folder_id": target.folder_id,
            "file_name_pattern": target.file_name_pattern,
            "gcs_prefix": target.gcs_prefix,
            "state_blob": target.state_blob,
            "detected": len(files),
            "selected_file": selected_file_payload,
            "transferred": 1,
            "skipped": 0,
            "failed": 0,
            "invalid": 0,
            "transferred_files": [file_name],
            "failed_files": [],
            "invalid_files": [],
            "bigquery": bq_result,
        }

    except Exception as e:
        failed_file = {
            "name": file_name,
            "id": file_id,
            "error": str(e),
        }
        state["last_attempt"] = attempt_payload(
            "failed",
            selected_file,
            file_timestamp,
            error=str(e),
            gcs_path=source_gcs_path,
        )
        save_state(bucket, target.state_blob, state)
        return {
            "target": target.name,
            "folder_id": target.folder_id,
            "file_name_pattern": target.file_name_pattern,
            "gcs_prefix": target.gcs_prefix,
            "state_blob": target.state_blob,
            "detected": len(files),
            "selected_file": selected_file_payload,
            "transferred": 0,
            "skipped": 0,
            "failed": 1,
            "invalid": 0,
            "transferred_files": [],
            "failed_files": [failed_file],
            "invalid_files": [],
            "bigquery": {
                "loaded": False,
                "error": str(e),
            },
            "error": str(e),
        }


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
        "invalid": sum(target_result.get("invalid", 0) for target_result in target_results),
    }

    result = {
        **summary,
        "targets": len(targets),
        "failed_targets": len(failed_targets),
        "results": target_results,
    }

    if len(target_results) == 1:
        target_result = target_results[0]
        result["selected_file"] = target_result.get("selected_file")
        result["transferred_files"] = target_result.get("transferred_files", [])
        result["failed_files"] = target_result.get("failed_files", [])
        result["invalid_files"] = target_result.get("invalid_files", [])
        result["bigquery"] = target_result.get("bigquery", {
            "loaded": False,
            "error": target_result.get("error", "Target failed before BigQuery load."),
        })

    status_code = 500 if failed_targets else 200
    return jsonify(result), status_code
