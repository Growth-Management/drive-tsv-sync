# Drive TSV Sync

Cloud Run service that syncs TSV files from Google Drive to GCS, loads the latest TSV into a BigQuery staging table, and merges it into the target table.

## Flow

Cloud Scheduler -> Cloud Run -> Google Drive -> GCS -> BigQuery staging -> BigQuery target

Cloud Run must stay authenticated. The OAuth Secret Manager secret names are fixed and remain unchanged:

- `drive-oauth-client-id`
- `drive-oauth-client-secret`
- `drive-oauth-refresh-token`

## GCP

- Project: `ice-magapocke-project`
- Region: `asia-northeast1`
- Cloud Run: `drive-tsv-sync`
- Runtime SA: `drive-tsv-runner@ice-magapocke-project.iam.gserviceaccount.com`
- Scheduler SA: `drive-tsv-scheduler@ice-magapocke-project.iam.gserviceaccount.com`
- Container image: `gcr.io/ice-magapocke-project/drive-tsv-sync`

## Sync Targets

Sync targets are defined in `config/sync_targets.json`. The Cloud Run service processes the `targets` array in order.

Each target supports:

- `name`: Optional display name used in responses
- `folder_id`: Google Drive folder ID
- `file_name_pattern`: Optional regular expression for selecting files in the folder
- `gcs_prefix`: GCS prefix where TSV files are uploaded
- `state_blob`: GCS blob path for the per-target sync state JSON
- `bq_dataset`: BigQuery dataset
- `bq_table`: BigQuery merge target table
- `bq_staging_table`: BigQuery staging table loaded from TSV
- `validation`: TSV schema validation settings
- `merge_keys`: Column names used in the BigQuery merge condition
- `bq_schema`: BigQuery schema for this target

The default `top_banner_tsv` target keeps the existing Drive folder, GCS prefix, state blob, BigQuery tables, merge key, and schema. It also filters Drive files with `^top_banner_tsv_download_\d{14}\.tsv$`.

## TSV Validation

Targets default to strict TSV validation. The service validates a downloaded TSV before uploading it to the normal GCS prefix.

```json
"validation": {
  "mode": "strict",
  "header_row_index": 1,
  "notify_on_invalid": true
}
```

In strict mode, the configured header row must match `bq_schema` exactly, including column order. Clear column additions and removals are classified as schema drift. Invalid files are excluded from GCS upload, BigQuery load, and successful state updates.

Validation failures are written to Cloud Logging with event `TSV_SCHEMA_VALIDATION_FAILED`. If `SLACK_WEBHOOK_URL` is set, or `SLACK_WEBHOOK_SECRET` points to a Secret Manager secret containing a webhook URL, the service also posts a Slack notification with the detected diff and a suggested `bq_schema` addition for added columns.

BigQuery state is saved only after the TSV is uploaded and the BigQuery load/merge succeeds. If BigQuery fails, the file remains retryable on the next run.

## Build

```bat
gcloud builds submit --tag gcr.io/ice-magapocke-project/drive-tsv-sync
```

## Deploy

Keep Cloud Run authentication required. `config/sync_targets.json` is copied into the Docker image.
