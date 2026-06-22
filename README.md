# Drive TSV Sync

Cloud Run service that syncs the latest timestamped TSV file from Google Drive to
GCS, loads it into a BigQuery staging table, and replaces the target table from
that staging snapshot.

## Flow

Cloud Scheduler -> Cloud Run -> Google Drive -> GCS -> BigQuery staging ->
BigQuery target

Cloud Run must stay authenticated. The OAuth Secret Manager secret names are
fixed and remain unchanged:

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

## Deploy

Deploys are handled by `.github/workflows/deploy-cloud-run.yml`.

The workflow runs on:

- pushes to `main`
- manual `workflow_dispatch`

Required GitHub Secrets:

- `GCP_WORKLOAD_IDENTITY_PROVIDER`: Workload Identity Provider resource name
- `GCP_SERVICE_ACCOUNT`: deploy service account email used by GitHub Actions

The deploy service account needs permissions to:

- build and push the container image with Cloud Build / Container Registry
- deploy `drive-tsv-sync` to Cloud Run
- act as `drive-tsv-runner@ice-magapocke-project.iam.gserviceaccount.com`

The workflow uses immutable image tags based on `${{ github.sha }}` and keeps
Cloud Run authentication required. It does not add public access.

For agent-driven deploys, run the `Deploy Cloud Run` GitHub Actions workflow on
`main`, then inspect the workflow run and the final `Verify deployed revision`
step. The verification step prints the latest ready revision and service URL.

## Sync Targets

Sync targets are defined in `config/sync_targets.json`. The Cloud Run service
processes the `targets` array in order.

Each target supports:

- `name`: Optional display name used in responses
- `folder_id`: Google Drive folder ID
- `file_name_pattern`: Optional regular expression for selecting files in the folder
- `gcs_prefix`: GCS prefix where valid TSV files are uploaded
- `state_blob`: GCS blob path for the per-target sync state JSON
- `bq_dataset`: BigQuery dataset
- `bq_table`: BigQuery snapshot target table
- `bq_staging_table`: BigQuery staging table loaded from TSV
- `validation`: TSV schema validation settings
- `bq_schema`: BigQuery schema for this target

The default `top_banner_tsv` target filters Drive files with
`^top_banner_tsv_download_\d{14}\.tsv$`. The timestamp in the file name selects
the latest Drive file to process.

## TSV Validation

Targets default to strict TSV validation. The service validates a downloaded TSV
before uploading it to the normal GCS prefix.

```json
"validation": {
  "mode": "strict",
  "header_row_index": 1,
  "notify_on_invalid": true
}
```

In strict mode, the configured header row must match `bq_schema` exactly,
including column order. Clear column additions and removals are classified as
schema drift. Invalid files are excluded from the normal GCS prefix, BigQuery
load, and successful state updates.

Validation failures are written to Cloud Logging with event
`TSV_SCHEMA_VALIDATION_FAILED`. If `SLACK_WEBHOOK_URL` is set, or
`SLACK_WEBHOOK_SECRET` points to a Secret Manager secret containing a webhook
URL, the service also posts a Slack notification with the detected diff and a
suggested `bq_schema` addition for added columns.

Invalid TSV files are stored under `invalid/top_banner_tsv/` with their
validation result JSON. BigQuery state is saved to `last_success` only after the
TSV is uploaded, staging is loaded, and the target table replacement succeeds.
If validation or BigQuery fails, `last_success` remains unchanged and the latest
file remains retryable on the next run.
