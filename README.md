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
- `gcs_prefix`: GCS prefix where TSV files are uploaded
- `state_blob`: GCS blob path for the per-target sync state JSON
- `bq_dataset`: BigQuery dataset
- `bq_table`: BigQuery merge target table
- `bq_staging_table`: BigQuery staging table loaded from TSV
- `merge_keys`: Column names used in the BigQuery merge condition
- `bq_schema`: BigQuery schema for this target

The default `top_banner_tsv` target keeps the existing Drive folder, GCS prefix, state blob, BigQuery tables, merge key, and schema.

## Build

```bat
gcloud builds submit --tag gcr.io/ice-magapocke-project/drive-tsv-sync
```

## Deploy

Keep Cloud Run authentication required. `config/sync_targets.json` is copied into the Docker image.
