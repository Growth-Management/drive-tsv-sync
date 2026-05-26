# Drive TSV Sync

Google Drive上のTSVを差分検知してGCSへ転送し、最新TSVをBigQueryへ同期するCloud Runアプリ。

## Flow

Cloud Scheduler -> Cloud Run -> Google Drive -> GCS -> BigQuery staging -> BigQuery target

## GCP

- Project: `ice-magapocke-project`
- Region: `asia-northeast1`
- Cloud Run: `drive-tsv-sync`
- Runtime SA: `drive-tsv-runner@ice-magapocke-project.iam.gserviceaccount.com`
- Scheduler SA: `drive-tsv-scheduler@ice-magapocke-project.iam.gserviceaccount.com`

## Build

```bat
gcloud builds submit --tag gcr.io/ice-magapocke-project/drive-tsv-sync