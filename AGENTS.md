# AGENTS.md

## Project overview

This repository manages a Cloud Run job that synchronizes TSV files from Google Drive to Google Cloud Storage, then loads the latest TSV into BigQuery.

Current flow:

1. Cloud Scheduler calls Cloud Run.
2. Cloud Run uses OAuth refresh token secrets to access Google Drive.
3. Changed or newly added TSV files are uploaded to GCS.
4. The latest TSV file is selected by timestamp in the filename.
5. TSV is converted from UTF-16 to UTF-8.
6. Data is loaded into a BigQuery staging table.
7. The staging table is merged into the target BigQuery table.

## Runtime

- Python 3.11
- Flask
- Gunicorn
- Google Cloud Run
- Google Cloud Scheduler
- Google Cloud Storage
- Google Secret Manager
- Google Drive API
- BigQuery

## Deployment target

- GCP project: `ice-magapocke-project`
- Region: `asia-northeast1`
- Cloud Run service: `drive-tsv-sync`
- Runtime service account: `drive-tsv-runner@ice-magapocke-project.iam.gserviceaccount.com`
- Scheduler service account: `drive-tsv-scheduler@ice-magapocke-project.iam.gserviceaccount.com`
- Container image: `gcr.io/ice-magapocke-project/drive-tsv-sync`

## Important commands

Build:

```bat
gcloud builds submit --tag gcr.io/ice-magapocke-project/drive-tsv-sync