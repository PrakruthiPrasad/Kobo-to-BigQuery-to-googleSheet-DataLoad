# Kobo Pipeline

A production-grade data pipeline that syncs KoboToolbox survey submissions to Google BigQuery and Google Sheets in real time.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [BigQuery Tables](#bigquery-tables)
- [Prerequisites](#prerequisites)
- [Quick Setup](#quick-setup)
- [Configuration Reference](#configuration-reference)
- [GitHub Secrets](#github-secrets)
- [Deployment](#deployment)
- [Local Development](#local-development)
- [Testing](#testing)
- [Operations](#operations)
- [Troubleshooting](#troubleshooting)

---

## Overview

The Kobo Pipeline connects KoboToolbox survey forms to BigQuery and Google Sheets automatically. Every new form submission is processed in real time via webhook and appears in Google Sheets within 15 seconds. A nightly sync job acts as a safety net to catch any missed submissions.

**Key features:**

- Real-time webhook processing — new submissions appear in under 15 seconds
- Automatic BigQuery table creation and schema evolution as form fields change
- Google Sheets updated automatically via webhook append and nightly full refresh
- Test submission filtering — entries containing test keywords are quarantined, never mixed with real data
- Duplicate detection — every submission is processed exactly once
- Full audit trail via `pipeline_runs` table
- Zero-downtime deployments via GitHub Actions CI/CD
- Scales to zero when idle — no idle compute costs

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        KoboToolbox                              │
│                    (Survey Form Submission)                      │
└──────────────────────────┬──────────────────────────────────────┘
                           │  HTTP POST (REST Hook)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│              Cloud Run — Webhook Receiver                        │
│                  (kobo-webhook-receiver)                         │
│                                                                  │
│  1. Validate X-Kobo-Webhook-Secret header                        │
│  2. Check processed_ids — skip if duplicate                      │
│  3. Filter test submissions → quarantine table                   │
│  4. Transform and flatten nested fields                          │
│  5. Create BQ tables if first run                                │
│  6. Streaming insert → kobo_post_survey                          │
│  7. Append new row → Google Sheet                                │
│  8. Send email notification                                      │
└──────────────────────────┬──────────────────────────────────────┘
                           │  Streaming Insert
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                      BigQuery                                    │
│                    (kobo_data dataset)                           │
│                                                                  │
│   kobo_post_survey          ← production data                   │
│   kobo_post_survey_staging  ← nightly sync staging              │
│   kobo_post_survey_quarantine ← test/invalid submissions        │
│   pipeline_runs             ← audit log                         │
│   pipeline_state            ← sheet ID, first-run state         │
│   processed_ids             ← deduplication store               │
│   schema_changelog          ← form field change history         │
└──────────────────────────┬──────────────────────────────────────┘
                           │  Query all rows
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Google Sheets                                  │
│               (Kobo Pipeline Data — Survey Data tab)             │
│                                                                  │
│   • Webhook: appends new row in real time                        │
│   • Nightly sync: overwrites sheet with all BQ data              │
│     (ensures sheet always mirrors BigQuery exactly)              │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│              Cloud Run Job — Nightly Sync                        │
│                   (kobo-full-sync)                               │
│              Triggered by Cloud Scheduler at 2AM                │
│                                                                  │
│  Step 1: Fetch ALL submissions from KoboToolbox API             │
│  Step 2: Transform and filter test submissions                   │
│  Step 3: Check processed_ids — skip already loaded              │
│  Step 4: Batch load new rows → staging table                     │
│  Step 5: Schema evolution → promote to production                │
│  Step 6: Overwrite Google Sheet with all BigQuery data           │
│  Step 7: Send notification email if new rows found               │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                     GitHub Actions CI/CD                         │
│                                                                  │
│  On every push to main:                                          │
│  1. Build Docker test image → run pytest (105+ tests)           │
│  2. Build production Docker image (--no-cache)                   │
│  3. Push to Artifact Registry                                    │
│  4. Deploy kobo-webhook-receiver (Cloud Run Service)            │
│  5. Deploy kobo-full-sync (Cloud Run Job)                        │
└─────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
kobo-pipeline/
├── .github/
│   └── workflows/
│       └── deploy.yml          # CI/CD — test, build, deploy on push to main
├── services/
│   ├── webhook/
│   │   └── main.py             # FastAPI webhook receiver (real-time)
│   └── sync/
│       └── main.py             # Nightly full reconciliation job
├── shared/
│   ├── config.py               # Environment variable loading
│   ├── fetcher.py              # KoboToolbox API client
│   ├── transformer.py          # Data cleaning, test filtering, repeat groups
│   ├── loader.py               # BigQuery load operations (streaming + batch)
│   ├── schema_manager.py       # BQ schema inference and evolution
│   ├── sheets_writer.py        # Google Sheets write operations
│   └── alerting.py             # Error alerting
├── tests/                      # Pytest test suite (105+ tests)
├── Dockerfile                  # Production image
├── Dockerfile.test             # Test image
├── docker-compose.test.yml     # Local test runner
├── env.yaml.example            # Environment variable template
└── README.md
```

---

## BigQuery Tables

All tables live in the `kobo_data` dataset and are created automatically on first run.

| Table | Type | Purpose | When to check |
|---|---|---|---|
| `kobo_post_survey` | Production | Clean validated submissions | Query for real data |
| `kobo_post_survey_staging` | Staging | Temporary landing zone for nightly sync | Usually empty between syncs |
| `kobo_post_survey_quarantine` | Quarantine | Test submissions and validation failures | If a submission seems missing |
| `pipeline_runs` | Audit | Every pipeline run with status and row counts | First place to check on any error |
| `pipeline_state` | State | Google Sheet ID, URL, first-run flags | To get the Sheet URL |
| `processed_ids` | Deduplication | Every processed submission ID | If submissions are being skipped |
| `schema_changelog` | History | Form field changes over time | To track form evolution |

### Useful diagnostic queries

```sql
-- Check last 5 pipeline runs
SELECT status, rows_fetched, rows_loaded, error_message, source, started_at
FROM `YOUR_PROJECT.kobo_data.pipeline_runs`
ORDER BY started_at DESC LIMIT 5;

-- Get Google Sheet URL
SELECT sheet_id, sheet_url FROM `YOUR_PROJECT.kobo_data.pipeline_state`;

-- Check for duplicates
SELECT COUNT(*) as total, COUNT(DISTINCT _id) as unique_ids
FROM `YOUR_PROJECT.kobo_data.kobo_post_survey`;

-- Find quarantined submissions
SELECT quarantine_reason, quarantine_at
FROM `YOUR_PROJECT.kobo_data.kobo_post_survey_quarantine`
ORDER BY quarantine_at DESC LIMIT 10;
```

---

## Prerequisites

### Accounts and access required

| Account | Required for |
|---|---|
| KoboToolbox account | Access to the survey form and API token |
| GCP project with billing enabled | BigQuery, Cloud Run, Secret Manager, Artifact Registry |
| GitHub account with repo write access | CI/CD via GitHub Actions |
| Google Workspace account | Domain-Wide Delegation for Google Sheets creation |
| Google Workspace Admin access | Authorising Domain-Wide Delegation in admin.google.com |

### Local software

| Tool | Version | Install |
|---|---|---|
| Git | 2.x+ | git-scm.com |
| Google Cloud CLI | Latest | cloud.google.com/sdk |
| Docker Desktop | Latest | docker.com/products/docker-desktop |
| Python | 3.11+ | python.org (optional, for local dev) |

---

## Quick Setup

### Step 1 — Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/YOUR_REPO.git
cd YOUR_REPO
```

### Step 2 — GCP setup (one time per project)

```bash
# Enable required APIs
gcloud services enable bigquery.googleapis.com run.googleapis.com cloudscheduler.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com secretmanager.googleapis.com sheets.googleapis.com drive.googleapis.com

# Create Artifact Registry repository
gcloud artifacts repositories create kobo-pipeline --repository-format=docker --location=us-west1

# Create service account
gcloud iam service-accounts create kobo-pipeline-sa --display-name="Kobo Pipeline SA"

# Grant required roles
gcloud projects add-iam-policy-binding YOUR_PROJECT --member="serviceAccount:kobo-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com" --role="roles/bigquery.dataEditor"
gcloud projects add-iam-policy-binding YOUR_PROJECT --member="serviceAccount:kobo-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com" --role="roles/bigquery.jobUser"
gcloud projects add-iam-policy-binding YOUR_PROJECT --member="serviceAccount:kobo-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com" --role="roles/run.admin"
gcloud projects add-iam-policy-binding YOUR_PROJECT --member="serviceAccount:kobo-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com" --role="roles/iam.serviceAccountUser"
gcloud projects add-iam-policy-binding YOUR_PROJECT --member="serviceAccount:kobo-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com" --role="roles/secretmanager.secretAccessor"
gcloud projects add-iam-policy-binding YOUR_PROJECT --member="serviceAccount:kobo-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com" --role="roles/artifactregistry.writer"
gcloud projects add-iam-policy-binding YOUR_PROJECT --member="serviceAccount:kobo-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com" --role="roles/run.invoker"

# Create service account JSON key
gcloud iam service-accounts keys create kobo-sa-key.json --iam-account=kobo-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com

# Store secrets in Secret Manager
gcloud secrets create KOBO_API_TOKEN --replication-policy=automatic
echo -n "YOUR_KOBO_TOKEN" | gcloud secrets versions add KOBO_API_TOKEN --data-file=-

gcloud secrets create kobo-webhook-secret --replication-policy=automatic
echo -n "YOUR_WEBHOOK_SECRET" | gcloud secrets versions add kobo-webhook-secret --data-file=-

gcloud secrets create kobo-sa-key --replication-policy=automatic
gcloud secrets versions add kobo-sa-key --data-file="kobo-sa-key.json"

# Grant service account access to secrets
gcloud secrets add-iam-policy-binding KOBO_API_TOKEN --member="serviceAccount:kobo-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com" --role="roles/secretmanager.secretAccessor"
gcloud secrets add-iam-policy-binding kobo-webhook-secret --member="serviceAccount:kobo-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com" --role="roles/secretmanager.secretAccessor"
gcloud secrets add-iam-policy-binding kobo-sa-key --member="serviceAccount:kobo-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com" --role="roles/secretmanager.secretAccessor"
```

### Step 3 — Domain-Wide Delegation (for Google Sheets creation)

1. Get the service account Client ID:
```bash
gcloud iam service-accounts describe kobo-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com --format="value(uniqueId)"
```

2. Go to [admin.google.com](https://admin.google.com) → Security → API controls → Manage domain-wide delegation → Add new
3. Enter the Client ID and these scopes:
```
https://www.googleapis.com/auth/drive,https://www.googleapis.com/auth/spreadsheets,https://spreadsheets.google.com/feeds
```

### Step 4 — Google Drive folder

1. Log in to [drive.google.com](https://drive.google.com) with your Workspace account
2. Create a folder named `Kobo Pipeline Data`
3. Share it with `kobo-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com` as Editor
4. Copy the Folder ID from the URL: `https://drive.google.com/drive/folders/FOLDER_ID_HERE`

### Step 5 — Cloud Scheduler (nightly sync at 2AM Pacific)

```bash
gcloud scheduler jobs create http kobo-nightly-sync \
  --location=us-west1 \
  --schedule="0 2 * * *" \
  --uri=https://us-west1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/YOUR_PROJECT/jobs/kobo-full-sync:run \
  --message-body="{}" \
  --oauth-service-account-email=kobo-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com \
  --time-zone="America/Los_Angeles"
```

### Step 6 — Configure GitHub Secrets

Go to **GitHub → repo → Settings → Secrets and Variables → Actions** and add all secrets listed in the [GitHub Secrets](#github-secrets) section below.

### Step 7 — Deploy

```bash
git add .
git commit -m "Initial deployment"
git push origin main
```

GitHub Actions will automatically run tests, build the Docker image, and deploy both services.

### Step 8 — Get the webhook URL

After deployment completes:

```bash
gcloud run services describe kobo-webhook-receiver --region=us-west1 --format="value(status.url)"
```

Your webhook endpoint is the URL with `/webhook` appended:
```
https://kobo-webhook-receiver-xxxxxxxx-uw.a.run.app/webhook
```

### Step 9 — Configure KoboToolbox REST Hook

1. Open your form in KoboToolbox → Settings → REST Services → Add Service
2. Set Endpoint URL to the webhook URL from Step 8
3. Add custom header: `X-Kobo-Webhook-Secret` with the value from your `kobo-webhook-secret` Secret Manager secret
4. Save

### Step 10 — First submission and save Sheet ID

1. Submit a real form entry in KoboToolbox
2. Wait 15 seconds — check your email for the Google Sheet link
3. Get the Sheet ID from BigQuery:
```sql
SELECT sheet_id FROM `YOUR_PROJECT.kobo_data.pipeline_state`;
```
4. Update the `SHEET_ID` GitHub Secret with the real Sheet ID
5. Push any change to trigger redeployment:
```bash
echo "" >> README.md && git add README.md && git commit -m "Save Sheet ID" && git push origin main
```

---

## Configuration Reference

Copy `env.yaml.example` to `env.yaml` and fill in all values. This file is gitignored and used for local development only. GitHub Actions uses GitHub Secrets for production.

| Variable | Description | Example |
|---|---|---|
| `BQ_PROJECT` | GCP project ID | `contactus-form-test` |
| `BQ_DATASET` | BigQuery dataset name | `kobo_data` |
| `BQ_TABLE` | BigQuery table name | `kobo_post_survey` |
| `FORM_UID` | KoboToolbox form Asset UID | `aHRk3TGZLzu2TjtWwMvjBa` |
| `KOBO_BASE_URL` | KoboToolbox base URL | `https://kf.kobotoolbox.org` |
| `SHEET_ID` | Google Sheet ID (blank on first run) | `` |
| `SHEET_NAME` | Google Sheet filename | `Kobo Pipeline Data` |
| `SHEET_TAB` | Sheet tab name | `Survey Data` |
| `SHARED_DRIVE_FOLDER_ID` | Google Drive folder ID | `1ABC...xyz` |
| `DELEGATED_EMAIL` | Workspace email for sheet creation | `you@yourdomain.com` |
| `SA_KEY_SECRET` | Secret Manager secret name for SA key | `kobo-sa-key` |
| `TEAM_EMAILS` | Comma-separated emails for first-run notification | `a@org.com,b@org.com` |
| `NEW_ENTRY_NOTIFY_EMAILS` | Comma-separated emails for new entry notifications | `a@org.com` |
| `SYNC_MODE` | BigQuery write mode for sync | `append` |
| `MAX_SHEET_ROWS` | Maximum rows in Google Sheet | `10000` |
| `TEST_KEYWORDS` | Comma-separated keywords that trigger quarantine | `test,testing,demo,sample,dummy` |
| `ALERT_EMAILS` | Comma-separated emails for pipeline failure alerts | `a@org.com` |

---

## GitHub Secrets

Add all of the following at **GitHub → repo → Settings → Secrets and Variables → Actions**:

### GCP Authentication

| Secret | Value |
|---|---|
| `GCP_SA_KEY` | Full contents of `kobo-sa-key.json` |
| `GCP_PROJECT` | Your GCP project ID |
| `GCP_REGION` | `us-west1` |
| `GCP_SA_EMAIL` | `kobo-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com` |

### Pipeline Configuration

| Secret | Value |
|---|---|
| `BQ_PROJECT` | Your GCP project ID |
| `BQ_DATASET` | `kobo_data` |
| `BQ_TABLE` | `kobo_post_survey` |
| `FORM_UID` | Your KoboToolbox Asset UID |
| `SHEET_ID` | `none` (update after first run) |
| `SHEET_NAME` | `Kobo Pipeline Data` |
| `SHEET_TAB` | `Survey Data` |
| `SHARED_DRIVE_FOLDER_ID` | Your Google Drive folder ID |
| `DELEGATED_EMAIL` | Your Workspace email |
| `SA_KEY_SECRET` | `kobo-sa-key` |
| `SYNC_MODE` | `append` |
| `MAX_SHEET_ROWS` | `10000` |
| `TEST_KEYWORDS` | `test,testing,demo,sample,dummy` |
| `TEAM_EMAILS` | Comma-separated team emails |
| `NEW_ENTRY_NOTIFY_EMAILS` | Comma-separated notification emails |
| `ALERT_EMAILS` | Comma-separated alert emails |

---

## Deployment

Deployment is fully automated via GitHub Actions on every push to `main`.

```
Push to main
     ↓
Run tests (pytest — must pass)
     ↓
Build Docker image (--no-cache)
     ↓
Push to Artifact Registry
     ↓
Deploy kobo-webhook-receiver (Cloud Run Service)
Deploy kobo-full-sync (Cloud Run Job)
```

### Manual deployment (emergency only)

```bash
# Build and push image
gcloud builds submit --tag us-west1-docker.pkg.dev/YOUR_PROJECT/kobo-pipeline/kobo-pipeline:latest .

# Deploy webhook
gcloud run deploy kobo-webhook-receiver \
  --image=us-west1-docker.pkg.dev/YOUR_PROJECT/kobo-pipeline/kobo-pipeline:latest \
  --region=us-west1 --min-instances=0 --platform=managed \
  --allow-unauthenticated --env-vars-file=env.yaml \
  --service-account=kobo-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com \
  --set-secrets=KOBO_TOKEN=KOBO_API_TOKEN:latest,KOBO_WEBHOOK_SECRET=kobo-webhook-secret:latest

# Update sync job
gcloud run jobs update kobo-full-sync \
  --image=us-west1-docker.pkg.dev/YOUR_PROJECT/kobo-pipeline/kobo-pipeline:latest \
  --region=us-west1 --env-vars-file=env.yaml \
  --service-account=kobo-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com \
  --set-secrets=KOBO_TOKEN=KOBO_API_TOKEN:latest
```

---

## Local Development

### Run tests

```bash
docker compose -f docker-compose.test.yml build --no-cache
docker compose -f docker-compose.test.yml up
# Expected: 105+ passed, 0 failed
```

### Authenticate locally

```bash
gcloud auth login
gcloud config set project YOUR_PROJECT
gcloud auth application-default login
gcloud auth application-default set-quota-project YOUR_PROJECT
```

### Trigger nightly sync manually

```bash
gcloud run jobs execute kobo-full-sync --region=us-west1
```

---

## Testing

The test suite covers all shared modules with 105+ tests.

```bash
# Run all tests
docker compose -f docker-compose.test.yml up

# Test modules
tests/test_transformer.py      # Data transformation and test filtering
tests/test_loader.py           # BigQuery load operations
tests/test_fetcher.py          # KoboToolbox API client
tests/test_sheets_writer.py    # Google Sheets operations
tests/test_schema_manager.py   # BQ schema inference and evolution
tests/test_idempotency.py      # Duplicate detection
tests/test_webhook_receiver.py # Webhook secret validation
```

### End-to-end test checklist

- [ ] Submit a real KoboToolbox form entry (no test keywords)
- [ ] Entry appears in `kobo_post_survey` BigQuery table within 15 seconds
- [ ] Entry appears in Google Sheet within 15 seconds
- [ ] Notification email received
- [ ] Submit a test entry (with keyword `test`) — confirm it does NOT appear in production table
- [ ] Test entry appears in `kobo_post_survey_quarantine` table
- [ ] `pipeline_runs` table shows `status = ok`
- [ ] Trigger nightly sync manually — sheet refreshes with all data

---

## Operations

### Check logs

```bash
# Webhook logs
gcloud logging read "resource.labels.service_name=kobo-webhook-receiver" --project=YOUR_PROJECT --limit=20 --format="value(timestamp,textPayload)"

# Sync job logs
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=kobo-full-sync" --project=YOUR_PROJECT --limit=20 --format="value(timestamp,textPayload)"
```

### Check service account roles

```bash
gcloud projects get-iam-policy YOUR_PROJECT \
  --flatten="bindings[].members" \
  --filter="bindings.members:kobo-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com" \
  --format="table(bindings.role)"
```

### Check deployed services

```bash
gcloud run services list --region=us-west1
gcloud run jobs list --region=us-west1
```

### Morning health check query

```sql
SELECT status, rows_fetched, rows_loaded, error_message, started_at
FROM `YOUR_PROJECT.kobo_data.pipeline_runs`
WHERE DATE(started_at) = CURRENT_DATE()
ORDER BY started_at DESC;
```

---

## Troubleshooting

| Symptom | Where to check | Fix |
|---|---|---|
| Submission not in BigQuery | `pipeline_runs` → check `error_message` | Check webhook logs for root cause |
| Submission in quarantine | `kobo_post_survey_quarantine` → check `quarantine_reason` | Remove test keyword from submission |
| Google Sheet not created | `pipeline_state` → check `first_run_at` | Verify Domain-Wide Delegation is set up |
| Email not received | `pipeline_state` → check `emails_sent` | Check spam folder, verify `TEAM_EMAILS` secret |
| Webhook returning 401 | KoboToolbox REST Services | `X-Kobo-Webhook-Secret` header doesn't match Secret Manager value |
| Drive quota exceeded | Cloud Run webhook logs | Domain-Wide Delegation not configured — follow Step 3 in setup |
| Deployment failing | GitHub Actions → failed job | Check error in step logs, verify all GitHub Secrets are set |
| Duplicates in sheet | Run sync manually | Sync job overwrites sheet from BigQuery — duplicates are resolved |
| `processed_ids` silently returning empty | Webhook/sync logs show 403 | Service account missing `bigquery.readSessionUser` role |

---

## For Non-Developer Team Members

You do not need to clone this repository or install anything. Simply:

1. Open the KoboToolbox form link shared with you
2. Fill in and submit the form
3. Data appears in the Google Sheet within 15 seconds
4. You will receive an email with the sheet link on your first submission

> **Note:** Submissions containing the words `test`, `testing`, `demo`, `sample`, or `dummy` are automatically filtered and will not appear in the Google Sheet. Use these keywords freely when testing the form.

---

*Kobo Pipeline — Built for NPOs. Maintained internally.*
