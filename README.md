# Kobo → BigQuery → Google Sheets Pipeline

Real-time data pipeline for KoboToolbox survey forms.

## Quick start

```bash
# 1. Clone the repo
git clone <your-repo-url>
cd kobo-pipeline

# 2. Set up environment
cp .env.example .env
# Fill in your values in .env

# 3. Run tests
docker compose -f docker-compose.test.yml up

# 4. Run the full sync locally
docker compose up sync

# 5. Run the webhook receiver locally
docker compose up webhook
# Then test it:
curl -X POST http://localhost:8080/webhook \
  -H "Content-Type: application/json" \
  -H "X-Kobo-Webhook-Secret: your-secret" \
  -d '{"_id": 999, "Full Name": "Test User"}'
```

## Adding a new form

1. Create a new Cloud Run Job pointing at the same Docker image
2. Set `FORM_UID`, `BQ_TABLE`, `SHEET_NAME`, `TEAM_EMAILS` as env vars
3. Add a Cloud Scheduler trigger for the new job
4. Configure a Kobo REST Hook pointing to the webhook receiver URL
5. No code changes needed

## Project structure

```
kobo-pipeline/
├── shared/          # Core business logic — used by both services
│   ├── fetcher.py       Kobo API fetch, pagination, retry
│   ├── transformer.py   Cleaning, test filtering, repeat groups
│   ├── schema_manager.py BQ schema inference and evolution
│   ├── loader.py        BQ load operations, idempotency, quarantine
│   ├── sheets_writer.py  Sheet writes, notifications
│   ├── alerting.py      Failure alerts
│   └── config.py        Environment variable loading
├── services/
│   ├── webhook/     Cloud Run Service — real-time webhook receiver
│   └── sync/        Cloud Run Job — nightly full reconciliation
├── tests/           Full pytest test suite
├── docker-compose.yml
├── docker-compose.test.yml
└── Dockerfile.test
```

## Environment variables

See `.env.example` for the full list. Key variables:

| Variable | Description |
|---|---|
| `KOBO_TOKEN` | KoboToolbox API token (from Account Settings → API) |
| `FORM_UID` | Form asset UID (from form URL or Settings) |
| `BQ_PROJECT` | GCP project ID |
| `BQ_TABLE` | BigQuery table name |
| `TEAM_EMAILS` | Comma-separated emails — first-run share only |
| `NEW_ENTRY_NOTIFY_EMAILS` | Comma-separated emails — every new submission |
| `TEST_KEYWORDS` | Comma-separated keywords to quarantine test entries |
