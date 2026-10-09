#!/usr/bin/env bash
# Build the jobs image once and create-or-update two Cloud Run jobs + schedulers:
#   nba-espn-ingest   python espn_ingest.py daily   (yesterday's games + today's ownership)
#   nba-api-healthcheck   python healthcheck.py         (fails the run if the pipeline is unhealthy)
#
# Additive: writes only to the *_espn tables. Does not touch nba-daily-ingest.
#
# Usage:  ./deploy_espn_jobs.sh
# Optional env: PROJECT_ID REGION SERVICE_ACCOUNT IMAGE_TAG SKIP_BUILD=1 (reuse an existing IMAGE_TAG)
# Optional secret: a Secret Manager secret named `discord-webhook-url` is mounted
# into the health check as DISCORD_WEBHOOK_URL when it exists.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ID=${PROJECT_ID:-fantasy-survivor-app}
REGION=${REGION:-us-central1}
SERVICE_ACCOUNT=${SERVICE_ACCOUNT:-nba-gbq-sa@${PROJECT_ID}.iam.gserviceaccount.com}
IMAGE_TAG=${IMAGE_TAG:-$(date +%Y%m%d%H%M%S)}
IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/nba-jobs/nba-espn-jobs:${IMAGE_TAG}"

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  echo "Building ${IMAGE_URI}..."
  gcloud builds submit "${SCRIPT_DIR}" --project "${PROJECT_ID}" --tag "${IMAGE_URI}"
else
  echo "SKIP_BUILD=1: reusing ${IMAGE_URI}"
fi

upsert_job() {  # name script args timeout
  local name="$1" script="$2" args="$3" timeout="$4"; shift 4
  local extra=("$@")
  if gcloud run jobs describe "${name}" --project "${PROJECT_ID}" --region "${REGION}" >/dev/null 2>&1; then
    verb=update
  else
    verb=create
  fi
  local arglist="${script}"
  [[ -n "${args}" ]] && arglist="${script},${args}"
  gcloud run jobs "${verb}" "${name}" --project "${PROJECT_ID}" --region "${REGION}" \
    --image "${IMAGE_URI}" --service-account "${SERVICE_ACCOUNT}" \
    --command python --args "${arglist}" \
    --task-timeout "${timeout}" --max-retries 1 --memory 1Gi \
    --set-env-vars "PROJECT_ID=${PROJECT_ID}" ${extra[@]+"${extra[@]}"}
}

upsert_schedule() {  # job_name schedule
  local job="$1" cron="$2" sched="${1}-schedule"
  local uri="https://run.googleapis.com/v2/projects/${PROJECT_ID}/locations/${REGION}/jobs/${job}:run"
  local flags=(--project "${PROJECT_ID}" --location "${REGION}" --schedule "${cron}"
               --time-zone "America/New_York" --uri "${uri}" --http-method POST
               --oauth-service-account-email "${SERVICE_ACCOUNT}")
  if gcloud scheduler jobs describe "${sched}" --project "${PROJECT_ID}" --location "${REGION}" >/dev/null 2>&1; then
    gcloud scheduler jobs update http "${sched}" "${flags[@]}"
  else
    gcloud scheduler jobs create http "${sched}" "${flags[@]}"
  fi
}

upsert_job nba-espn-ingest espn_ingest.py daily 900s
upsert_schedule nba-espn-ingest "30 2 * * *"     # after games are final; SportsBlaze runs at 01:00 ET

HC_EXTRA=()
if gcloud secrets describe discord-webhook-url --project "${PROJECT_ID}" >/dev/null 2>&1; then
  HC_EXTRA=(--set-secrets "DISCORD_WEBHOOK_URL=discord-webhook-url:latest")
fi
upsert_job nba-api-healthcheck healthcheck.py "" 300s ${HC_EXTRA[@]+"${HC_EXTRA[@]}"}
upsert_schedule nba-api-healthcheck "0 8 * * *"      # after both ingests

echo
echo "Deployed nba-espn-ingest (02:30 ET) and nba-api-healthcheck (08:00 ET) using ${IMAGE_URI}"
