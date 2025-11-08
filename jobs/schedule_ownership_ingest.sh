#!/usr/bin/env bash
# Configure a Cloud Scheduler job that runs the nba-ownership-ingest Cloud Run job
# every day at 1:00 AM Eastern Time.
#
# Usage:
#   chmod +x schedule_ownership_ingest.sh
#   ./schedule_ownership_ingest.sh
#
# Optional environment variables:
#   PROJECT_ID               (default: fantasy-survivor-app)
#   REGION                   (default: us-central1)
#   JOB_NAME                 (default: nba-ownership-ingest)
#   SCHEDULER_JOB_NAME       (default: nba-ownership-ingest-daily)
#   SCHEDULER_LOCATION       (default: us-central1)
#   SCHEDULE                 (default: "0 1 * * *" -> 1:00 AM America/New_York)
#   TIME_ZONE                (default: America/New_York)
#   SCHEDULER_SERVICE_ACCOUNT (required; service account used for Scheduler invocations)
#   OAUTH_SCOPE              (default: https://www.googleapis.com/auth/cloud-platform)
#
# The script will create or update an HTTP Cloud Scheduler job that posts directly to the
# Cloud Run Jobs API endpoint responsible for running the target job.

set -euo pipefail

PROJECT_ID=${PROJECT_ID:-fantasy-survivor-app}
REGION=${REGION:-us-central1}
JOB_NAME=${JOB_NAME:-nba-ownership-ingest}
SCHEDULER_JOB_NAME=${SCHEDULER_JOB_NAME:-${JOB_NAME}-daily}
SCHEDULER_LOCATION=${SCHEDULER_LOCATION:-us-central1}
SCHEDULE=${SCHEDULE:-"0 1 * * *"}
TIME_ZONE=${TIME_ZONE:-America/New_York}
OAUTH_SCOPE=${OAUTH_SCOPE:-"https://www.googleapis.com/auth/cloud-platform"}

if ! gcloud run jobs describe "${JOB_NAME}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" >/dev/null 2>&1; then
  cat <<EOF >&2
Cloud Run job "${JOB_NAME}" was not found in project ${PROJECT_ID} (${REGION}).
Deploy the job first, then configure Cloud Scheduler.
EOF
  exit 1
fi

resolve_service_account() {
  if [[ -n "${SCHEDULER_SERVICE_ACCOUNT:-}" ]]; then
    echo "${SCHEDULER_SERVICE_ACCOUNT}"
    return
  fi

# Remove resolve_service_account + related checks
if ! gcloud run jobs describe "${JOB_NAME}" --project "${PROJECT_ID}" --region "${REGION}" >/dev/null 2>&1; then
  >&2 echo "Cloud Run job \"${JOB_NAME}\" was not found in project ${PROJECT_ID} (${REGION})."
  exit 1
fi

RUN_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run"

COMMON_ARGS=(
  "--project" "${PROJECT_ID}"
  "--location" "${SCHEDULER_LOCATION}"
  "--schedule" "${SCHEDULE}"
  "--time-zone" "${TIME_ZONE}"
  "--uri" "${RUN_URI}"
  "--http-method" "POST"
  "--headers" "Content-Type=application/json"
  "--message-body" "{}"
  "--oauth-service-account-email" "${SCHEDULER_SERVICE_ACCOUNT}"
  "--oauth-token-scope" "${OAUTH_SCOPE}"
)

if gcloud scheduler jobs describe "${SCHEDULER_JOB_NAME}" --project "${PROJECT_ID}" --location "${SCHEDULER_LOCATION}" >/dev/null 2>&1; then
  echo "Updating existing Cloud Scheduler job ${SCHEDULER_JOB_NAME}..."
  gcloud scheduler jobs update http "${SCHEDULER_JOB_NAME}" "${COMMON_ARGS[@]}"
else
  echo "Creating Cloud Scheduler job ${SCHEDULER_JOB_NAME}..."
  gcloud scheduler jobs create http "${SCHEDULER_JOB_NAME}" "${COMMON_ARGS[@]}"
fi

echo "Cloud Scheduler job ${SCHEDULER_JOB_NAME} now targets Cloud Run job ${JOB_NAME} on schedule ${SCHEDULE} (${TIME_ZONE})."
