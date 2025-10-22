#!/usr/bin/env bash
# Deploy the daily_ingest Cloud Run job image and update the job to use it.
#
# Usage:
#   chmod +x deploy_daily_ingest.sh
#   IMAGE_REPO=us-central1-docker.pkg.dev/fantasy-survivor-app/nba-jobs \
#     ./deploy_daily_ingest.sh
#
# Required environment variables:
#   IMAGE_REPO  Artifact Registry repo (e.g., us-central1-docker.pkg.dev/PROJECT/repo)
# Optional environment variables:
#   PROJECT_ID       (default: fantasy-survivor-app)
#   REGION           (default: us-central1)
#   JOB_NAME         (default: nba-daily-ingest)
#   IMAGE_TAG        (default: timestamp)
#   SERVICE_ACCOUNT  (if set, used when updating the job)
#   EXECUTE_AFTER_DEPLOY (set to 1 to run the job immediately after updating)

set -euo pipefail

PROJECT_ID=${PROJECT_ID:-fantasy-survivor-app}
REGION=${REGION:-us-central1}
JOB_NAME=${JOB_NAME:-nba-daily-ingest}
IMAGE_TAG=${IMAGE_TAG:-$(date +%Y%m%d%H%M%S)}

if [[ -z "${IMAGE_REPO:-}" ]]; then
  echo "ERROR: IMAGE_REPO must be set (e.g., us-central1-docker.pkg.dev/${PROJECT_ID}/nba-jobs)" >&2
  exit 1
fi

IMAGE_URI="${IMAGE_REPO}/nba-daily-ingest:${IMAGE_TAG}"

echo "Building container image ${IMAGE_URI}..."
gcloud builds submit jobs \
  --project "${PROJECT_ID}" \
  --tag "${IMAGE_URI}"

declare -a UPDATE_ARGS=(
  "--image" "${IMAGE_URI}"
  "--project" "${PROJECT_ID}"
  "--region" "${REGION}"
)

if [[ -n "${SERVICE_ACCOUNT:-}" ]]; then
  UPDATE_ARGS+=("--service-account" "${SERVICE_ACCOUNT}")
fi

printf '\nUpdating Cloud Run job %s...\n' "${JOB_NAME}"
gcloud run jobs update "${JOB_NAME}" "${UPDATE_ARGS[@]}"

if [[ "${EXECUTE_AFTER_DEPLOY:-0}" == "1" ]]; then
  printf '\nExecuting Cloud Run job %s...\n' "${JOB_NAME}"
  gcloud run jobs execute "${JOB_NAME}" \
    --project "${PROJECT_ID}" \
    --region "${REGION}"
fi

printf '\nDeployment complete. Job %s now uses image %s.\n' "${JOB_NAME}" "${IMAGE_URI}"
