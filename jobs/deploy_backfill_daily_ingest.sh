#!/usr/bin/env bash
# Deploy the backfill_daily_ingest Cloud Run job image and update the
# existing nba-daily-ingest job to execute the backfill script.
#
# Usage:
#   chmod +x deploy_backfill_daily_ingest.sh
#   ./deploy_backfill_daily_ingest.sh
#
# Optional environment variables:
#   IMAGE_REPO        (default: us-central1-docker.pkg.dev/$PROJECT_ID/nba-jobs)
#   PROJECT_ID        (default: fantasy-survivor-app)
#   REGION            (default: us-central1)
#   JOB_NAME          (default: nba-daily-ingest)
#   IMAGE_NAME        (default: backfill-daily-ingest)
#   IMAGE_TAG         (default: timestamp)
#   SERVICE_ACCOUNT   (if set, used when updating the job)
#   EXECUTE_AFTER_DEPLOY (set to 1 to run the job immediately after updating)
#   BACKFILL_ARGS     (optional space-delimited arguments appended after
#                      backfill_daily_ingest.py when updating the job)

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
BUILD_CONTEXT="${SCRIPT_DIR}"

PROJECT_ID=${PROJECT_ID:-fantasy-survivor-app}
REGION=${REGION:-us-central1}
JOB_NAME=${JOB_NAME:-nba-daily-ingest}
IMAGE_NAME=${IMAGE_NAME:-backfill-daily-ingest}
IMAGE_TAG=${IMAGE_TAG:-$(date +%Y%m%d%H%M%S)}

if [[ -z "${IMAGE_REPO:-}" ]]; then
  IMAGE_REPO="us-central1-docker.pkg.dev/${PROJECT_ID}/nba-jobs"
fi

ensure_repo_exists() {
  local image_repo="$1"

  local host_part="${image_repo%%/*}"
  local remainder="${image_repo#${host_part}/}"
  local repo_project="${remainder%%/*}"
  local repo_name="${remainder#${repo_project}/}"

  if [[ -z "${repo_name}" || "${repo_name}" == "${remainder}" ]]; then
    echo "Unable to determine repository name from IMAGE_REPO=${image_repo}" >&2
    exit 1
  fi

  local location="${host_part%%-docker.pkg.dev}"

  if [[ -z "${repo_project}" || -z "${location}" ]]; then
    echo "Unable to parse IMAGE_REPO=${image_repo}." >&2
    exit 1
  fi

  if ! gcloud artifacts repositories describe "${repo_name}" \
    --project "${repo_project}" \
    --location "${location}" >/dev/null 2>&1; then
    echo "Creating Artifact Registry repository ${repo_name} in ${location}..."
    gcloud artifacts repositories create "${repo_name}" \
      --project "${repo_project}" \
      --location "${location}" \
      --repository-format=docker \
      --description "Created by deploy_backfill_daily_ingest.sh"
  fi
}

ensure_repo_exists "${IMAGE_REPO}"

IMAGE_URI="${IMAGE_REPO}/${IMAGE_NAME}:${IMAGE_TAG}"

if ! gcloud run jobs describe "${JOB_NAME}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" >/dev/null 2>&1; then
  cat <<EOF >&2
Cloud Run job "${JOB_NAME}" was not found in project ${PROJECT_ID} (${REGION}).
Create the job manually first, then rerun this script to update its image.
EOF
  exit 1
fi

echo "Building container image ${IMAGE_URI}..."
gcloud builds submit "${BUILD_CONTEXT}" \
  --project "${PROJECT_ID}" \
  --tag "${IMAGE_URI}"

declare -a UPDATE_ARGS=(
  "--image" "${IMAGE_URI}"
  "--project" "${PROJECT_ID}"
  "--region" "${REGION}"
  "--command" "python"
  "--args" "backfill_daily_ingest.py"
)

if [[ -n "${SERVICE_ACCOUNT:-}" ]]; then
  UPDATE_ARGS+=("--service-account" "${SERVICE_ACCOUNT}")
fi

if [[ -n "${BACKFILL_ARGS:-}" ]]; then
  # shellcheck disable=SC2206
  EXTRA_ARGS=(${BACKFILL_ARGS})
  for arg in "${EXTRA_ARGS[@]}"; do
    UPDATE_ARGS+=("--args" "${arg}")
  done
fi

printf '\nUpdating Cloud Run job %s...\n' "${JOB_NAME}"
gcloud run jobs update "${JOB_NAME}" "${UPDATE_ARGS[@]}"

if [[ "${EXECUTE_AFTER_DEPLOY:-0}" == "1" ]]; then
  printf '\nExecuting Cloud Run job %s...\n' "${JOB_NAME}"
  gcloud run jobs execute "${JOB_NAME}" \
    --project "${PROJECT_ID}" \
    --region "${REGION}"
fi

printf '\nDeployment complete. Job %s now uses image %s and runs backfill_daily_ingest.py.\n' \
  "${JOB_NAME}" "${IMAGE_URI}"
