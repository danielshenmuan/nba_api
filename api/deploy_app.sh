#!/usr/bin/env bash
# Build and deploy the FastAPI service to Cloud Run.
#
# Usage:
#   chmod +x deploy_app.sh
#   IMAGE_REPO=us-central1-docker.pkg.dev/fantasy-survivor-app/nba-api \
#     ./deploy_app.sh
#
# Required environment variables:
#   IMAGE_REPO  Artifact Registry repo (e.g. us-central1-docker.pkg.dev/PROJECT/repo)
# Optional environment variables:
#   PROJECT_ID       (default: fantasy-survivor-app)
#   REGION           (default: us-central1)
#   SERVICE_NAME     (default: nba-api)
#   IMAGE_TAG        (default: timestamp)
#   PLATFORM         (default: managed)
#   SERVICE_ACCOUNT  (if set, used during deployment)
#   ALLOW_UNAUTH     (default: 1, pass 0 to skip --allow-unauthenticated)

set -euo pipefail

PROJECT_ID=${PROJECT_ID:-fantasy-survivor-app}
REGION=${REGION:-us-central1}
SERVICE_NAME=${SERVICE_NAME:-nba-api}
IMAGE_TAG=${IMAGE_TAG:-$(date +%Y%m%d%H%M%S)}
PLATFORM=${PLATFORM:-managed}

if [[ -z "${IMAGE_REPO:-}" ]]; then
  echo "ERROR: IMAGE_REPO must be set (e.g., us-central1-docker.pkg.dev/${PROJECT_ID}/nba-api)" >&2
  exit 1
fi

IMAGE_URI="${IMAGE_REPO}/nba-api:${IMAGE_TAG}"

echo "Building container image ${IMAGE_URI}..."
gcloud builds submit api \
  --project "${PROJECT_ID}" \
  --tag "${IMAGE_URI}"

declare -a DEPLOY_ARGS=(
  "--project" "${PROJECT_ID}"
  "--region" "${REGION}"
  "--image" "${IMAGE_URI}"
  "--platform" "${PLATFORM}"
)

if [[ -n "${SERVICE_ACCOUNT:-}" ]]; then
  DEPLOY_ARGS+=("--service-account" "${SERVICE_ACCOUNT}")
fi

if [[ "${ALLOW_UNAUTH:-1}" == "1" ]]; then
  DEPLOY_ARGS+=("--allow-unauthenticated")
fi

printf '\nDeploying Cloud Run service %s...\n' "${SERVICE_NAME}"
gcloud run deploy "${SERVICE_NAME}" "${DEPLOY_ARGS[@]}"

printf '\nDeployment complete. Service %s now serves image %s.\n' "${SERVICE_NAME}" "${IMAGE_URI}"
