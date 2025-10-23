#!/usr/bin/env bash
# Build and deploy the FastAPI service to Cloud Run.
#
# Usage:
#   chmod +x deploy_app.sh
#   ./deploy_app.sh
#
# Optional environment variables:
#   IMAGE_REPO       (default: us-central1-docker.pkg.dev/$PROJECT_ID/nba-api)
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
  IMAGE_REPO="us-central1-docker.pkg.dev/${PROJECT_ID}/nba-api"
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
      --description "Created by deploy_app.sh"
  fi
}

ensure_repo_exists "${IMAGE_REPO}"

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
