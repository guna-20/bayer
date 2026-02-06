sudo mkdir -p /opt/app
sudo tee /opt/app/deploy.sh >/dev/null <<'SH'
#!/usr/bin/env bash
set -euo pipefail

AWS_REGION="${AWS_REGION:-us-east-1}"
ECR_URI="${ECR_URI:?missing ECR_URI (e.g. 123.dkr.ecr.us-east-1.amazonaws.com/myapp)}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
CONTAINER_NAME="${CONTAINER_NAME:-myapp}"
PORT="${PORT:-8000}"

aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$ECR_URI"

docker pull "$ECR_URI:$IMAGE_TAG"

if docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
fi

docker run -d \
  --name "$CONTAINER_NAME" \
  --restart unless-stopped \
  -p "$PORT:$PORT" \
  -e PORT="$PORT" \
  "$ECR_URI:$IMAGE_TAG"

docker image prune -f >/dev/null 2>&1 || true
SH

sudo chmod +x /opt/app/deploy.sh
