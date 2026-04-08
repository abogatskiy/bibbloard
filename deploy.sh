#!/bin/bash
# Deploy Billboard H-Index to Pi via GitHub Container Registry + SSH

set -e

GITHUB_USERNAME="abogatskiy"
PI_HOST="100.94.40.119"    # Tailscale IP
CONTAINER="billboard-web"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load secrets from .env
if [ ! -f "$SCRIPT_DIR/.env" ]; then
    echo "Error: $SCRIPT_DIR/.env not found"
    echo "Create it with: GITHUB_TOKEN=..."
    exit 1
fi
set -a; source "$SCRIPT_DIR/.env"; set +a

# Ensure Docker is running (macOS)
if ! docker info &>/dev/null; then
    echo "Docker not running — launching Docker Desktop..."
    open -a Docker
    echo -n "Waiting for Docker"
    until docker info &>/dev/null; do echo -n "."; sleep 2; done
    echo " ready."
fi

# Build and push
echo "Logging into ghcr.io..."
echo "$GITHUB_TOKEN" | docker login ghcr.io -u "$GITHUB_USERNAME" --password-stdin

echo "Building $CONTAINER..."
docker buildx build --platform linux/arm64 --no-cache \
    -t "ghcr.io/$GITHUB_USERNAME/$CONTAINER:latest" \
    --push "$SCRIPT_DIR"
echo "Image pushed!"

# Deploy via SSH: pull new image, stop old container, start fresh
echo "Deploying via SSH..."
ssh "$PI_HOST" "
    docker pull ghcr.io/$GITHUB_USERNAME/$CONTAINER:latest
    docker stop $CONTAINER 2>/dev/null || true
    docker rm   $CONTAINER 2>/dev/null || true
    docker run -d --name $CONTAINER \
        --network nginx_proxy-network \
        --restart unless-stopped \
        ghcr.io/$GITHUB_USERNAME/$CONTAINER:latest
" && echo "Done! Hard-refresh (Cmd+Shift+R)" || \
    echo "SSH deploy failed — check Pi connectivity (start Tailscale if needed)."
