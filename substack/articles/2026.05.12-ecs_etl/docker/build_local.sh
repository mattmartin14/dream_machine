#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

IMAGE_TAG="${1:-etl:latest}"
NO_CACHE_FLAG="${2:-}"
DOCKER_PLATFORM="${DOCKER_PLATFORM:-linux/amd64}"

if [[ "$NO_CACHE_FLAG" == "--no-cache" ]]; then
  echo "Building Docker image (no cache): $IMAGE_TAG (platform=$DOCKER_PLATFORM)"
  docker build --platform "$DOCKER_PLATFORM" --no-cache -f "$SCRIPT_DIR/Dockerfile" -t "$IMAGE_TAG" "$PROJECT_ROOT"
else
  echo "Building Docker image: $IMAGE_TAG (platform=$DOCKER_PLATFORM)"
  docker build --platform "$DOCKER_PLATFORM" -f "$SCRIPT_DIR/Dockerfile" -t "$IMAGE_TAG" "$PROJECT_ROOT"
fi

echo "Build complete: $IMAGE_TAG"
