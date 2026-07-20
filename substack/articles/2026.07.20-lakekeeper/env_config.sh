#!/usr/bin/env bash

load_project_env() {
    local root_dir="$1"
    local env_file="$root_dir/.env"

    if [[ -f "$env_file" ]]; then
        set -a
        # shellcheck disable=SC1090
        source "$env_file"
        set +a
    fi
}

resolve_bucket_for_mode() {
    local mode="$1"

    case "$mode" in
        dev)
            if [[ -z "${MINIO_BUCKET_NAME:-}" ]]; then
                echo "MINIO_BUCKET_NAME is not set (expected in .env or environment)." >&2
                return 1
            fi
            BUCKET="$MINIO_BUCKET_NAME"
            ;;
        prod)
            if [[ -z "${AWS_BUCKET_NAME:-}" ]]; then
                echo "AWS_BUCKET_NAME is not set (expected in .env or environment)." >&2
                return 1
            fi
            BUCKET="$AWS_BUCKET_NAME"
            ;;
        *)
            echo "Invalid mode: $mode" >&2
            return 1
            ;;
    esac

    export BUCKET
}
