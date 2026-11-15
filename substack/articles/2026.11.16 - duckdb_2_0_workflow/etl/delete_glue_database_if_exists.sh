#!/bin/sh

database_name=${1:?Usage: delete_glue_database_if_exists.sh DATABASE_NAME}

if output=$(aws glue delete-database --name "$database_name" 2>&1); then
    [ -z "$output" ] || printf '%s\n' "$output"
else
    status=$?
    case "$output" in
        *"An error occurred (EntityNotFoundException) when calling the DeleteDatabase operation:"*)
            printf 'Glue database %s does not exist; skipping deletion.\n' "$database_name"
            ;;
        *)
            printf '%s\n' "$output" >&2
            exit "$status"
            ;;
    esac
fi
