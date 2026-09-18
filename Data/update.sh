#!/bin/bash

IMAGE="khanh/datalogger-linux:latest"

echo "Checking for update..."

OLD_ID=$(docker image inspect "$IMAGE" --format '{{.Id}}' 2>/dev/null)

docker pull "$IMAGE"

NEW_ID=$(docker image inspect "$IMAGE" --format '{{.Id}}')

if [ "$OLD_ID" = "$NEW_ID" ]; then
    echo "No update available."
else
    echo "New version found."
    echo "Updating Datalogger..."

    docker compose up -d

    echo "Update completed."
fi