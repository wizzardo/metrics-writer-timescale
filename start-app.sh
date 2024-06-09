#!/usr/bin/env bash
set -e

./build.sh

IMAGE_NAME="metrics-timescale"

if [ -n "$(docker images -q "$IMAGE_NAME")" ]; then
    docker rmi "$IMAGE_NAME"
    echo "Removed existing $IMAGE_NAME image"
fi

docker build -t "$IMAGE_NAME" -f Dockerfile .

docker run --rm -it \
  --name "$IMAGE_NAME" \
  -p 8080:8080 \
  -e DATASOURCE_URL="jdbc:postgresql://docker.for.mac.host.internal:5435/metricsdb" \
  "$IMAGE_NAME"
