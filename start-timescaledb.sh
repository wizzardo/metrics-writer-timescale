#!/usr/bin/env bash

mkdir -p .data

DATA_PATH=$(realpath .data)

docker run --rm \
  --name metrics-timescaledb \
  -p 5435:5432 \
  -e POSTGRES_DB=metricsdb \
  -e POSTGRES_USER=metrics-rw \
  -e POSTGRES_PASSWORD=qwerty \
  -v $DATA_PATH:/var/lib/postgresql/data \
  timescale/timescaledb:latest-pg18