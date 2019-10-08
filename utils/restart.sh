#!/usr/bin/env bash
# Utility script to restart a running `docker-compose` cluster

docker-compose down -v --remove-orphans -t 0
docker-compose up --build -d
