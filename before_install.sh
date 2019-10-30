#!/usr/bin/env bash

# Build base hadoop image
docker build -t hadoop-base -f docker/hadoop/Dockerfile docker
# Build rest of images (done here instead of with `docker-compose up --build` to keep test output clean)
docker-compose build
