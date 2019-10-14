#!/usr/bin/env bash

# Build base hadoop image
docker build -t hadoop-base hadoop
# Build rest of images (done here instead of with `docker-compose up --build` to keep test output clean)
docker-compose build
