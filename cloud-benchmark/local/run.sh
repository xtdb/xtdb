#!/usr/bin/env bash

# Create Docker volume
docker volume create xtdb-am-data

set -e
(
  echo Running Docker image ...
  docker run -it --env-file local-vars.env -p 10001:10001 --memory=4096m --rm -v xtdb-am-data:/var/lib/xtdb/ xtdb-local-auctionmark:latest
)

