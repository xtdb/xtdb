#!/usr/bin/env bash

# Create Docker volume
docker volume create xtdb-am-data

set -e
(
  echo Running Docker image ...
  docker run -it --env-file local-vars.env -p 10001:10001 -p 8080:8080 --memory=4096m --rm -v xtdb-am-data:/var/lib/xtdb/ -v .:/external/dir xtdb-local-auctionmark:latest
)
