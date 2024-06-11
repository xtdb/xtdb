#!/usr/bin/env bash

set -e
(
  echo Running Docker image ...
  docker run -it --env-file local-vars.env -p 10001:10001 --rm xtdb-local-auctionmark:latest
)

