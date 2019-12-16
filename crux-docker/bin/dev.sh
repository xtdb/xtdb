#!/bin/bash
docker run --rm -ti -p 3000:3000 -v $HOME/.m2:/root/.m2 juxt/crux-http
