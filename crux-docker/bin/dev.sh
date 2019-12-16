#!/bin/bash
docker run --rm -ti -p 3000:3000 \
       -v $HOME/.m2:/root/.m2 \
       -v $PWD/crux.edn:/etc/crux.edn \
       -v $PWD/deps.edn:/usr/lib/crux/deps.edn \
       juxt/crux-http
