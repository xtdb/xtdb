#!/usr/bin/env bash

if [[ $(lein project-version) =~ "-SNAPSHOT$" ]]; then
    lein deploy
fi
