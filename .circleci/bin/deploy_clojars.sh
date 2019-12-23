#!/usr/bin/env bash
if [[ -z "$CIRCLE_PR_NUMBER" && $(lein project-version) =~ -SNAPSHOT$ ]]
then
    lein sub deploy
else
    echo "Either we're building a PR, or a tagged version - not deploying to Clojars."
fi
