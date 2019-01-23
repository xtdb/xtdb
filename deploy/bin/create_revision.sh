#!/usr/bin/env bash

source "${BASH_SOURCE%/*}/shared.sh"

echo "creating revision from crux jar"

JAR_FILE="../target/crux-0.1.0-SNAPSHOT-standalone.jar"

REVISION_BUILD_DIR="$(mktemp -d)"
DESCRIPTION="Built by $(git config --get user.name) [SHA1 - $(git describe --match=NeVeRmAtCh --always --dirty)]"

cp -r codedeploy/* $REVISION_BUILD_DIR

echo $(ls $REVISION_BUILD_DIR)

cp $JAR_FILE $REVISION_BUILD_DIR/crux.jar

with-common-env && aws deploy push \
   --description "${DESCRIPTION}" \
   --application-name ${AWS_CODEDEPLOY_APPLICATION} \
   --s3-location "s3://${AWS_CODEDEPLOY_BUCKET}/crux_revision.zip" \
   --source "${REVISION_BUILD_DIR}"
