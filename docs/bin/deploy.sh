#!/usr/bin/env bash

S3_BUCKET=xtdb-website-docs
DOMAIN=docs.xtdb.com
DEPLOY_PATH=build/site

set -x
set -e

(
    echo "Your AWS profile is currently set to: '$AWS_PROFILE'"

    cd $(dirname "$0")/../

    aws s3 sync --delete $DEPLOY_PATH s3://$S3_BUCKET \
    --exclude "*"       \
    --include "*.jpg"   \
    --include "*.jpeg"  \
    --include "*.svg"   \
    --include "*.gif"   \
    --include "*.png"   \
    --include "*.webp"  \
    --include "*.avif"  \
    --include "*.woff2" \
    --include "*.js"    \
    --include "*.css"   \
    --include "*.ico"   \
    --cache-control "max-age=172800,public"  # 2 days

    # use cp to avoid sync flake with timestamps/same-sized items (see https://github.com/aws/aws-cli/issues/3273)
    aws s3 cp --recursive $DEPLOY_PATH s3://$S3_BUCKET \
    --exclude "*.jpg"   \
    --exclude "*.jpeg"  \
    --exclude "*.svg"   \
    --exclude "*.gif"   \
    --exclude "*.png"   \
    --exclude "*.webp"  \
    --exclude "*.avif"  \
    --exclude "*.woff2" \
    --exclude "*.js"    \
    --exclude "*.css"   \
    --exclude "*.ico"

    DISTRIBUTION_ID=`aws cloudfront list-distributions --query "DistributionList.Items[*].{Id:Id,alias:Aliases.Items[0]}[?alias=='$DOMAIN'].Id" --output text`
    aws cloudfront create-invalidation --distribution-id $DISTRIBUTION_ID --paths '/*' --output text
)
