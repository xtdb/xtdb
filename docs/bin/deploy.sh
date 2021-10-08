#!/usr/bin/env bash
set -x
set -e

(
    echo "Your AWS profile is currently set to: '$AWS_PROFILE'"

    cd $(dirname "$0")/../

    # TODO: change `build/site` to `build/docs`
    aws s3 sync --delete build/site s3://xtdb-docs-2 --cache-control no-cache

    # DISTRIBUTION_ID="E165CLVKXU6TNR"
    # echo "If you re-run the CloudFormation setup, you'll need to specify a new Distribution id."
    # echo "CloudFront Distribution id is set manually to: $DISTRIBUTION_ID"
    DISTRIBUTION_ID=`aws cloudfront list-distributions --query "DistributionList.Items[*].{Id:Id,alias:Aliases.Items[0]}[?alias=='docs.xtdb.com'].Id" --output text`
    aws cloudfront create-invalidation --distribution-id $DISTRIBUTION_ID --paths '/*' > /tmp/xtdb-docs-last-cf-invalidation.json
    echo "CloudFront invalidation written to /tmp/xtdb-docs-last-cf-invalidation.json"
)
