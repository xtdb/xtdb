set -e

# latest commit
LATEST_COMMIT=$(git rev-parse HEAD)

WEBSITE_COMMIT=$(git log -1 --format=format:%H --full-diff website)


if [ $WEBSITE_COMMIT = $LATEST_COMMIT ];
    then
        echo "website being updated"
        .circleci/aerobatic_deploy.sh
