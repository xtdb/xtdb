#!/usr/bin/env bash

(
    cd $(dirname $0)/..

    INCLUDES='--include ts-devices/small/* --include ts-weather/small/*'

    while [[ "$#" -gt 0 ]]; do
        case $1 in
            --watdiv)
                INCLUDES+=' --include watdiv/*'
                shift;;
            --devices-med)
                INCLUDES+=' --include ts-devices/med/* --include ts-devices/med/*'
                shift;;
            --devices-big)
                INCLUDES+=' --include ts-devices/big/* --include ts-devices/big/*'
                shift;;
            --weather-med)
                INCLUDES+=' --include ts-weather/med/* --include ts-weather/med/*'
                shift;;
            --weather-big)
                INCLUDES+=' --include ts-weather/big/* --include ts-weather/big/*'
                shift;;
            --auctionmark)
                INCLUDES+=' --include auctionmark/*'
                shift;;
            --gleif)
                # GLEIF (mirrored transit) loads from the dev dir, not test/resources;
                # synced separately below.
                GLEIF=1
                shift;;
            --help)
                echo "Flags: --watdiv, --devices-med, --devices-big, --weather-med, --weather-big, --gleif"
                exit 0;;
            *) echo "Unknown parameter passed: $1"; exit 1;;
        esac
    done

    set -xe
    aws s3 sync --exclude '*' $INCLUDES s3://xtdb-datasets ../../src/test/resources/data/

    if [[ -n "${GLEIF:-}" ]]; then
        aws s3 sync s3://xtdb-datasets/gleif/ ../../src/dev/resources/data/gleif/
    fi
)
