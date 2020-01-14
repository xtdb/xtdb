#!/usr/bin/env bash

set -e

WATDIV_ARCIVE="watdiv.10M.tar.bz2"
STRESS_ARCIVE="stress-workloads.tar.gz"
TARGET_DIR="resources/watdiv/data"

mkdir -p ${TARGET_DIR}

if [ ! -f "${TARGET_DIR}/watdiv.10M.nt" ]; then
	wget https://dsg.uwaterloo.ca/watdiv/${WATDIV_ARCIVE}
	echo "extracting: ${WATDIV_ARCIVE}"
	tar -xvjf ${WATDIV_ARCIVE} -C ${TARGET_DIR}
        rm ${WATDIV_ARCIVE}
fi

if [ ! -f "${TARGET_DIR}/watdiv-stress-100" ]; then
	wget https://dsg.uwaterloo.ca/watdiv/${STRESS_ARCIVE}
	echo "extracting: ${STRESS_ARCIVE}"
	tar -xvzf ${STRESS_ARCIVE} -C ${TARGET_DIR}
        rm ${STRESS_ARCIVE}
fi
