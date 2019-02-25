#!/bin/bash -ex

source ./variables.sh

cd ../example/standalone_webservice/

for build in ${BUILDS[@]}; do
    tag=${CONTAINER}/${build}:${STUB_DEP_VERSION}_${BUILD_VERSION}
    echo "building ${build} container with tag ${tag}"
	docker build -t ${tag} \
        -f ./Dockerfile \
        --build-arg stub_dep_version=${STUB_DEP_VERSION} \
        --target ${build} \
        .

    if [ "${LATEST}" = true ]; then
        echo "setting ${tag} to latest"
        docker tag ${tag} ${CONTAINER}/${build}:latest
    fi
done
