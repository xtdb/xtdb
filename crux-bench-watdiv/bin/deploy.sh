#!/usr/bin/env bash

set -e

if [ ! -z "$BUILD" ]; then
    echo "building transactor image"
    bin/build_and_upload_transactor_image.sh
    echo "building crux image"
    bin/build_and_upload_image.sh
fi

kubectl apply -f kubernetes_templates/app_deployment.yml -n crux
kubectl apply -f kubernetes_templates/app_service.yml -n crux

kubectl apply -f kubernetes_templates/transactor_storage.yml -n crux
kubectl apply -f kubernetes_templates/transactor_deployment.yml -n crux
kubectl apply -f kubernetes_templates/transactor_service.yml -n crux
