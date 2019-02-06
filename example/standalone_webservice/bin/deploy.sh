#!/usr/bin/env bash

set -e

kubectl apply -f kubernetes_templates/app_deployment.yml -n crux
kubectl apply -f kubernetes_templates/app_service.yml -n crux
