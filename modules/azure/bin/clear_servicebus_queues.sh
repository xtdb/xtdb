#!/bin/sh

results=$(az servicebus queue list -g "azure-modules-test" --namespace-name "xtdb-test-storage-account-eventbus" --query "[].{Name:name}" --output json)

for row in $(echo "$results" | jq -r '.[]|"\(.Name)"')
do
  az servicebus queue delete -g "azure-modules-test" --namespace-name "xtdb-test-storage-account-eventbus" --name "$row"
done
