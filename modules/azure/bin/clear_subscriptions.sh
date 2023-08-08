#!/bin/sh

results=$(az eventgrid system-topic event-subscription list -g "azure-modules-test" --system-topic-name "xtdb-test-storage-account-system-topic" --query "[].{Name:name}" --output json)

for row in $(echo "$results" | jq -r '.[]|"\(.Name)"')
do
  az eventgrid system-topic event-subscription delete -g "azure-modules-test" --system-topic-name "xtdb-test-storage-account-system-topic" --name "$row" -y &
done
