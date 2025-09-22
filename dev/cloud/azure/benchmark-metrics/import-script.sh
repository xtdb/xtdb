#!/usr/bin/env bash
set -euo pipefail

# --- Config: adjust if different ---
SUBSCRIPTION_ID="91804669-c60b-4727-afa2-d7021fe5055b"
RG="xtdb-benchmark-metrics"
LAW="xtdb-benchmark-metrics"
DCE="xtdb-benchmark-metrics"
DCR="xtdb-benchmark-metrics"
TABLE="XTDBBenchmark_CL"
# Service Principal (or User) OBJECT ID that already has/should have the DCR sender role:
SENDER_OBJECT_ID="0995c238-9646-4014-be4d-487ff8300975"
# ----------------------------------

# Sanity checks
command -v az >/dev/null || { echo "Azure CLI (az) not found"; exit 1; }
command -v terraform >/dev/null || { echo "terraform not found"; exit 1; }

echo "Logging into Azure (make sure you're on the right tenant/subscription)..."
az account show >/dev/null 2>&1 || az login >/dev/null
az account set -s "$SUBSCRIPTION_ID"

echo "Initializing Terraform backend (ensure backend storage exists per main.tf)..."
terraform init -input=false

echo "Resolving resource IDs..."
SUB=$(az account show --query id -o tsv)
RG_ID="/subscriptions/${SUB}/resourceGroups/${RG}"
LAW_ID=$(az monitor log-analytics workspace show -g "$RG" -n "$LAW" --query id -o tsv)
DCE_ID=$(az monitor data-collection endpoint show -g "$RG" -n "$DCE" --query id -o tsv)
DCR_ID=$(az monitor data-collection rule show -g "$RG" -n "$DCR" --query id -o tsv)
TABLE_ID="${LAW_ID}/tables/${TABLE}"

echo "Found:"
echo "  RG_ID   = $RG_ID"
echo "  LAW_ID  = $LAW_ID"
echo "  DCE_ID  = $DCE_ID"
echo "  DCR_ID  = $DCR_ID"
echo "  TABLE_ID= $TABLE_ID"

echo "Importing Resource Group..."
echo terraform import -input=false azurerm_resource_group.rg "$RG_ID" || true
terraform import -input=false azurerm_resource_group.rg "$RG_ID" || true

echo "Importing Log Analytics Workspace..."
echo terraform import -input=false azurerm_log_analytics_workspace.law "$LAW_ID" || true
terraform import -input=false azurerm_log_analytics_workspace.law "$LAW_ID" || true

echo "Importing Custom Table..."
echo terraform import -input=false azurerm_log_analytics_workspace_table.bench "$TABLE_ID" || true
terraform import -input=false azurerm_log_analytics_workspace_table.bench "$TABLE_ID" || true

echo "Importing Data Collection Endpoint (DCE)..."
echo terraform import -input=false azurerm_monitor_data_collection_endpoint.dce "$DCE_ID" || true
terraform import -input=false azurerm_monitor_data_collection_endpoint.dce "$DCE_ID" || true

echo "Importing Data Collection Rule (DCR)..."
echo terraform import -input=false azurerm_monitor_data_collection_rule.dcr "$DCR_ID" || true
terraform import -input=false azurerm_monitor_data_collection_rule.dcr "$DCR_ID" || true

echo "Checking for existing role assignment on the DCR for principal $SENDER_OBJECT_ID..."
EXISTING_RA_ID=$(az role assignment list \
  --assignee "$SENDER_OBJECT_ID" \
  --scope "$DCR_ID" \
  --query "[0].id" -o tsv || echo "")

if [ -n "${EXISTING_RA_ID}" ] && [ "${EXISTING_RA_ID}" != "null" ]; then
  echo "Importing existing role assignment: $EXISTING_RA_ID"
  echo terraform import -input=false azurerm_role_assignment.dcr_sender "$EXISTING_RA_ID" || true
  terraform import -input=false azurerm_role_assignment.dcr_sender "$EXISTING_RA_ID" || true
else
  echo "No existing role assignment found for principal on DCR."
  echo "If you expect Terraform to manage it, either:"
  echo "  - let Terraform create it on apply (resource already in main.tf), or"
  echo "  - create it now, then import:"
  echo "    az role assignment create --assignee-object-id \"$SENDER_OBJECT_ID\" --assignee-principal-type ServicePrincipal --role \"Monitoring Data Collection Rule Data Sender\" --scope \"$DCR_ID\""
  echo "    # then:"
  echo "    EXISTING_RA_ID=\$(az role assignment list --assignee \"$SENDER_OBJECT_ID\" --scope \"$DCR_ID\" --query \"[0].id\" -o tsv)"
  echo "    terraform import azurerm_role_assignment.dcr_sender \"\$EXISTING_RA_ID\""
fi

echo
echo "Done. Next: review the plan for drift."
terraform plan
