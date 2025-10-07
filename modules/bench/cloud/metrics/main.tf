terraform {
  required_version = ">= 1.5.0"

  backend "azurerm" {
    resource_group_name  = "xtdb-bench-terraform-state"
    storage_account_name = "xtdbbenchstate"
    container_name      = "benchmark-metrics-tfstate"
    key                 = "benchmark-metrics.terraform.tfstate"
  }

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 4.44.0"
    }
    azapi = {
      source  = "azure/azapi"
      version = ">= 1.13.0"
    }
  }
}

provider "azurerm" {
  features {}
  subscription_id = "91804669-c60b-4727-afa2-d7021fe5055b"
}

provider "azapi" {}

locals {
  stream_name = "Custom-${var.table_name}"
}

# Optional subscription context
data "azurerm_client_config" "current" {}

# Resource Group (optional)
resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location
}

# Log Analytics Workspace
resource "azurerm_log_analytics_workspace" "law" {
  name                = var.workspace_name
  location            = var.location
  resource_group_name = var.resource_group_name
  sku                 = "PerGB2018"
  retention_in_days   = 30
  depends_on = [
    azurerm_resource_group.rg
  ]
}

# Custom table for benchmarks (AzureRM v4)
resource "azurerm_log_analytics_workspace_table" "bench" {
  name                    = var.table_name
  workspace_id            = azurerm_log_analytics_workspace.law.id
  retention_in_days       = 30
  total_retention_in_days = 90
}

# Data Collection Endpoint (DCE)
resource "azurerm_monitor_data_collection_endpoint" "dce" {
  name                = var.dce_name
  location            = var.location
  resource_group_name = var.resource_group_name
}

# Data Collection Rule (DCR)
resource "azurerm_monitor_data_collection_rule" "dcr" {
  name                        = var.dcr_name
  location                    = var.location
  resource_group_name         = var.resource_group_name
  data_collection_endpoint_id = azurerm_monitor_data_collection_endpoint.dce.id

  destinations {
    log_analytics {
      name                  = "laDest"
      workspace_resource_id = azurerm_log_analytics_workspace.law.id
    }
  }

  data_flow {
    streams      = [local.stream_name]
    destinations = ["laDest"]
    output_stream = local.stream_name
    transform_kql = <<-KQL
      source | project TimeGenerated=todatetime(ts), run_id=tostring(run_id), git_sha=tostring(git_sha), repo=tostring(repo), benchmark=tostring(benchmark), step=tostring(step), node_id=tostring(node_id), metric=tostring(metric), value=todouble(value), unit=tostring(unit), ts=todatetime(ts), params=todynamic(params)
    KQL
  }

  # Associate the stream with the LA table
  stream_declaration {
    stream_name = local.stream_name
    column {
      name = "run_id"
      type = "string"
    }
    column {
      name = "git_sha"
      type = "string"
    }
    column {
      name = "repo"
      type = "string"
    }
    column {
      name = "benchmark"
      type = "string"
    }
    column {
      name = "step"
      type = "string"
    }
    column {
      name = "node_id"
      type = "string"
    }
    column {
      name = "metric"
      type = "string"
    }
    column {
      name = "value"
      type = "real"
    }
    column {
      name = "unit"
      type = "string"
    }
    column {
      name = "ts"
      type = "datetime"
    }
    column {
      name = "params"
      type = "dynamic"
    }
  }
}

resource "azurerm_role_assignment" "dcr_sender" {
  scope                = azurerm_monitor_data_collection_rule.dcr.id
  role_definition_name = "Monitoring Metrics Publisher"
  principal_id         = var.sender_principal_id
}

# Logic App: Relay Azure Monitor alerts to Slack (Incoming Webhook)
resource "azurerm_logic_app_workflow" "bench_alert_relay" {
  name                = "xtdb-bench-alert-relay"
  location            = var.location
  resource_group_name = var.resource_group_name
  enabled             = true
}

resource "azurerm_logic_app_trigger_http_request" "bench_alert_trigger" {
  name         = "http_trigger"
  logic_app_id = azurerm_logic_app_workflow.bench_alert_relay.id
  schema       = <<SCHEMA
{
  "type": "object"
}
SCHEMA
}

resource "azurerm_logic_app_action_http" "bench_alert_post_to_slack" {
  name         = "PostToSlack"
  logic_app_id = azurerm_logic_app_workflow.bench_alert_relay.id
  method       = "POST"
  uri          = var.slack_webhook_url

  headers = {
    "Content-Type" = "application/json"
  }

  body = jsonencode({
    attachments = [
      {
        color       = "#D32F2F"
        title       = "@{coalesce(triggerBody()?['data']?['essentials']?['alertRule'], 'Alert')}"
        title_link  = "@{concat('https://portal.azure.com/#blade/Microsoft_Azure_Monitoring/AlertDetailsTemplateBlade/alertId/', uriComponent(triggerBody()?['data']?['essentials']?['alertId']))}"
        text        = "@{concat('*Severity:* ', coalesce(string(triggerBody()?['data']?['essentials']?['severity']), 'N/A'), '\n', '*Description:* ', coalesce(triggerBody()?['data']?['essentials']?['description'], ''), '\n', '*Target:* ', coalesce(triggerBody()?['data']?['essentials']?['alertTargetIDs'][0], ''))}"
        mrkdwn_in   = ["text"]
      }
    ]
  })
}

resource "azapi_resource_action" "bench_alert_relay_cb" {
  type        = "Microsoft.Logic/workflows/triggers@2019-05-01"
  resource_id = "${azurerm_logic_app_workflow.bench_alert_relay.id}/triggers/${azurerm_logic_app_trigger_http_request.bench_alert_trigger.name}"
  action      = "listCallbackUrl"
  method      = "POST"
  response_export_values = ["value"]
}

# Action Group for alerts (Slack via webhook)
resource "azurerm_monitor_action_group" "bench_slack" {
  name                = var.action_group_name
  resource_group_name = var.resource_group_name
  short_name          = var.action_group_short_name

  logic_app_receiver {
    name                    = "slack-relay"
    resource_id             = azurerm_logic_app_workflow.bench_alert_relay.id
    callback_url            = azapi_resource_action.bench_alert_relay_cb.output.value
    use_common_alert_schema = true
  }
}

# Logic App: Scheduled anomaly detection (2σ over P30D via Logs Query API)
resource "azapi_resource" "bench_anomaly" {
  type      = "Microsoft.Logic/workflows@2019-05-01"
  name      = var.anomaly_logic_app_name
  location  = var.location
  parent_id = azurerm_resource_group.rg.id

  identity {
    type = "SystemAssigned"
  }

  body = {
    properties = {
      state      = var.anomaly_alert_enabled ? "Enabled" : "Disabled"
      definition = {
        "$schema"        = "https://schema.management.azure.com/providers/Microsoft.Logic/schemas/2016-06-01/workflowdefinition.json#"
        "contentVersion" = "1.0.0.0"
        "parameters"     = {}
        "triggers" = {
          "recurrence" = {
            "type"       = "Recurrence"
            "recurrence" = {
              "frequency" = var.anomaly_schedule_frequency
              "interval"  = var.anomaly_schedule_interval
            }
          }
        }
        "actions" = {
          "QueryLogs" = {
            "type"   = "Http"
            "inputs" = {
              "method" = "POST"
              "uri"    = "https://api.loganalytics.io/v1/workspaces/${azurerm_log_analytics_workspace.law.workspace_id}/query"
              "authentication" = {
                "type"     = "ManagedServiceIdentity"
                "audience" = "https://api.loganalytics.io/"
              }
              "headers" = {
                "Content-Type" = "application/json"
              }
              "body" = {
                "timespan" = var.anomaly_timespan
                "query"    = <<-KQL
                  let N = toint(${var.anomaly_baseline_n});
                  let latestBenchmark =
                      ${var.table_name}
                      | where step == "overall" and metric == "duration_ms"
                      | where benchmark == "tpch" and toreal(params.scaleFactor) == ${var.anomaly_scale_factor}
                      | where repo == "${var.anomaly_repo}"
                      | summarize arg_max(TimeGenerated, *)
                      | extend current_ms = todouble(value), k = 1;
                  let prevN =
                      ${var.table_name}
                      | where step == "overall" and metric == "duration_ms"
                      | where benchmark == "tpch" and toreal(params.scaleFactor) == ${var.anomaly_scale_factor}
                      | where repo == "${var.anomaly_repo}"
                      | order by TimeGenerated desc
                      | top N+1 by TimeGenerated desc
                      | top N by TimeGenerated asc
                      | project value = todouble(value), TimeGenerated;
                  let baseline = prevN | summarize baseline_mean = avg(value), baseline_std = stdev(value) | extend k = 1;
                  latestBenchmark
                    | join kind=inner baseline on k
                    | where isnotnull(baseline_std) and baseline_std > 0
                    | extend threshold_value_slow = baseline_mean + (${var.anomaly_sigma} * baseline_std),
                             threshold_value_fast = baseline_mean - (${var.anomaly_sigma} * baseline_std)
                    | extend slow_violation = current_ms > threshold_value_slow,
                             fast_violation = current_ms < threshold_value_fast
                    | where slow_violation or fast_violation
                    | project run_id = tostring(run_id), slow_violation, fast_violation, TimeGenerated, current_ms, baseline_mean, baseline_std
                KQL
              }
            }
          }
          "IfAnomaly" = {
            "type"       = "If"
            "expression" = {
              "and" = [
                {
                  "greater" = [
                    {
                      "length" = [
                        "@coalesce(body('QueryLogs')?['tables'][0]?['rows'], createArray())"
                      ]
                    },
                    0
                  ]
                }
              ]
            }
            "runAfter" = {
              "QueryLogs" = [
                "Succeeded"
              ]
            }
            "actions" = {
              "PostAnomalyToSlack" = {
                "type"   = "Http"
                "inputs" = {
                  "method" = "POST"
                  "uri"    = var.slack_webhook_url
                  "headers" = {
                    "Content-Type" = "application/json"
                  }
                  "body" = {
                    "attachments" = [
                      {
                        "color" = "#D32F2F"
                        "title" = "@{concat('Benchmark anomaly: TPCH ', if(equals(first(body('QueryLogs')?['tables'][0]?['rows'])[1], true), 'ran slower', 'ran faster'))}"
                        "text"  = "@{concat('*Scale factor:* ', string(${var.anomaly_scale_factor}), '\n', '*Condition:* ± ', string(${var.anomaly_sigma}), 'σ over last ', '${var.anomaly_baseline_n} runs', '\n', '*Run:* https://github.com/${var.anomaly_repo}/actions/runs/', string(first(body('QueryLogs')?['tables'][0]?['rows'])[0]))}"
                        "mrkdwn_in" = ["text"]
                      }
                    ]
                  }
                }
              }
            }
            "else" = {}
          }
        }
        "outputs" = {}
      }
    }
  }
}


resource "azurerm_role_assignment" "bench_anomaly_la_reader" {
  scope                = azurerm_log_analytics_workspace.law.id
  role_definition_name = "Log Analytics Reader"
  principal_id         = azapi_resource.bench_anomaly.output.identity.principalId
}

# Scheduled query alert: missing ingestion (no TPC-H runs at the given scale factor within the window)
resource "azurerm_monitor_scheduled_query_rules_alert_v2" "bench_missing_ingestion" {
  name                = var.missing_alert_name
  resource_group_name = var.resource_group_name
  location            = var.location
  severity            = var.missing_alert_severity
  enabled             = var.missing_alert_enabled

  evaluation_frequency = var.missing_alert_evaluation_frequency
  window_duration      = var.missing_alert_window_duration

  scopes = [azurerm_log_analytics_workspace.law.id]

  criteria {
    query = <<-KQL
      ${var.table_name}
      | where step == "overall" and metric == "duration_ms"
      | where benchmark == "tpch" and toreal(params.scaleFactor) == ${var.anomaly_scale_factor}
      | where repo == "${var.anomaly_repo}"
      | summarize c = count()
      | where c == 0
      | project trigger = 1
    KQL
    time_aggregation_method = "Count"
    operator                = "GreaterThan"
    threshold               = 0
    failing_periods {
      number_of_evaluation_periods            = 1
      minimum_failing_periods_to_trigger_alert = 1
    }
  }

  action {
    action_groups = [azurerm_monitor_action_group.bench_slack.id]
  }

  description = "Alert when no TPC-H scaleFactor=${var.anomaly_scale_factor} 'overall' rows are ingested within ${var.missing_alert_window_duration}"
}

# Azure Portal Dashboard: 10 most recent runs (line chart)
resource "azurerm_portal_dashboard" "bench_dashboard" {
  name                = var.dashboard_name
  resource_group_name = var.resource_group_name
  location            = var.location

  dashboard_properties = jsonencode({
    lenses = {
      "0" = {
        order = 0
        parts = {
          "runs-timeseries" = {
            position = {
              x       = 0
              y       = 0
              colSpan = 6
              rowSpan = 4
            }
            metadata = {
              inputs = [
                {
                  name       = "resourceTypeMode"
                  isOptional = true
                },
                {
                  name       = "ComponentId"
                  isOptional = true
                },
                {
                  name       = "Scope"
                  value = {
                    resourceIds = [
                      "/subscriptions/91804669-c60b-4727-afa2-d7021fe5055b/resourcegroups/xtdb-benchmark-metrics/providers/microsoft.operationalinsights/workspaces/xtdb-benchmark-metrics"
                    ]
                  }
                  isOptional = true
                },
                {
                  name       = "PartId"
                  value      = "158d931e-8294-47ce-8d13-c04026627f42"
                  isOptional = true
                },
                {
                  name       = "Version"
                  value      = "2.0"
                  isOptional = true
                },
                {
                  name       = "TimeRange"
                  value      = "P1D"
                  isOptional = true
                },
                {
                  name       = "DashboardId"
                  isOptional = true
                },
                {
                  name       = "DraftRequestParameters"
                  isOptional = true
                },
                {
                  name       = "Query"
                  value      = <<-KQL
                    XTDBBenchmark_CL
                    | where step == "overall" and metric == "duration_ms"
                    | where benchmark == "tpch" and toreal(params.scaleFactor) == ${var.anomaly_scale_factor}
                    | where repo == "${var.anomaly_repo}"
                    | top ${var.anomaly_baseline_n} by TimeGenerated desc
                    | order by TimeGenerated asc
                    | project TimeGenerated, duration_ms = todouble(value)
                  KQL
                  isOptional = true
                },
                {
                  name       = "ControlType"
                  value      = "FrameControlChart"
                  isOptional = true
                },
                {
                  name       = "SpecificChart"
                  value      = "Line"
                  isOptional = true
                },
                {
                  name       = "PartTitle"
                  value      = "Benchmark TPC-H Runs"
                  isOptional = true
                },
                {
                  name       = "PartSubTitle"
                  value      = "xtdb-benchmark-metrics"
                  isOptional = true
                },
                {
                  name = "Dimensions"
                  value = {
                    xAxis = {
                      name = "TimeGenerated"
                      type = "datetime"
                    }
                    yAxis = [
                      {
                        name = "duration_ms"
                        type = "real"
                      }
                    ]
                    splitBy     = []
                    aggregation = "Sum"
                  }
                  isOptional = true
                },
                {
                  name = "LegendOptions"
                  value = {
                    isEnabled = true
                    position  = "Bottom"
                  }
                  isOptional = true
                },
                {
                  name       = "IsQueryContainTimeRange"
                  value      = false
                  isOptional = true
                }
              ]
              type     = "Extension/Microsoft_OperationsManagementSuite_Workspace/PartType/LogsDashboardPart"
              settings = {}
            }
          }
        }
      }
    }
    metadata = {
      model = {
        timeRange = {
          value = {
            relative = {
              duration = 24
              timeUnit = 1
            }
          }
          type = "MsPortalFxTimeRange"
        }
        filterLocale = {
          value = "en-us"
        }
        filters = {
          value = {
            MsPortalFx_TimeRange = {
              model = {
                format      = "utc"
                granularity = "auto"
                relative    = "24h"
              }
              displayCache = {
                name  = "UTC Time"
                value = "Past 24 hours"
              }
              filteredPartIds = [
                "StartboardPart-LogsDashboardPart-22bc70d2-f7ad-42ba-a586-df4ab8272012"
              ]
            }
          }
        }
      }
    }
  })
}
