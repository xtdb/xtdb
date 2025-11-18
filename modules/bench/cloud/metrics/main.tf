terraform {
  required_version = ">= 1.5.0"

  backend "azurerm" {
    resource_group_name  = "xtdb-bench-terraform-state"
    storage_account_name = "xtdbbenchstate"
    container_name       = "benchmark-metrics-tfstate"
    key                  = "benchmark-metrics.terraform.tfstate"
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

# Optional subscription context
data "azurerm_client_config" "current" {}

# Use the existing cluster LAW from cloud-benchmark-resources
data "azurerm_log_analytics_workspace" "cluster_law" {
  name                = var.cluster_law_name
  resource_group_name = var.resource_group_name
}

# Get existing resource group for creating new resources
data "azurerm_resource_group" "rg" {
  name = var.resource_group_name
}

# Common KQL function to parse benchmark JSON logs from ContainerLog
locals {
  # Base query to extract benchmark metrics from container logs
  benchmark_logs_base = <<-KQL
    ContainerLog
    | where LogEntry has "benchmark" and LogEntry has "step" and LogEntry has "metric"
    | extend log = parse_json(LogEntry)
    | where isnotnull(log.benchmark) and isnotnull(log.step) and isnotnull(log.metric)
  KQL
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
        color      = "#D32F2F"
        title      = "@{coalesce(triggerBody()?['data']?['essentials']?['alertRule'], 'Alert')}"
        title_link = "@{concat('https://portal.azure.com/#blade/Microsoft_Azure_Monitoring/AlertDetailsTemplateBlade/alertId/', uriComponent(triggerBody()?['data']?['essentials']?['alertId']))}"
        text       = "${var.slack_subteam_id != "" ? "<!subteam^${var.slack_subteam_id}|${var.slack_subteam_label}> " : ""}@{concat('*Severity:* ', coalesce(string(triggerBody()?['data']?['essentials']?['severity']), 'N/A'), '\n', '*Description:* ', coalesce(triggerBody()?['data']?['essentials']?['description'], ''), '\n', '*Target:* ', coalesce(triggerBody()?['data']?['essentials']?['alertTargetIDs'][0], ''))}"
        mrkdwn_in  = ["text"]
      }
    ]
  })
}

resource "azapi_resource_action" "bench_alert_relay_cb" {
  type                   = "Microsoft.Logic/workflows/triggers@2019-05-01"
  resource_id            = "${azurerm_logic_app_workflow.bench_alert_relay.id}/triggers/${azurerm_logic_app_trigger_http_request.bench_alert_trigger.name}"
  action                 = "listCallbackUrl"
  method                 = "POST"
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
  parent_id = data.azurerm_resource_group.rg.id

  identity {
    type = "SystemAssigned"
  }

  body = {
    properties = {
      state = var.anomaly_alert_enabled ? "Enabled" : "Disabled"
      definition = {
        "$schema"        = "https://schema.management.azure.com/providers/Microsoft.Logic/schemas/2016-06-01/workflowdefinition.json#"
        "contentVersion" = "1.0.0.0"
        "parameters"     = {}
        "triggers" = {
          "recurrence" = {
            "type" = "Recurrence"
            "recurrence" = {
              "frequency" = var.anomaly_schedule_frequency
              "interval"  = var.anomaly_schedule_interval
              "timeZone"  = var.anomaly_schedule_timezone
              "schedule" = {
                "hours" = [var.anomaly_schedule_hour]
              }
            }
          }
        }
        "actions" = {
          "QueryLogs" = {
            "type" = "Http"
            "inputs" = {
              "method" = "POST"
              "uri"    = "https://api.loganalytics.io/v1/workspaces/${data.azurerm_log_analytics_workspace.cluster_law.workspace_id}/query"
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
                  let benchmarkLogs =
                      ContainerLog
                      | where LogEntry startswith "{" and LogEntry has "benchmark"
                      | extend log = parse_json(LogEntry)
                      | where isnotnull(log.benchmark)
                      | extend benchmark = tostring(log.benchmark),
                               duration_ms = todouble(log['time-taken-ms']),
                               run_id = "TODO",
                               scale_factor = todouble(log.parameters['scale-factor'])
                      | where benchmark == "TPC-H (OLAP)" and scale_factor == ${var.anomaly_scale_factor};
                  let latestBenchmark =
                      benchmarkLogs
                      | summarize arg_max(TimeGenerated, *)
                      | extend current_ms = todouble(duration_ms), k = 1;
                  let previousBenchmark =
                      benchmarkLogs
                      | where TimeGenerated < toscalar(latestBenchmark | project TimeGenerated)
                      | top 1 by TimeGenerated desc
                      | project prev_ms = todouble(duration_ms), k = 1;
                  let prevN =
                      benchmarkLogs
                      | order by TimeGenerated desc
                      | top N+1 by TimeGenerated desc
                      | top N by TimeGenerated asc
                      | project duration_ms = todouble(duration_ms), TimeGenerated;
                  let baseline = prevN | summarize baseline_mean = avg(duration_ms), baseline_std = stdev(duration_ms) | extend k = 1;
                  latestBenchmark
                    | join kind=inner baseline on k
                    | join kind=leftouter previousBenchmark on k
                    | where isnotnull(baseline_std) and baseline_std > 0
                    | extend threshold_value_slow = baseline_mean + (${var.anomaly_sigma} * baseline_std),
                             threshold_value_fast = baseline_mean - (${var.anomaly_sigma} * baseline_std)
                    | extend slow_violation = current_ms > threshold_value_slow,
                             fast_violation = current_ms < threshold_value_fast,
                             prev_diff_ratio = iif(isnotnull(prev_ms) and prev_ms > 0, abs(current_ms - prev_ms) / prev_ms, real(null))
                    | extend new_normal_candidate = isnotnull(prev_diff_ratio) and prev_diff_ratio <= ${var.anomaly_new_normal_relative_threshold}
                    | where (slow_violation or fast_violation) // TEMP and not(new_normal_candidate)
                    | project run_id, slow_violation, fast_violation, TimeGenerated, current_ms, baseline_mean, baseline_std, prev_ms, prev_diff_ratio
                KQL
              }
            }
          }
          "IfAnomaly" = {
            "type" = "If"
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
                "type" = "Http"
                "inputs" = {
                  "method" = "POST"
                  "uri"    = var.slack_webhook_url
                  "headers" = {
                    "Content-Type" = "application/json"
                  }
                  "body" = {
                    "attachments" = [
                      {
                        "color"     = "#D32F2F"
                        "title"     = "@{concat('Benchmark anomaly: TPCH ', if(equals(first(body('QueryLogs')?['tables'][0]?['rows'])[1], true), 'ran slower', 'ran faster'))}"
                        "text"      = "${var.slack_subteam_id != "" ? "<!subteam^${var.slack_subteam_id}|${var.slack_subteam_label}> " : ""}@{concat('*Scale factor:* ', string(${var.anomaly_scale_factor}), '\n', '*Condition:* ± ', string(${var.anomaly_sigma}), 'σ over last ', '${var.anomaly_baseline_n} runs', '\n', '*Run:* https://github.com/${var.anomaly_repo}/actions/runs/', string(first(body('QueryLogs')?['tables'][0]?['rows'])[0]))}"
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
  scope                = data.azurerm_log_analytics_workspace.cluster_law.id
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

  scopes = [data.azurerm_log_analytics_workspace.cluster_law.id]

  criteria {
    query                   = <<-KQL
      ContainerLog
      | where LogEntry startswith "{" and LogEntry has "benchmark"
      | extend log = parse_json(LogEntry)
      | where isnotnull(log.benchmark)
      | extend benchmark = tostring(log.benchmark),
               scale_factor = todouble(log.parameters['scale-factor'])
      | where benchmark == "TPC-H (OLAP)" and scale_factor == ${var.anomaly_scale_factor}
      | summarize c = count()
      | where c == 0
      | project trigger = 1
    KQL
    time_aggregation_method = "Count"
    operator                = "GreaterThan"
    threshold               = 0
    failing_periods {
      number_of_evaluation_periods             = 1
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
                  name = "Scope"
                  value = {
                    resourceIds = [
                      data.azurerm_log_analytics_workspace.cluster_law.id
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
                  value      = "P30D"
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
                    ContainerLog
                    | where LogEntry startswith "{" and LogEntry has "benchmark"
                    | extend log = parse_json(LogEntry)
                    | where isnotnull(log.benchmark)
                    | extend benchmark = tostring(log.benchmark),
                             duration_ms = todouble(log['time-taken-ms']),
                             scale_factor = todouble(log.parameters['scale-factor'])
                    | where benchmark == "TPC-H (OLAP)" and scale_factor == ${var.anomaly_scale_factor}
                    | top ${var.anomaly_baseline_n} by TimeGenerated desc
                    | extend duration_minutes = todouble(duration_ms) / 60000
                    | order by TimeGenerated asc
                    | project TimeGenerated, duration_minutes
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
                  value      = var.cluster_law_name
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
                        name = "duration_minutes"
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
            relative = "30d"
          }
          type = "MsPortalFx.Composition.Configuration.ValueTypes.TimeRange"
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
                relative    = "30d"
              }
              displayCache = {
                name  = "UTC Time"
                value = "Past 30 days"
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
