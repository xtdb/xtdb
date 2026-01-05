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

variable "subscription_id" {
  description = "Azure subscription ID for benchmark resources"
  type        = string
  default     = "91804669-c60b-4727-afa2-d7021fe5055b"
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
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
    | where isnotnull(log.benchmark) and isnotnull(log.step) and isnotnull(log.metric) and log.stage != "init"
  KQL

  # Benchmark configurations for anomaly detection and dashboard
  # param_path: path from log object to the filter parameter (e.g., "parameters['scale-factor']")
  # metric_path: path to the metric being measured (default: 'time-taken-ms', auctionmark uses 'throughput')
  # dashboard_idx: position index for 2x2 grid layout (0=top-left, 1=top-right, 2=bottom-left, 3=bottom-right)
  benchmarks = {
    tpch = {
      name            = "TPC-H (OLAP)"
      display_name    = "TPC-H"
      logic_app_name  = var.tpch_anomaly_logic_app_name
      enabled         = var.tpch_anomaly_alert_enabled
      param_name      = "scale-factor"
      param_path      = "parameters['scale-factor']"
      param_value     = var.tpch_anomaly_scale_factor
      param_is_string = false
      metric_path     = "'time-taken-ms'"
      metric_name     = "duration_minutes"
      dashboard_idx   = 0
    }
    yakbench = {
      name            = "Yakbench"
      display_name    = "Yakbench"
      logic_app_name  = var.yakbench_anomaly_logic_app_name
      enabled         = var.yakbench_anomaly_alert_enabled
      param_name      = "scale-factor"
      param_path      = "parameters['scale-factor']"
      param_value     = var.yakbench_anomaly_scale_factor
      param_is_string = false
      metric_path     = "'time-taken-ms'"
      metric_name     = "duration_minutes"
      dashboard_idx   = 1
    }
    readings = {
      name            = "Readings benchmarks"
      display_name    = "Readings"
      logic_app_name  = var.readings_anomaly_logic_app_name
      enabled         = var.readings_anomaly_alert_enabled
      param_name      = "devices"
      param_path      = "parameters['devices']"
      param_value     = var.readings_anomaly_devices
      param_is_string = false
      metric_path     = "'time-taken-ms'"
      metric_name     = "duration_minutes"
      dashboard_idx   = 2
    }
    auctionmark = {
      name            = "Auction Mark OLTP"
      display_name    = "AuctionMark"
      logic_app_name  = var.auctionmark_anomaly_logic_app_name
      enabled         = var.auctionmark_anomaly_alert_enabled
      param_name      = "duration"
      param_path      = "parameters['duration']"
      param_value     = var.auctionmark_anomaly_duration
      param_is_string = true
      metric_path     = "'throughput'"
      metric_name     = "throughput"
      dashboard_idx   = 3
    }
    tsbs-iot = {
      name            = "TSBS IoT"
      display_name    = "TSBS IoT"
      logic_app_name  = var.tsbs_iot_anomaly_logic_app_name
      enabled         = var.tsbs_iot_anomaly_alert_enabled
      param_name      = "devices"
      param_path      = "parameters['devices']"
      param_value     = var.tsbs_iot_anomaly_devices
      param_is_string = false
      metric_path     = "'time-taken-ms'"
      metric_name     = "duration_minutes"
      dashboard_idx   = 4
    }
  }

  # Dashboard part positions (6 cols wide, 4 rows tall each)
  dashboard_positions = {
    0 = { x = 0, y = 0 }
    1 = { x = 6, y = 0 }
    2 = { x = 0, y = 4 }
    3 = { x = 6, y = 4 }
    4 = { x = 0, y = 8 }
  }

  # Filter expressions per benchmark (string params quoted, numeric params use todouble)
  param_filter_expr = {
    for key, bench in local.benchmarks : key => bench.param_is_string
    ? "filter_param = tostring(log.${bench.param_path})\n                      | where benchmark == \"${bench.name}\" and filter_param == \"${bench.param_value}\""
    : "filter_param = todouble(log.${bench.param_path})\n                      | where benchmark == \"${bench.name}\" and filter_param == ${bench.param_value}"
  }

  # TPC-H individual query breakdown KQL (cold and hot)
  # Note: query-level logs don't have "benchmark" field, so we match on stage pattern
  tpch_query_types = ["cold", "hot"]
  tpch_query_kql = {
    for query_type in local.tpch_query_types : query_type => trimspace(<<-KQL
      let raw = ContainerLog
      | where LogEntry startswith "{" and LogEntry contains "${query_type}-queries-q"
      | extend log = parse_json(LogEntry)
      | extend stage = tostring(log.stage),
               duration_ms = todouble(log['time-taken-ms']),
               bench_id = tostring(log['bench-id'])
      | where stage startswith "${query_type}-queries-q"
      | extend query_name = extract("${query_type}-queries-(q[0-9]+-[a-z-]+)", 1, stage)
      | where isnotempty(query_name)
      | summarize duration_ms = max(duration_ms) by bench_id, query_name, TimeGenerated;
      let thresholds = raw | summarize p20 = percentile(duration_ms, 20) by query_name;
      raw
      | join kind=inner thresholds on query_name
      | where duration_ms > p20
      | top ${var.anomaly_baseline_n * 20} by TimeGenerated desc
      | order by TimeGenerated asc
      | project TimeGenerated, query_name, duration_ms
    KQL
    )
  }

  # Yakbench profile query breakdown KQL (global, max-user, mean-user)
  # Extracts from the "profiles" JSON blob in output-profile-data stage
  yakbench_profile_types = ["global", "max-user", "mean-user"]
  yakbench_query_kql = {
    for profile_type in local.yakbench_profile_types : profile_type => trimspace(<<-KQL
      let raw = ContainerLog
      | where LogEntry startswith "{" and LogEntry contains "profiles"
      | extend log = parse_json(LogEntry)
      | where isnotnull(log.profiles)
      | extend bench_id = tostring(log['bench-id'])
      | mv-expand profile = log.profiles['${profile_type}']
      | extend query_id = tostring(profile.id),
               mean_ms = todouble(profile['mean']) / 1000000.0
      | summarize mean_ms = max(mean_ms) by bench_id, query_id, TimeGenerated;
      let thresholds = raw | summarize p20 = percentile(mean_ms, 20) by query_id;
      raw
      | join kind=inner thresholds on query_id
      | where mean_ms > p20
      | top ${var.anomaly_baseline_n * 20} by TimeGenerated desc
      | order by TimeGenerated asc
      | project TimeGenerated, query_id, mean_ms
    KQL
    )
  }

  # Dashboard KQL queries per benchmark
  # auctionmark uses string comparison (ISO duration), others use numeric comparison
  dashboard_queries = {
    for key, bench in local.benchmarks : key => bench.param_is_string ? trimspace(<<-KQL
      ContainerLog
      | where LogEntry startswith "{" and LogEntry has "benchmark"
      | extend log = parse_json(LogEntry)
      | where isnotnull(log.benchmark) and log.stage != "init"
      | extend benchmark = tostring(log.benchmark),
               filter_param = tostring(log.${bench.param_path}),
               ${bench.metric_name} = todouble(log[${bench.metric_path}])
      | where benchmark == "${bench.name}" and filter_param == "${bench.param_value}"
      | top ${var.anomaly_baseline_n} by TimeGenerated desc
      | order by TimeGenerated asc
      | project TimeGenerated, ${bench.metric_name}
    KQL
      ) : trimspace(<<-KQL
      ContainerLog
      | where LogEntry startswith "{" and LogEntry has "benchmark"
      | extend log = parse_json(LogEntry)
      | where isnotnull(log.benchmark) and log.stage != "init"
      | extend benchmark = tostring(log.benchmark),
               filter_param = todouble(log.${bench.param_path}),
               duration_ms = todouble(log[${bench.metric_path}])
      | where benchmark == "${bench.name}" and filter_param == ${bench.param_value}
      | top ${var.anomaly_baseline_n} by TimeGenerated desc
      | extend duration_minutes = todouble(duration_ms) / 60000
      | order by TimeGenerated asc
      | project TimeGenerated, duration_minutes
    KQL
    )
  }
}

# Logic App: Relay Azure Monitor alerts to Slack (Incoming Webhook)
resource "azurerm_logic_app_workflow" "bench_alert_relay" {
  name                = "xtdb-bench-alert-relay"
  location            = var.location
  resource_group_name = var.resource_group_name
  enabled             = true

  lifecycle {
    prevent_destroy = true
  }
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

  lifecycle {
    prevent_destroy = true
  }
}

# Logic App: Scheduled anomaly detection (2σ over P30D via Logs Query API)
resource "azapi_resource" "bench_anomaly" {
  for_each = local.benchmarks

  type      = "Microsoft.Logic/workflows@2019-05-01"
  name      = each.value.logic_app_name
  location  = var.location
  parent_id = data.azurerm_resource_group.rg.id

  identity {
    type = "SystemAssigned"
  }

  body = {
    properties = {
      state = each.value.enabled ? "Enabled" : "Disabled"
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
                      | where isnotnull(log.benchmark) and log.stage != "init"
                      | extend benchmark = tostring(log.benchmark),
                               metric_value = todouble(log[${each.value.metric_path}]),
                               run_id = coalesce(tostring(log['github-run-id']), "n/a"),
                               ${local.param_filter_expr[each.key]};
                  let latestBenchmark =
                      benchmarkLogs
                      | summarize arg_max(TimeGenerated, *)
                      | extend current_value = todouble(metric_value), k = 1;
                  let previousBenchmark =
                      benchmarkLogs
                      | where TimeGenerated < toscalar(latestBenchmark | project TimeGenerated)
                      | top 1 by TimeGenerated desc
                      | project prev_value = todouble(metric_value), k = 1;
                  let prevN =
                      benchmarkLogs
                      | order by TimeGenerated desc
                      | top N+1 by TimeGenerated desc
                      | top N by TimeGenerated asc
                      | project metric_value = todouble(metric_value), TimeGenerated;
                  let baseline = prevN | summarize baseline_mean = avg(metric_value), baseline_std = stdev(metric_value) | extend k = 1;
                  latestBenchmark
                    | join kind=inner baseline on k
                    | join kind=leftouter previousBenchmark on k
                    | where isnotnull(baseline_std) and baseline_std > 0
                    | extend threshold_value_slow = baseline_mean + (${var.anomaly_sigma} * baseline_std),
                             threshold_value_fast = baseline_mean - (${var.anomaly_sigma} * baseline_std)
                    | extend slow_violation = current_value > threshold_value_slow,
                             fast_violation = current_value < threshold_value_fast,
                             prev_diff_ratio = iif(isnotnull(prev_value) and prev_value > 0, abs(current_value - prev_value) / prev_value, real(null))
                    | extend new_normal_candidate = isnotnull(prev_diff_ratio) and prev_diff_ratio <= ${var.anomaly_new_normal_relative_threshold}
                    | where (slow_violation or fast_violation) and not(new_normal_candidate)
                    | project run_id, slow_violation, fast_violation, TimeGenerated, current_value, baseline_mean, baseline_std, prev_value, prev_diff_ratio
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
                        "title"     = "@{concat('Benchmark anomaly: ${each.value.display_name} ', if(equals(first(body('QueryLogs')?['tables'][0]?['rows'])[1], true), 'ran slower', 'ran faster'))}"
                        "text"      = "${var.slack_subteam_id != "" ? "<!subteam^${var.slack_subteam_id}|${var.slack_subteam_label}> " : ""}@{concat('*${each.value.param_name}:* ', '${each.value.param_value}', '\n', '*Condition:* ± ', string(${var.anomaly_sigma}), 'σ over last ', '${var.anomaly_baseline_n} runs', '\n', '*Run:* https://github.com/${var.anomaly_repo}/actions/runs/', string(first(body('QueryLogs')?['tables'][0]?['rows'])[0]))}"
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
  for_each = local.benchmarks

  scope                            = data.azurerm_log_analytics_workspace.cluster_law.id
  role_definition_name             = "Log Analytics Reader"
  principal_id                     = azapi_resource.bench_anomaly[each.key].output.identity.principalId
  skip_service_principal_aad_check = true

  lifecycle {
    create_before_destroy = true
    # These assignments already exist - ignore drift from terraform defaults
    ignore_changes = [principal_id, skip_service_principal_aad_check]
  }
}

# Azure Portal Dashboard: recent runs (line chart per benchmark)
resource "azurerm_portal_dashboard" "bench_dashboard" {
  name                = var.dashboard_name
  resource_group_name = var.resource_group_name
  location            = var.location

  dashboard_properties = jsonencode({
    lenses = {
      "0" = {
        order = 0
        parts = merge(
          # Existing benchmark summary charts (2x2 grid)
          {
            for key, bench in local.benchmarks : "${key}-runs-timeseries" => {
              position = {
                x       = local.dashboard_positions[bench.dashboard_idx].x
                y       = local.dashboard_positions[bench.dashboard_idx].y
                colSpan = 6
                rowSpan = 4
              }
              metadata = {
                inputs = [
                  { name = "resourceTypeMode", isOptional = true },
                  { name = "ComponentId", isOptional = true },
                  {
                    name = "Scope"
                    value = {
                      resourceIds = [data.azurerm_log_analytics_workspace.cluster_law.id]
                    }
                    isOptional = true
                  },
                  { name = "PartId", value = "bench-${key}-part", isOptional = true },
                  { name = "Version", value = "2.0", isOptional = true },
                  { name = "TimeRange", value = "P30D", isOptional = true },
                  { name = "DashboardId", isOptional = true },
                  { name = "DraftRequestParameters", isOptional = true },
                  { name = "Query", value = local.dashboard_queries[key], isOptional = true },
                  { name = "ControlType", value = "FrameControlChart", isOptional = true },
                  { name = "SpecificChart", value = "Line", isOptional = true },
                  { name = "PartTitle", value = "Benchmark ${bench.display_name} Runs", isOptional = true },
                  { name = "PartSubTitle", value = var.cluster_law_name, isOptional = true },
                  {
                    name = "Dimensions"
                    value = {
                      xAxis       = { name = "TimeGenerated", type = "datetime" }
                      yAxis       = [{ name = bench.metric_name, type = "real" }]
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
                  { name = "IsQueryContainTimeRange", value = false, isOptional = true }
                ]
                type     = "Extension/Microsoft_OperationsManagementSuite_Workspace/PartType/LogsDashboardPart"
                settings = {}
              }
            }
          },
          # TPC-H individual query breakdown charts (cold and hot) - row 12
          {
            for idx, query_type in local.tpch_query_types : "tpch-${query_type}-queries" => {
              position = {
                x       = idx * 6
                y       = 12
                colSpan = 6
                rowSpan = 4
              }
              metadata = {
                inputs = [
                  { name = "resourceTypeMode", isOptional = true },
                  { name = "ComponentId", isOptional = true },
                  {
                    name = "Scope"
                    value = {
                      resourceIds = [data.azurerm_log_analytics_workspace.cluster_law.id]
                    }
                    isOptional = true
                  },
                  { name = "PartId", value = "tpch-${query_type}-queries-part", isOptional = true },
                  { name = "Version", value = "2.0", isOptional = true },
                  { name = "TimeRange", value = "P30D", isOptional = true },
                  { name = "DashboardId", isOptional = true },
                  { name = "DraftRequestParameters", isOptional = true },
                  { name = "Query", value = local.tpch_query_kql[query_type], isOptional = true },
                  { name = "ControlType", value = "FrameControlChart", isOptional = true },
                  { name = "SpecificChart", value = "Line", isOptional = true },
                  { name = "PartTitle", value = "TPC-H ${title(query_type)} Queries (SF=${var.tpch_anomaly_scale_factor})", isOptional = true },
                  { name = "PartSubTitle", value = "Individual query times", isOptional = true },
                  {
                    name = "Dimensions"
                    value = {
                      xAxis       = { name = "TimeGenerated", type = "datetime" }
                      yAxis       = [{ name = "duration_ms", type = "real" }]
                      splitBy     = [{ name = "query_name", type = "string" }]
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
                  { name = "IsQueryContainTimeRange", value = false, isOptional = true }
                ]
                type     = "Extension/Microsoft_OperationsManagementSuite_Workspace/PartType/LogsDashboardPart"
                settings = {}
              }
            }
          },
          # Yakbench profile breakdown charts (global, max-user, mean-user) - row 16
          {
            for idx, profile_type in local.yakbench_profile_types : "yakbench-${profile_type}-queries" => {
              position = {
                x       = idx * 4
                y       = 16
                colSpan = 4
                rowSpan = 4
              }
              metadata = {
                inputs = [
                  { name = "resourceTypeMode", isOptional = true },
                  { name = "ComponentId", isOptional = true },
                  {
                    name = "Scope"
                    value = {
                      resourceIds = [data.azurerm_log_analytics_workspace.cluster_law.id]
                    }
                    isOptional = true
                  },
                  { name = "PartId", value = "yakbench-${profile_type}-queries-part", isOptional = true },
                  { name = "Version", value = "2.0", isOptional = true },
                  { name = "TimeRange", value = "P30D", isOptional = true },
                  { name = "DashboardId", isOptional = true },
                  { name = "DraftRequestParameters", isOptional = true },
                  { name = "Query", value = local.yakbench_query_kql[profile_type], isOptional = true },
                  { name = "ControlType", value = "FrameControlChart", isOptional = true },
                  { name = "SpecificChart", value = "Line", isOptional = true },
                  { name = "PartTitle", value = "Yakbench ${replace(title(profile_type), "-", " ")} Queries", isOptional = true },
                  { name = "PartSubTitle", value = "Mean query times (ms)", isOptional = true },
                  {
                    name = "Dimensions"
                    value = {
                      xAxis       = { name = "TimeGenerated", type = "datetime" }
                      yAxis       = [{ name = "mean_ms", type = "real" }]
                      splitBy     = [{ name = "query_id", type = "string" }]
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
                  { name = "IsQueryContainTimeRange", value = false, isOptional = true }
                ]
                type     = "Extension/Microsoft_OperationsManagementSuite_Workspace/PartType/LogsDashboardPart"
                settings = {}
              }
            }
          }
        )
      }
    }
    metadata = {
      model = {
        timeRange = {
          value = { relative = "30d" }
          type  = "MsPortalFx.Composition.Configuration.ValueTypes.TimeRange"
        }
        filterLocale = { value = "en-us" }
        filters = {
          value = {
            MsPortalFx_TimeRange = {
              model        = { format = "utc", granularity = "auto", relative = "30d" }
              displayCache = { name = "UTC Time", value = "Past 30 days" }
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
