(ns xtdb.bench.cloud.scripts.azure
  "Azure Log Analytics integration for benchmark data."
  (:require [babashka.process :as proc]
            [cheshire.core :as json]
            [clojure.string :as str]))

(defn fetch-azure-benchmark-timeseries
  "Fetch benchmark timeseries data from Azure Log Analytics.

  opts: {:workspace-id \"<workspace-id>\"                         ; Log Analytics workspace ID (optional, will query Azure if not provided)
         :resource-group \"cloud-benchmark-resources\"            ; Azure resource group (default)
         :workspace-name \"xtdb-bench-cluster-law\"               ; Log Analytics workspace name (default)
         :table-name \"ContainerLogV2\"                           ; Log Analytics table name (default: ContainerLogV2)
         :log-field \"LogMessage\"                                ; Field containing log messages (default: LogMessage for ContainerLogV2, use LogEntry for ContainerLog)
         :benchmark \"TPC-H (OLAP)\"                              ; benchmark name (default)
         :scale-factor 1                                          ; scale factor (default: 1)
         :limit 30                                                ; max number of results (default: 30)
         :metric \"time-taken-ms\"                                 ; metric to extract (default: \"time-taken-ms\")
         :unit :millis}                                           ; unit for conversion (default: :millis)

  Returns data in the format expected by plot-timeseries:
  [{:timestamp \"2025-01-15T10:30:00Z\" :value 1234.5} ...]"
  [{:keys [workspace-id resource-group workspace-name table-name log-field benchmark scale-factor limit metric unit]
    :or {resource-group "cloud-benchmark-resources"
         workspace-name "xtdb-bench-cluster-law"
         table-name "ContainerLog"
         log-field "LogEntry"
         benchmark "TPC-H (OLAP)"
         scale-factor nil
         limit 30
         metric "time-taken-ms"
         unit :millis}}]
  (let [;; Get workspace customer ID (GUID) from Azure if not provided
        workspace-id (or workspace-id
                         (let [result (proc/shell {:out :string}
                                                  "az" "monitor" "log-analytics" "workspace" "show"
                                                  "--resource-group" resource-group
                                                  "--workspace-name" workspace-name
                                                  "--query" "customerId"
                                                  "--output" "tsv")]
                           (str/trim (:out result))))

        ;; Construct the Kusto query
        ;; Only filter by scale_factor if provided
        scale-factor-clause (if scale-factor
                              (format " and scale_factor == %s" scale-factor)
                              "")
        query (format "%s
  | where %s startswith \"{\" and %s has \"benchmark\"
  | extend log = parse_json(%s)
  | where isnotnull(log.benchmark) and (isnull(log.stage) or log.stage != \"init\")
  | extend
      benchmark = tostring(log.benchmark),
      metric_value = todouble(log['%s']),
      scale_factor = todouble(log.parameters['scale-factor'])
  | where benchmark == \"%s\"%s and isnotnull(metric_value)
  | top %d by TimeGenerated desc
  | order by TimeGenerated asc
  | project TimeGenerated, metric_value"
                      table-name
                      log-field
                      log-field
                      log-field
                      metric
                      benchmark
                      scale-factor-clause
                      limit)

        ;; Run az CLI command
        result (proc/shell {:out :string}
                           "az" "monitor" "log-analytics" "query"
                           "--workspace" workspace-id
                           "--analytics-query" query
                           "--output" "json")

        ;; Parse JSON response
        rows (json/parse-string (:out result) true)]

    ;; Check if we got any data
    (when (empty? rows)
      (throw (ex-info "No benchmark data found in Log Analytics"
                      {:benchmark benchmark
                       :scale-factor scale-factor
                       :metric metric
                       :table-name table-name
                       :workspace-id workspace-id})))

    ;; Convert to timeseries format
    (mapv (fn [{:keys [TimeGenerated metric_value]}]
            {:timestamp TimeGenerated
             :value metric_value})
          rows)))
