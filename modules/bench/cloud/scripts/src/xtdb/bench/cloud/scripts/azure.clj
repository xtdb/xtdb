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
         :filter-param \"scale-factor\"                           ; parameter name to filter by (e.g., \"scale-factor\", \"devices\", \"size\")
         :filter-value 1                                          ; value to filter by
         :filter-is-string false                                  ; whether filter-value is a string (for KQL quoting)
         :repo \"xtdb/xtdb\"                                       ; github repo to filter by (default: xtdb/xtdb)
         :branch \"main\"                                          ; git branch to filter by (default: main)
         :limit 30                                                ; max number of results (default: 30)
         :metric \"time-taken-ms\"                                 ; metric to extract (default: \"time-taken-ms\")
         :unit :millis}                                           ; unit for conversion (default: :millis)

  Returns data in the format expected by plot-timeseries:
  [{:timestamp \"2025-01-15T10:30:00Z\" :value 1234.5} ...]"
  [{:keys [workspace-id resource-group workspace-name table-name log-field benchmark
           filter-param filter-value filter-is-string repo branch limit metric unit]
    :or {resource-group "cloud-benchmark-resources"
         workspace-name "xtdb-bench-cluster-law"
         table-name "ContainerLog"
         log-field "LogEntry"
         benchmark "TPC-H (OLAP)"
         filter-param nil
         filter-value nil
         filter-is-string false
         repo "xtdb/xtdb"
         branch "main"
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

        ;; Construct the filter clause based on param type
        filter-clause (when (and filter-param filter-value)
                        (if filter-is-string
                          (format " and filter_param == \"%s\"" filter-value)
                          (format " and filter_param == %s" filter-value)))

        ;; Build the filter_param extraction based on what we're filtering by
        filter-extract (if filter-param
                         (if filter-is-string
                           (format "filter_param = tostring(log.parameters['%s'])" filter-param)
                           (format "filter_param = todouble(log.parameters['%s'])" filter-param))
                         "filter_param = \"\"")

        query (format "%s
  | where %s startswith \"{\" and %s has \"benchmark\"
  | extend log = parse_json(%s)
  | where isnotnull(log.benchmark) and (isnull(log.stage) or log.stage != \"init\")
  | extend
      benchmark = tostring(log.benchmark),
      metric_value = todouble(log['%s']),
      github_repo = tostring(log['github-repo']),
      git_branch = tostring(log['git-branch']),
      %s
  | where benchmark == \"%s\" and github_repo == \"%s\" and git_branch == \"%s\"%s and isnotnull(metric_value)
  | top %d by TimeGenerated desc
  | order by TimeGenerated asc
  | project TimeGenerated, metric_value"
                      table-name
                      log-field
                      log-field
                      log-field
                      metric
                      filter-extract
                      benchmark
                      repo
                      branch
                      (or filter-clause "")
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
                       :filter-param filter-param
                       :filter-value filter-value
                       :metric metric
                       :table-name table-name
                       :workspace-id workspace-id})))

    ;; Convert to timeseries format
    (mapv (fn [{:keys [TimeGenerated metric_value]}]
            {:timestamp TimeGenerated
             :value metric_value})
          rows)))
