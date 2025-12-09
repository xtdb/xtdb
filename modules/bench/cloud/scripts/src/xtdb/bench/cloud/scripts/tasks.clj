(ns xtdb.bench.cloud.scripts.tasks
  (:require [babashka.cli :as cli]
            [babashka.process :as proc]
            [cheshire.core :as json]
            [clojure.pprint :as pprint]
            [clojure.string :as str]))

(defn format-duration
  [unit value]
  (when value
    (let [nanos (case unit
                  :nanos value
                  :micros (* value 1e3)
                  :millis (* value 1e6)
                  :seconds (* value 1e9)
                  :minutes (* value 60e9)
                  :hours (* value 3600e9))]
      (cond
        (>= nanos 3600e9) (format "%.1fh" (/ nanos 3600e9))
        (>= nanos 60e9) (format "%.1fm" (/ nanos 60e9))
        (>= nanos 1e9) (format "%.2fs" (/ nanos 1e9))
        (>= nanos 1e6) (format "%.0fms" (/ nanos 1e6))
        (>= nanos 1e3) (format "%.0fµs" (/ nanos 1e3))
        :else (format "%.0fns" (double nanos))))))

(defn title-case
  [s]
  (->> (str/split s #"-")
       (map str/capitalize)
       (str/join " ")))

(defn parse-benchmark-summary
  [log-lines]
  (let [benchmark-line (first (filter #(str/includes? % "\"stage\":\"summary\"") log-lines))
        benchmark-summary (when benchmark-line
                            (try
                              (json/parse-string benchmark-line true)
                              (catch Exception _ nil)))
        benchmark-total-time-ms (when benchmark-summary
                                  (:time-taken-ms benchmark-summary))]
    {:benchmark-total-time-ms benchmark-total-time-ms
     :benchmark-summary benchmark-summary}))

(defmulti parse-log (fn [benchmark-type _log-file-path] benchmark-type))

(defmethod parse-log :default [benchmark-type _log-file-path]
  (throw (ex-info (format "Unsupported benchmark type: %s" benchmark-type)
                  {:benchmark-type benchmark-type})))

(defmethod parse-log "tpch" [_benchmark-type log-file-path]
  (let [content (slurp log-file-path)
        lines (str/split-lines content)
        stage-lines (filter #(str/starts-with? % "{\"stage\":") lines)
        stages (mapv (fn [line]
                       (try
                         (json/parse-string line true)
                         (catch Exception e
                           (throw (ex-info (str "Failed to parse JSON line: " line)
                                           {:line line :error (.getMessage e)})))))
                     stage-lines)
        query-stages (filterv (fn [stage]
                                (let [stage-name (:stage stage)]
                                  (or (str/starts-with? stage-name "hot-queries-q")
                                      (str/starts-with? stage-name "cold-queries-q"))))
                              stages)
        {:keys [benchmark-total-time-ms benchmark-summary]} (parse-benchmark-summary lines)]
    {:all-stages stages
     :query-stages query-stages
     :ingest-stages (filterv #(contains? #{"submit-docs" "sync" "finish-block" "compact" "ingest"} (:stage %)) stages)
     :benchmark-total-time-ms benchmark-total-time-ms
     :benchmark-summary benchmark-summary}))

(defmethod parse-log "yakbench" [_benchmark-type log-file-path]
  (let [content (slurp log-file-path)
        lines (str/split-lines content)
        profiles-line (first (filter #(str/includes? % "\"profiles\":") lines))
        profiles (when profiles-line
                   (try
                     (:profiles (json/parse-string profiles-line true))
                     (catch Exception _ nil)))
        {:keys [benchmark-total-time-ms benchmark-summary]} (parse-benchmark-summary lines)]
    {:profiles profiles
     :benchmark-total-time-ms benchmark-total-time-ms
     :benchmark-summary benchmark-summary}))

(defmethod parse-log "readings" [_benchmark-type log-file-path]
  (let [content (slurp log-file-path)
        lines (str/split-lines content)
        stage-lines (filter #(str/starts-with? % "{\"stage\":") lines)
        stages (mapv (fn [line]
                       (try
                         (json/parse-string line true)
                         (catch Exception e
                           (throw (ex-info (str "Failed to parse JSON line: " line)
                                           {:line line :error (.getMessage e)})))))
                     stage-lines)
        query-stages (filterv (fn [stage]
                                (let [stage-name (name (:stage stage))]
                                  (or (str/starts-with? stage-name "query-recent-interval-")
                                      (str/starts-with? stage-name "query-offset-"))))
                              stages)
        {:keys [benchmark-total-time-ms benchmark-summary]} (parse-benchmark-summary lines)]
    {:all-stages stages
     :query-stages query-stages
     :ingest-stages (filterv #(contains? #{"ingest" "sync" "compact"} (name (:stage %))) stages)
     :benchmark-total-time-ms benchmark-total-time-ms
     :benchmark-summary benchmark-summary}))

(defmethod parse-log "auctionmark" [_benchmark-type log-file-path]
  (let [content (slurp log-file-path)
        lines (str/split-lines content)
        auctionmark-line (first (filter #(str/includes? % "\"auctionmark\":") lines))
        auctionmark (when auctionmark-line
                      (try
                        (json/parse-string auctionmark-line true)
                        (catch Exception _ nil)))
        {:keys [benchmark-total-time-ms benchmark-summary]} (parse-benchmark-summary lines)]
    {:auctionmark auctionmark
     :benchmark-total-time-ms benchmark-total-time-ms
     :benchmark-summary benchmark-summary}))

(defmethod parse-log "clickbench" [_benchmark-type log-file-path]
  (let [content (slurp log-file-path)
        lines (str/split-lines content)
        stage-lines (filter #(str/starts-with? % "{\"stage\":") lines)
        stages (mapv (fn [line]
                       (try
                         (json/parse-string line true)
                         (catch Exception e
                           (throw (ex-info (str "Failed to parse JSON line: " line)
                                           {:line line :error (.getMessage e)})))))
                     stage-lines)
        {:keys [benchmark-total-time-ms benchmark-summary]} (parse-benchmark-summary lines)]
    {:all-stages stages
     :ingest-stages (filterv #(contains? #{"submit-docs" "sync" "finish-block" "compact" "ingest" "download"} (:stage %)) stages)
     :benchmark-total-time-ms benchmark-total-time-ms
     :benchmark-summary benchmark-summary}))

(defn tpch-stage->query-row
  [idx {:keys [stage time-taken-ms]}]
  (when-let [[_ hot-cold query-num name] (re-find #"^((?:hot|cold))-queries-q(\d+)-(.*)$" stage)]
    (let [friendly-name (title-case name)
          query-index (Long/parseLong query-num)
          duration-pt (format-duration :millis time-taken-ms)
          temp (str/capitalize hot-cold)
          q (str "Q" query-num)]
      {:query-order idx
       :query-index query-index
       :temp temp
       :q q
       :query (str temp " " q " " friendly-name)
       :query-name friendly-name
       :stage stage
       :time-taken-ms time-taken-ms
       :duration duration-pt})))

(defn tpch-summary->query-rows
  [summary]
  (let [rows (->> (:query-stages summary)
                  (map-indexed tpch-stage->query-row)
                  (remove nil?)
                  (sort-by :query-order)
                  vec)
        total-ms (reduce + (map :time-taken-ms rows))
        rows-with-percent (mapv (fn [row]
                                  (let [ms (:time-taken-ms row)
                                        pct (if (pos? total-ms)
                                              (* 100.0 (/ ms total-ms))
                                              0.0)]
                                    (-> row
                                        (assoc :percent-of-total (format "%.2f%%" pct))
                                        (dissoc :query-order :query-index))))
                                rows)]
    {:rows rows-with-percent
     :total-ms total-ms}))

(defn yakbench-query->query-row
  [profile {:keys [id mean p50 p90 p99 n sum]}]
  {:query (str (name profile) "/" id)
   :mean (format-duration :nanos mean)
   :p50 (format-duration :nanos p50)
   :p90 (format-duration :nanos p90)
   :p99 (format-duration :nanos p99)
   :n n
   :sum sum})

(defn yakbench-summary->query-rows
  [{:keys [profiles]}]
  (let [rows (mapcat (fn [[profile-name queries]]
                       (map (partial yakbench-query->query-row profile-name) queries))
                     profiles)
        total-nanos (reduce + 0 (keep :sum rows))
        rows-with-percent (mapv (fn [row]
                                  (let [sum-nanos (:sum row)
                                        pct (if (and sum-nanos (pos? total-nanos))
                                              (* 100.0 (/ sum-nanos total-nanos))
                                              0.0)]
                                    (assoc row :percent-of-total (format "%.2f%%" pct))))
                                rows)]
    {:rows rows-with-percent
     :total-ms (* total-nanos 1e-6)}))

(defn readings-stage->query-row
  [idx {:keys [stage time-taken-ms]}]
  (let [stage-name (name stage)
        friendly-name (cond
                        (str/starts-with? stage-name "query-recent-interval-")
                        (str "Recent " (title-case (subs stage-name (count "query-recent-interval-"))))

                        (str/starts-with? stage-name "query-offset-")
                        (let [[_ offset-len period interval] (re-find #"^query-offset-(\d+)-(.*?)-interval-(.*)$" stage-name)]
                          (str "Offset " offset-len " " (title-case period) " " (title-case interval)))

                        :else (title-case stage-name))
        duration-pt (format-duration :millis time-taken-ms)]
    {:query-order idx
     :query friendly-name
     :stage stage-name
     :time-taken-ms time-taken-ms
     :duration duration-pt}))

(defn readings-summary->query-rows
  [summary]
  (let [rows (->> (:query-stages summary)
                  (map-indexed readings-stage->query-row)
                  (sort-by :query-order)
                  vec)
        total-ms (reduce + (map :time-taken-ms rows))
        rows-with-percent (mapv (fn [row]
                                  (let [ms (:time-taken-ms row)
                                        pct (if (pos? total-ms)
                                              (* 100.0 (/ ms total-ms))
                                              0.0)]
                                    (-> row
                                        (assoc :percent-of-total (format "%.2f%%" pct))
                                        (dissoc :query-order))))
                                rows)]
    {:rows rows-with-percent
     :total-ms total-ms}))

(defn totals->string [query-ms benchmark-ms]
  (str (format "Total query time: %s (%s)"
               (or (format-duration :millis query-ms) "N/A")
               (if query-ms (java.time.Duration/ofMillis query-ms) "N/A"))
       "\n"
       (format "Total benchmark time: %s (%s)"
               (or (format-duration :millis benchmark-ms) "N/A")
               (if benchmark-ms (java.time.Duration/ofMillis benchmark-ms) "N/A"))))

(defn rows->string [columns rows]
  (-> (with-out-str
        (pprint/print-table columns rows))
      str/trim))

(defmulti summary->table :benchmark-type)

(defmethod summary->table "tpch" [summary]
  (let [{:keys [rows total-ms]} (tpch-summary->query-rows summary)]
    (str (totals->string total-ms (:benchmark-total-time-ms summary))
         "\n\n"
         (rows->string [:temp :q :query-name :time-taken-ms :duration :percent-of-total] rows))))

(defmethod summary->table "yakbench" [summary]
  (let [{:keys [rows total-ms]} (yakbench-summary->query-rows summary)]
    (str (totals->string total-ms (:benchmark-total-time-ms summary))
         "\n\n"
         (rows->string [:query :n :p50 :p90 :p99 :mean :percent-of-total] rows))))

(defmethod summary->table "readings" [summary]
  (let [{:keys [rows total-ms]} (readings-summary->query-rows summary)]
    (str (totals->string total-ms (:benchmark-total-time-ms summary))
         "\n\n"
         (rows->string [:query :time-taken-ms :duration :percent-of-total] rows))))

(defmethod summary->table "auctionmark" [summary]
  (let [{:keys [benchmark-total-time-ms]} summary]
    (format "Total benchmark time: %s (%s)"
            (or (format-duration :millis benchmark-total-time-ms) "N/A")
            (if benchmark-total-time-ms
              (java.time.Duration/ofMillis benchmark-total-time-ms)
              "N/A"))))

(defn clickbench-stage->row
  [idx {:keys [stage time-taken-ms]}]
  {:stage-order idx
   :stage (title-case stage)
   :time-taken-ms time-taken-ms
   :duration (format-duration :millis time-taken-ms)})

(defn clickbench-summary->stage-rows
  [summary]
  (let [rows (->> (:ingest-stages summary)
                  (map-indexed clickbench-stage->row)
                  (sort-by :stage-order)
                  vec)
        total-ms (reduce + (map :time-taken-ms rows))
        rows-with-percent (mapv (fn [row]
                                  (let [ms (:time-taken-ms row)
                                        pct (if (pos? total-ms)
                                              (* 100.0 (/ ms total-ms))
                                              0.0)]
                                    (-> row
                                        (assoc :percent-of-total (format "%.2f%%" pct))
                                        (dissoc :stage-order))))
                                rows)]
    {:rows rows-with-percent
     :total-ms total-ms}))

(defmethod summary->table "clickbench" [summary]
  (let [{:keys [rows total-ms]} (clickbench-summary->stage-rows summary)]
    (str (totals->string total-ms (:benchmark-total-time-ms summary))
         "\n\n"
         (rows->string [:stage :time-taken-ms :duration :percent-of-total] rows))))

(defn wrap-slack-code [& strings]
  (str "```\n" (str/join strings) "\n```"))

;; Slack wraps code blocks at 76 characters, so we need to keep columns minimal
(defmulti summary->slack :benchmark-type)

(defmethod summary->slack "tpch" [summary]
  (let [{:keys [rows total-ms]} (tpch-summary->query-rows summary)]
    (str
     (totals->string total-ms (:benchmark-total-time-ms summary))
     "\n\n"
     (wrap-slack-code
      (rows->string [:query :duration] rows)))))

(defmethod summary->slack "yakbench" [summary]
  (let [{:keys [rows total-ms]} (yakbench-summary->query-rows summary)]
    (str
     (totals->string total-ms (:benchmark-total-time-ms summary))
     "\n\n"
     (wrap-slack-code
      (rows->string [:query :p50 :p99 :mean] rows)))))

(defmethod summary->slack "readings" [summary]
  (let [{:keys [rows total-ms]} (readings-summary->query-rows summary)]
    (str
     (totals->string total-ms (:benchmark-total-time-ms summary))
     "\n\n"
     (wrap-slack-code
      (rows->string [:query :duration] rows)))))

(defmethod summary->slack "auctionmark" [summary]
  (let [{:keys [benchmark-total-time-ms]} summary]
    (format "Total benchmark time: %s (%s)"
            (or (format-duration :millis benchmark-total-time-ms) "N/A")
            (if benchmark-total-time-ms
              (java.time.Duration/ofMillis benchmark-total-time-ms)
              "N/A"))))

(defmethod summary->slack "clickbench" [summary]
  (let [{:keys [rows total-ms]} (clickbench-summary->stage-rows summary)]
    (str
     (totals->string total-ms (:benchmark-total-time-ms summary))
     "\n\n"
     (wrap-slack-code
      (rows->string [:stage :duration] rows)))))


(defn github-table
  "Generate a GitHub-flavored markdown table from columns and rows.

   columns: vector of {:key :column-key :header \"Column Header\" :format (optional fn)}
   rows: sequence of maps with keys matching column :key values

   Example:
   (github-table
     [{:key :name :header \"Name\"}
      {:key :age :header \"Age\" :format str}
      {:key :score :header \"Score\" :format #(format \"%.2f\" %)}]
     [{:name \"Alice\" :age 30 :score 95.5}
      {:name \"Bob\" :age 25 :score 87.3}])"
  [columns rows]
  (let [headers (map :header columns)
        separator (map (fn [h] (apply str (repeat (max 3 (count h)) "-"))) headers)
        format-cell (fn [col value]
                      (let [formatter (or (:format col) str)]
                        (formatter value)))
        format-row (fn [row]
                     (str "| "
                          (->> columns
                               (map (fn [col]
                                      (format-cell col (get row (:key col)))))
                               (str/join " | "))
                          " |"))]
    (str "| " (str/join " | " headers) " |\n"
         "| " (str/join " | " separator) " |\n"
         (->> rows
              (map format-row)
              (str/join "\n")))))

(defmulti summary->github-markdown :benchmark-type)

(defmethod summary->github-markdown "tpch" [summary]
  (let [{:keys [rows total-ms]} (tpch-summary->query-rows summary)
        columns [{:key :temp :header "Temp"}
                 {:key :q :header "Query"}
                 {:key :query-name :header "Query Name"}
                 {:key :time-taken-ms :header "Time (ms)"}
                 {:key :duration :header "Duration"}
                 {:key :percent-of-total :header "% of total"}]]
    (str (github-table columns rows)
         "\n\n"
         (totals->string total-ms (:benchmark-total-time-ms summary)))))

(defmethod summary->github-markdown "yakbench" [summary]
  (let [{:keys [rows total-ms]} (yakbench-summary->query-rows summary)
        columns [{:key :query :header "Query"}
                 {:key :n :header "N"}
                 {:key :p50 :header "P50"}
                 {:key :p90 :header "P90"}
                 {:key :p99 :header "P99"}
                 {:key :mean :header "Mean"}
                 {:key :percent-of-total :header "% of total"}]]
    (str (github-table columns rows)
         "\n\n"
         (totals->string total-ms (:benchmark-total-time-ms summary)))))

(defmethod summary->github-markdown "readings" [summary]
  (let [{:keys [rows total-ms]} (readings-summary->query-rows summary)
        columns [{:key :query :header "Query"}
                 {:key :time-taken-ms :header "Time (ms)"}
                 {:key :duration :header "Duration"}
                 {:key :percent-of-total :header "% of total"}]]
    (str (github-table columns rows)
         "\n\n"
         (totals->string total-ms (:benchmark-total-time-ms summary)))))

(defmethod summary->github-markdown "auctionmark" [summary]
  (let [{:keys [benchmark-total-time-ms]} summary]
    (format "Total benchmark time: %s (%s)"
            (or (format-duration :millis benchmark-total-time-ms) "N/A")
            (if benchmark-total-time-ms
              (java.time.Duration/ofMillis benchmark-total-time-ms)
              "N/A"))))

(defmethod summary->github-markdown "clickbench" [summary]
  (let [{:keys [rows total-ms]} (clickbench-summary->stage-rows summary)
        columns [{:key :stage :header "Stage"}
                 {:key :time-taken-ms :header "Time (ms)"}
                 {:key :duration :header "Duration"}
                 {:key :percent-of-total :header "% of total"}]]
    (str (github-table columns rows)
         "\n\n"
         (totals->string total-ms (:benchmark-total-time-ms summary)))))

(defn load-summary
  [benchmark-type log-file-path]
  (-> (parse-log benchmark-type log-file-path)
      (assoc :benchmark-type benchmark-type)))

(def supported-formats #{:table :slack :github})

(defn normalize-format
  [format]
  (let [fmt (cond
              (keyword? format) format
              (string? format) (keyword (str/lower-case format))
              :else format)
        fmt (or fmt :table)
        normalized (if (contains? supported-formats fmt) fmt :table)]
    normalized))

(defn render-summary
  [summary {:keys [format]}]
  (let [fmt (normalize-format format)]
    (case fmt
      :table (summary->table summary)
      :slack (summary->slack summary)
      :github (summary->github-markdown summary))))

(defn summarize-log
  [args]
  (let [{:keys [args opts]} (cli/parse-args args {:coerce {:format keyword}})
        [benchmark-type log-file-path & extra] args
        format (:format opts)]
    (when (seq extra)
      (throw (ex-info "Too many positional arguments supplied."
                      {:arguments args})))
    (when-not benchmark-type
      (throw (ex-info "Benchmark type is required."
                      {:arguments args})))
    (when-not log-file-path
      (throw (ex-info "Log file path is required."
                      {:arguments args})))
    (let [summary (load-summary benchmark-type log-file-path)]
      (case (:benchmark-type summary)
        "auctionmark" nil
        "clickbench" nil
        "tpch" (when (empty? (:query-stages summary))
                 (throw (ex-info "No query stages found in log file"
                                 {:benchmark-type "tpch"
                                  :log-file log-file-path})))
        "yakbench" (when (or (nil? (:profiles summary))
                             (empty? (:profiles summary)))
                     (throw (ex-info "No profile data found in log file"
                                     {:benchmark-type "yakbench"
                                      :log-file log-file-path})))
        "readings" (when (empty? (:query-stages summary))
                     (throw (ex-info "No query stages found in log file"
                                     {:benchmark-type "readings"
                                      :log-file log-file-path}))))
      (render-summary summary {:format format}))))

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
         scale-factor 1
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

(defn plot-timeseries-vega
  "Plot a timeseries line chart and save to SVG using Vega-Lite.

  Requires vega-cli to be installed:
    npm install -g vega vega-lite vega-cli

  data: sequence of maps with :timestamp and :value keys
        e.g., [{:timestamp \"2025-01-15T10:30:00Z\" :value 1234.5}
               {:timestamp \"2025-01-16T11:00:00Z\" :value 1150.2}]

  opts: {:output-path \"chart.svg\"       ; where to save
         :title \"Benchmark Performance\"  ; chart title
         :x-label \"Date\"                 ; x-axis label (default: \"Time\")
         :y-label \"Duration (ms)\"        ; y-axis label (default: \"Value\")
         :width 800                       ; image width (default: 800)
         :height 600}                     ; image height (default: 600)"
  [data {:keys [output-path title x-label y-label width height]
         :or {x-label "Time"
              y-label "Value"
              width 800
              height 600}}]
  (let [;; Transform data for Vega-Lite (timestamp as string, value as number)
        ;; Ensure values are coerced to doubles
        vega-data (mapv (fn [{:keys [timestamp value]}]
                          {:timestamp timestamp :value (double value)})
                        data)

        ;; Calculate y-axis range (min - 2, max + 2 for padding)
        values (map :value vega-data)
        y-min (- (apply min values) 2.0)
        y-max (+ (apply max values) 2.0)

        ;; Create Vega-Lite spec
        vega-spec {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
                   :title title
                   :width width
                   :height height
                   :data {:values vega-data}
                   :mark {:type "line" :point true}
                   :encoding {:x {:field "timestamp"
                                  :type "temporal"
                                  :title x-label
                                  :axis {:labelAngle -45}}
                              :y {:field "value"
                                  :type "quantitative"
                                  :title y-label
                                  :scale {:domain [y-min y-max]
                                          :reverse false}}}}

        ;; Write spec to temporary file
        temp-spec-file (str (java.io.File/createTempFile "vega-spec" ".json"))
        _ (spit temp-spec-file (json/generate-string vega-spec))]

    ;; Convert to SVG using vl2svg
    (proc/shell "vl2svg" temp-spec-file output-path)

    ;; Clean up temp file
    (.delete (java.io.File. temp-spec-file))))

(defn plot-timeseries
  "Plot a timeseries line chart and save to SVG using Vega-Lite.

  Requires vega-cli to be installed:
    npm install -g vega vega-lite vega-cli

  data: sequence of maps with :timestamp and :value keys
        e.g., [{:timestamp \"2025-01-15T10:30:00Z\" :value 1234.5}
               {:timestamp \"2025-01-16T11:00:00Z\" :value 1150.2}]

  opts: {:output-path \"chart.svg\"       ; where to save
         :title \"Benchmark Performance\"  ; chart title
         :x-label \"Date\"                 ; x-axis label (default: \"Time\")
         :y-label \"Duration (ms)\"        ; y-axis label (default: \"Value\")
         :width 800                       ; image width (default: 800)
         :height 600}                     ; image height (default: 600)"
  [data {:keys [output-path title x-label y-label width height]
         :or {x-label "Time"
              y-label "Value"
              width 800
              height 600}}]
  (let [;; Transform data for Vega-Lite (timestamp as string, value as number)
        ;; Ensure values are coerced to doubles
        vega-data (mapv (fn [{:keys [timestamp value]}]
                          {:timestamp timestamp :value (double value)})
                        data)

        ;; Calculate y-axis range (min - 2, max + 2 for padding)
        values (map :value vega-data)
        y-min (- (apply min values) 2.0)
        y-max (+ (apply max values) 2.0)

        ;; Create Vega-Lite spec
        vega-spec {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
                   :title title
                   :width width
                   :height height
                   :data {:values vega-data}
                   :mark {:type "line" :point true}
                   :encoding {:x {:field "timestamp"
                                  :type "temporal"
                                  :title x-label
                                  :axis {:labelAngle -45}}
                              :y {:field "value"
                                  :type "quantitative"
                                  :title y-label
                                  :scale {:domain [y-min y-max]
                                          :reverse false}}}}

        ;; Write spec to temporary file
        temp-spec-file (str (java.io.File/createTempFile "vega-spec" ".json"))
        _ (spit temp-spec-file (json/generate-string vega-spec))]

    ;; Convert to SVG using vl2svg
    (proc/shell "vl2svg" temp-spec-file output-path)

    ;; Clean up temp file
    (.delete (java.io.File. temp-spec-file))))

(def ^:private benchmark-configs
  {"tpch"       {:benchmark-name "TPC-H (OLAP)"
                 :title "TPC-H Benchmark Performance"
                 :default-scale-factor 1.0}
   "yakbench"   {:benchmark-name "Yakbench"
                 :title "Yakbench Benchmark Performance"
                 :default-scale-factor 1.0}
   "auctionmark" {:benchmark-name "Auction Mark OLTP"
                  :title "AuctionMark Benchmark Performance"
                  :default-scale-factor 0.1}
   "readings"   {:benchmark-name "Readings benchmarks"
                 :title "Readings Benchmark Performance"
                 :default-scale-factor nil} ;; readings doesn't use scale-factor
   "clickbench" {:benchmark-name "Clickbench Hits"
                 :title "Clickbench Benchmark Performance"
                 :default-scale-factor nil}}) ;; clickbench uses size, not scale-factor

(defn plot-benchmark-timeseries
  "Plot a benchmark timeseries chart from Azure Log Analytics.

  benchmark-type: benchmark type (e.g., \"tpch\", \"yakbench\", \"auctionmark\", \"readings\")
  opts: {:scale-factor 1.0}  ; scale factor to filter by (uses default if not provided)

  Fetches benchmark data and plots it to an SVG file.
  Uses default parameters suitable for the specified benchmark type."
  ([benchmark-type] (plot-benchmark-timeseries benchmark-type {}))
  ([benchmark-type {:keys [scale-factor]}]
   (let [config (get benchmark-configs benchmark-type)
         _ (when-not config
             (throw (ex-info (format "Unsupported benchmark type for timeseries plotting: %s" benchmark-type)
                             {:benchmark-type benchmark-type
                              :supported (keys benchmark-configs)})))
         {:keys [benchmark-name title default-scale-factor]} config
         sf (or scale-factor default-scale-factor)
         fetch-opts (cond-> {:benchmark benchmark-name}
                      sf (assoc :scale-factor sf))
         data-ms (fetch-azure-benchmark-timeseries fetch-opts)
         ;; Convert milliseconds to minutes
         data (mapv (fn [{:keys [timestamp value]}]
                      (let [num-value (if (string? value)
                                        (Double/parseDouble value)
                                        (double value))]
                        {:timestamp timestamp
                         :value (/ num-value 60000.0)}))
                    data-ms)
         output-path (str benchmark-type "-benchmark-timeseries.svg")
         chart-title (if sf
                       (str title " (SF " sf ")")
                       title)]
     (plot-timeseries data {:output-path output-path
                            :title chart-title
                            :x-label "Time"
                            :y-label "Duration (minutes)"})
     (println "Generated chart:" output-path)
     output-path)))

(defn help []
  (println "Usage: bb <command> [args...]")
  (println)
  (println "Commands:")
  (println "  summarize-log [--format table|slack|github] <benchmark-type> <log-file>")
  (println "      Print a benchmark summary. Default format is 'table'.")
  (println "  plot-benchmark-timeseries [--scale-factor SF] <benchmark-type>")
  (println "      Plot a benchmark timeseries chart from Azure Log Analytics.")
  (println "      Supported benchmark types: tpch, yakbench, auctionmark, readings")
  (println "      --scale-factor: Filter by scale factor (default: 1.0 for tpch/yakbench, 0.1 for auctionmark)")
  (println "  help")
  (println "      Show this help message"))

(defn -main [& args]
  (if (empty? args)
    (help)
    (let [[command & rest-args] args]
      (try
        (case command
          "summarize-log"
          (let [output (summarize-log rest-args)]
            (print output)
            (flush))

          "plot-benchmark-timeseries"
          (let [{:keys [args opts]} (cli/parse-args rest-args {:coerce {:scale-factor :double}})
                [benchmark-type & extra] args
                scale-factor (:scale-factor opts)]
            (when (seq extra)
              (throw (ex-info "Too many positional arguments supplied."
                              {:arguments rest-args})))
            (when-not benchmark-type
              (throw (ex-info "Benchmark type is required."
                              {:arguments rest-args})))
            (plot-benchmark-timeseries benchmark-type {:scale-factor scale-factor}))

          (do
            (println (str "Unknown command: " command))
            (help)))
        (catch Exception e
          (println "Error:" (.getMessage e))
          (when-let [data (ex-data e)]
            (println "Details:" data))
          (println "\nStack trace:")
          (.printStackTrace e)
          (System/exit 1))))))

(when (= *file* (System/getProperty "babashka.file"))
  (apply -main *command-line-args*))
