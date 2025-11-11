(ns xtdb.bench.cloud.scripts.tasks
  (:require [babashka.cli :as cli]
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

(defn parse-benchmark-line
  [log-lines]
  (let [benchmark-line (first (filter #(str/includes? % "\"benchmark\":") log-lines))
        benchmark-summary (when benchmark-line
                            (try
                              (json/parse-string benchmark-line true)
                              (catch Exception _ nil)))
        benchmark-total-time-ms (when benchmark-summary
                                  (:time-taken-ms benchmark-summary))]
    {:benchmark-total-time-ms benchmark-total-time-ms
     :benchmark-summary benchmark-summary}))

(defn parse-tpch-log
  [log-file-path]
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
        {:keys [benchmark-total-time-ms benchmark-summary]} (parse-benchmark-line lines)]
    {:all-stages stages
     :query-stages query-stages
     :ingest-stages (filterv #(contains? #{"submit-docs" "sync" "finish-block" "compact" "ingest"} (:stage %)) stages)
     :benchmark-total-time-ms benchmark-total-time-ms
     :benchmark-summary benchmark-summary}))

(defn parse-yakbench-log
  [log-file-path]
  (let [content (slurp log-file-path)
        lines (str/split-lines content)
        profiles-line (first (filter #(str/includes? % "\"profiles\":") lines))
        profiles (when profiles-line
                   (try
                     (:profiles (json/parse-string profiles-line true))
                     (catch Exception _ nil)))
        {:keys [benchmark-total-time-ms benchmark-summary]} (parse-benchmark-line lines)]
    {:profiles profiles
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

(defn totals->string [query-ms benchmark-ms]
  (str (format "Total query time: %s (%s)"
               (format-duration :millis query-ms)
               (if query-ms (java.time.Duration/ofMillis query-ms) "N/A"))
       "\n"
       (format "Total benchmark time: %s (%s)"
               (format-duration :millis benchmark-ms)
               (if benchmark-ms (java.time.Duration/ofMillis benchmark-ms) "N/A"))))

(defn rows->string [columns rows]
  (-> (with-out-str
        (pprint/print-table columns rows))
      str/trim))

(defn rows-and-totals->summary-string [{:keys [columns rows query-ms benchmark-ms]}]
  (str (rows->string columns rows)
       "\n\n"
       (totals->string query-ms benchmark-ms)))

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

(defn load-summary
  [benchmark-type log-file-path]
  (-> (case benchmark-type
        "tpch" (parse-tpch-log log-file-path)
        "yakbench" (parse-yakbench-log log-file-path)
        (throw (ex-info (format "Unsupported benchmark type: %s" benchmark-type)
                        {:benchmark-type benchmark-type})))
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
        "tpch" (when (empty? (:query-stages summary))
                 (throw (ex-info "No query stages found in log file"
                                 {:benchmark-type "tpch"
                                  :log-file log-file-path})))
        "yakbench" (when (or (nil? (:profiles summary))
                             (empty? (:profiles summary)))
                     (throw (ex-info "No profile data found in log file"
                                     {:benchmark-type "yakbench"
                                      :log-file log-file-path}))))
      (render-summary summary {:format format}))))

(defn help []
  (println "Usage: bb <command> [args...]")
  (println)
  (println "Commands:")
  (println "  summarize-log [--format table|slack|github] <benchmark-type> <log-file>")
  (println "      Print a benchmark summary. Default format is 'table'.")
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

          (do
            (println (str "Unknown command: " command))
            (help)))
        (catch Exception e
          (println "Error:" (.getMessage e))
          (when-let [data (ex-data e)]
            (println "Details:" data))
          (System/exit 1))))))

(when (= *file* (System/getProperty "babashka.file"))
  (apply -main *command-line-args*))
