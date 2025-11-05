(ns xtdb.bench.cloud.scripts.tasks
  (:require [babashka.cli :as cli]
            [cheshire.core :as json]
            [clojure.pprint :as pprint]
            [clojure.string :as str]))

(defn format-duration
  "Format a duration value with appropriate unit (h/m/s/ms/µs/ns).

   Args:
     value: The numeric value to format
     unit: The input unit - :nanos, :micros, :millis, :seconds, :minutes, or :hours

   Returns a human-readable string with the most appropriate unit."
  [value unit]
  (when value
    (let [;; Convert everything to nanoseconds first
          nanos (case unit
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
          duration-pt (.toString (java.time.Duration/ofMillis time-taken-ms))]
      {:query-order idx
       :query-index query-index
       :temp (str/capitalize hot-cold)
       :q (str "Q" query-num)
       :query-name friendly-name
       :stage stage
       :time-taken-ms time-taken-ms
       :time-taken-duration duration-pt})))

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
   :mean (format-duration mean :nanos)
   :p50 (format-duration p50 :nanos)
   :p90 (format-duration p90 :nanos)
   :p99 (format-duration p99 :nanos)
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

(defmulti summary->table :benchmark-type)

(defmethod summary->table "tpch" [summary]
  (let [{:keys [rows total-ms]} (tpch-summary->query-rows summary)]
    (str (-> (with-out-str
               (pprint/print-table [:temp :q :query-name :time-taken-ms :time-taken-duration :percent-of-total] rows))
             str/trim)
         "\n\n"
         (format "Query total time: %s"
                 (format-duration total-ms :millis))
         "\n"
         (format "Benchmark total time: %s"
                 (format-duration (:benchmark-total-time-ms summary) :millis)))))

(defmethod summary->table "yakbench" [summary]
  (let [{:keys [rows total-ms]} (yakbench-summary->query-rows summary)]
    (str (-> (with-out-str
               (pprint/print-table [:query :n :p50 :p90 :p99 :mean :percent-of-total] rows))
             str/trim)
         "\n\n"
         (format "Query total time: %s"
                 (format-duration total-ms :millis))
         "\n"
         (format "Benchmark total time: %s"
                 (format-duration (:benchmark-total-time-ms summary) :millis)))))

;; Slack wraps code blocks at 76 characters, so we need to keep columns minimal
(defmulti summary->slack :benchmark-type)

(defmethod summary->slack "tpch" [summary]
  (let [{:keys [rows total-ms]} (tpch-summary->query-rows summary)
        table-rows (map (fn [{:keys [temp q query-name time-taken-ms]}]
                          {:query (str temp " " q " " query-name)
                           :duration (format-duration time-taken-ms :millis)})
                        rows)]
    (str "```\n"
         (-> (with-out-str
               (pprint/print-table [:query :duration] table-rows))
             str/trim)
         "\n\n"
         (format "Query total time: %s"
                 (format-duration total-ms :millis))
         "\n"
         (format "Benchmark total time: %s"
                 (format-duration (:benchmark-total-time-ms summary) :millis))
         "\n```")))

(defmethod summary->slack "yakbench" [summary]
  (let [{:keys [rows total-ms]} (yakbench-summary->query-rows summary)]
    (str "```\n"
         (-> (with-out-str
               (pprint/print-table [:query :p50 :p99 :mean] rows))
             str/trim)
         "\n\n"
         (format "Query total time: %s"
                 (format-duration total-ms :millis))
         "\n"
         (format "Benchmark total time: %s"
                 (format-duration (:benchmark-total-time-ms summary) :millis))
         "\n```")))

(defmulti summary->github-markdown :benchmark-type)

(defmethod summary->github-markdown "tpch" [summary]
  (let [{:keys [rows total-ms]} (tpch-summary->query-rows summary)]
    (str "| Temp | Query | Query Name | Time (ms) | Duration | % of total |\n"
         "|------|-------|------------|-----------|----------|------------|\n"
         (->> rows
              (map (fn [{:keys [temp q query-name time-taken-ms time-taken-duration percent-of-total]}]
                     (format "| %s | %s | %s | %d | %s | %s |"
                             temp q query-name time-taken-ms time-taken-duration percent-of-total)))
              (str/join "\n"))
         "\n\n"
         (format "Query total time: %s"
                 (format-duration total-ms :millis))
         "\n"
         (format "Benchmark total time: %s"
                 (format-duration (:benchmark-total-time-ms summary) :millis)))))

(defmethod summary->github-markdown "yakbench" [summary]
  (let [{:keys [rows total-ms]} (yakbench-summary->query-rows summary)]
    (str "| Query | N | P50 | P90 | P99 | Mean | % of total |\n"
         "|-------|---|-----|-----|-----|------|------------|\n"
         (->> rows
              (map (fn [{:keys [query mean p50 p90 p99 n percent-of-total]}]
                     (format "| %s | %d | %s | %s | %s | %s | %s |"
                             query n p50 p90 p99 mean percent-of-total)))
              (str/join "\n"))
         "\n\n"
         (format "Query total time: %s"
                 (format-duration total-ms :millis))
         "\n"
         (format "Benchmark total time: %s"
                 (format-duration (:benchmark-total-time-ms summary) :millis)))))

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
