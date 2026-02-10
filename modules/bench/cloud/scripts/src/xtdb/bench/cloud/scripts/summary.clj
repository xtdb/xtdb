(ns xtdb.bench.cloud.scripts.summary
  "Summary formatting for benchmark results (table, slack, github)."
  (:require [clojure.pprint :as pprint]
            [clojure.string :as str]
            [xtdb.bench.cloud.scripts.util :as util]))

;; Row transformation helpers

(defn tpch-stage->query-row
  [idx {:keys [stage time-taken-ms]}]
  (when-let [[_ hot-cold query-num name] (re-find #"^((?:hot|cold))-queries-q(\d+)-(.*)$" stage)]
    (let [friendly-name (util/title-case name)
          query-index (Long/parseLong query-num)
          duration-pt (util/format-duration :millis time-taken-ms)
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
   :mean (util/format-duration :nanos mean)
   :p50 (util/format-duration :nanos p50)
   :p90 (util/format-duration :nanos p90)
   :p99 (util/format-duration :nanos p99)
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
                        (str "Recent " (util/title-case (subs stage-name (count "query-recent-interval-"))))

                        (str/starts-with? stage-name "query-offset-")
                        (let [[_ offset-len period interval] (re-find #"^query-offset-(\d+)-(.*?)-interval-(.*)$" stage-name)]
                          (str "Offset " offset-len " " (util/title-case period) " " (util/title-case interval)))

                        :else (util/title-case stage-name))
        duration-pt (util/format-duration :millis time-taken-ms)]
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

(defn clickbench-stage->row
  [idx {:keys [stage time-taken-ms]}]
  {:stage-order idx
   :stage (util/title-case stage)
   :time-taken-ms time-taken-ms
   :duration (util/format-duration :millis time-taken-ms)})

(defn ingest-tx-overhead-stage->row
  [idx {:keys [stage time-taken-ms]}]
  (when-let [[_ batch-size] (re-find #"^ingest-batch-(\d+)$" (name stage))]
    {:stage-order idx
     :batch-size (Long/parseLong batch-size)
     :stage (str "Batch " batch-size)
     :time-taken-ms time-taken-ms
     :duration (util/format-duration :millis time-taken-ms)}))

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

(defn adjusted-total-ms
  [sum-ms benchmark-total-time-ms]
  (if (and benchmark-total-time-ms
           (pos? benchmark-total-time-ms)
           (> sum-ms benchmark-total-time-ms))
    benchmark-total-time-ms
    sum-ms))

(defn ingest-tx-overhead-summary->stage-rows
  [summary]
  (let [rows (->> (:batch-stages summary)
                  (map-indexed ingest-tx-overhead-stage->row)
                  (remove nil?)
                  (sort-by :batch-size >)
                  vec)
        sum-ms (reduce + (map :time-taken-ms rows))
        total-ms (adjusted-total-ms sum-ms (:benchmark-total-time-ms summary))
        rows-with-percent (mapv (fn [row]
                                  (let [ms (:time-taken-ms row)
                                        pct (if (pos? total-ms)
                                              (* 100.0 (/ ms total-ms))
                                              0.0)]
                                    (-> row
                                        (assoc :percent-of-total (format "%.2f%%" pct))
                                        (dissoc :stage-order :batch-size))))
                                rows)]
    {:rows rows-with-percent
     :total-ms total-ms}))

(defn products-summary->stage-rows
  [summary]
  (let [rows (->> (:ingest-stages summary)
                  (map-indexed clickbench-stage->row)
                  (sort-by :stage-order)
                  vec)
        sum-ms (reduce + (map :time-taken-ms rows))
        total-ms (adjusted-total-ms sum-ms (:benchmark-total-time-ms summary))
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

(defn patch-stage->row
  [idx {:keys [stage time-taken-ms]}]
  (let [stage-name (name stage)
        friendly-name (cond
                        (= stage-name "patch-existing-docs") "Patch Existing"
                        (= stage-name "patch-multiple-docs") "Patch Multiple"
                        (= stage-name "patch-non-existing-docs") "Patch Non-Existing"
                        :else (util/title-case stage-name))]
    {:stage-order idx
     :stage friendly-name
     :time-taken-ms time-taken-ms
     :duration (util/format-duration :millis time-taken-ms)}))

(defn patch-summary->stage-rows
  [summary]
  (let [rows (->> (:patch-stages summary)
                  (map-indexed patch-stage->row)
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

(defn fusion-stage->row
  [idx {:keys [stage time-taken-ms]}]
  {:stage-order idx
   :stage (name stage)
   :time-taken-ms time-taken-ms
   :duration (util/format-duration :millis time-taken-ms)})

(defn fusion-summary->stage-rows
  [summary]
  (let [rows (->> (:load-stages summary)
                  (map-indexed fusion-stage->row)
                  (sort-by :stage-order)
                  vec)
        total-ms (reduce + 0 (map :time-taken-ms rows))
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

(defn rows->string [columns rows]
  (-> (with-out-str
        (pprint/print-table columns rows))
      str/trim))

;; summary->table multimethod

(defmulti summary->table :benchmark-type)

(defmethod summary->table "tpch" [summary]
  (let [{:keys [rows total-ms]} (tpch-summary->query-rows summary)]
    (str (util/totals->string total-ms (:benchmark-total-time-ms summary))
         "\n\n"
         (rows->string [:temp :q :query-name :time-taken-ms :duration :percent-of-total] rows))))

(defmethod summary->table "yakbench" [summary]
  (let [{:keys [rows total-ms]} (yakbench-summary->query-rows summary)]
    (str (util/totals->string total-ms (:benchmark-total-time-ms summary))
         "\n\n"
         (rows->string [:query :n :p50 :p90 :p99 :mean :percent-of-total] rows))))

(defmethod summary->table "readings" [summary]
  (let [{:keys [rows total-ms]} (readings-summary->query-rows summary)]
    (str (util/totals->string total-ms (:benchmark-total-time-ms summary))
         "\n\n"
         (rows->string [:query :time-taken-ms :duration :percent-of-total] rows))))

(defmethod summary->table "auctionmark" [summary]
  (let [{:keys [benchmark-total-time-ms]} summary]
    (format "Total benchmark time: %s (%s)"
            (or (util/format-duration :millis benchmark-total-time-ms) "N/A")
            (if benchmark-total-time-ms
              (java.time.Duration/ofMillis benchmark-total-time-ms)
              "N/A"))))

(defmethod summary->table "clickbench" [summary]
  (let [{:keys [rows total-ms]} (clickbench-summary->stage-rows summary)]
    (str (util/totals->string total-ms (:benchmark-total-time-ms summary) "ingest")
         "\n\n"
         (rows->string [:stage :time-taken-ms :duration :percent-of-total] rows))))

(defmethod summary->table "ingest-tx-overhead" [summary]
  (let [{:keys [rows total-ms]} (ingest-tx-overhead-summary->stage-rows summary)]
    (str (util/totals->string total-ms (:benchmark-total-time-ms summary) "ingest")
         "\n\n"
         (rows->string [:stage :time-taken-ms :duration :percent-of-total] rows))))

(defmethod summary->table "patch" [summary]
  (let [{:keys [rows total-ms]} (patch-summary->stage-rows summary)]
    (str (util/totals->string total-ms (:benchmark-total-time-ms summary) "patch")
         "\n\n"
         (rows->string [:stage :time-taken-ms :duration :percent-of-total] rows))))

(defmethod summary->table "products" [summary]
  (let [{:keys [rows total-ms]} (products-summary->stage-rows summary)]
    (str (util/totals->string total-ms (:benchmark-total-time-ms summary) "ingest")
         "\n\n"
         (rows->string [:stage :time-taken-ms :duration :percent-of-total] rows))))

(defmethod summary->table "ts-devices" [summary]
  (let [{:keys [rows total-ms]} (clickbench-summary->stage-rows summary)]
    (str (util/totals->string total-ms (:benchmark-total-time-ms summary) "ingest")
         "\n\n"
         (rows->string [:stage :time-taken-ms :duration :percent-of-total] rows))))

(defmethod summary->table "fusion" [summary]
  (let [{:keys [rows total-ms]} (fusion-summary->stage-rows summary)]
    (str (util/totals->string total-ms (:benchmark-total-time-ms summary))
         "\n\n"
         (rows->string [:stage :time-taken-ms :duration :percent-of-total] rows))))

;; summary->slack multimethod

(defmulti summary->slack :benchmark-type)

(defmethod summary->slack "tpch" [summary]
  (let [{:keys [rows total-ms]} (tpch-summary->query-rows summary)]
    (str
     (util/totals->string total-ms (:benchmark-total-time-ms summary))
     "\n\n"
     (util/wrap-slack-code
      (rows->string [:query :duration] rows)))))

(defmethod summary->slack "yakbench" [summary]
  (let [{:keys [rows total-ms]} (yakbench-summary->query-rows summary)
        mid (Math/ceil (/ (count rows) 2))
        [first-half second-half] (split-at mid rows)]
    (if (seq second-half)
      (str
       (util/totals->string total-ms (:benchmark-total-time-ms summary))
       "\n\n"
       (util/wrap-slack-code
        (rows->string [:query :p50 :p99 :mean] first-half))
       "\n---SLACK-SPLIT---\n"
       (util/wrap-slack-code
        (rows->string [:query :p50 :p99 :mean] second-half)))
      (str
       (util/totals->string total-ms (:benchmark-total-time-ms summary))
       "\n\n"
       (util/wrap-slack-code
        (rows->string [:query :p50 :p99 :mean] rows))))))

(defmethod summary->slack "readings" [summary]
  (let [{:keys [rows total-ms]} (readings-summary->query-rows summary)]
    (str
     (util/totals->string total-ms (:benchmark-total-time-ms summary))
     "\n\n"
     (util/wrap-slack-code
      (rows->string [:query :duration] rows)))))

(defmethod summary->slack "auctionmark" [summary]
  (let [{:keys [benchmark-total-time-ms]} summary]
    (format "Total benchmark time: %s (%s)"
            (or (util/format-duration :millis benchmark-total-time-ms) "N/A")
            (if benchmark-total-time-ms
              (java.time.Duration/ofMillis benchmark-total-time-ms)
              "N/A"))))

(defmethod summary->slack "clickbench" [summary]
  (let [{:keys [rows total-ms]} (clickbench-summary->stage-rows summary)]
    (str
     (util/totals->string total-ms (:benchmark-total-time-ms summary) "ingest")
     "\n\n"
     (util/wrap-slack-code
      (rows->string [:stage :duration] rows)))))

(defmethod summary->slack "ingest-tx-overhead" [summary]
  (let [{:keys [rows total-ms]} (ingest-tx-overhead-summary->stage-rows summary)]
    (str
     (util/totals->string total-ms (:benchmark-total-time-ms summary) "ingest")
     "\n\n"
     (util/wrap-slack-code
      (rows->string [:stage :duration] rows)))))

(defmethod summary->slack "patch" [summary]
  (let [{:keys [rows total-ms]} (patch-summary->stage-rows summary)]
    (str
     (util/totals->string total-ms (:benchmark-total-time-ms summary) "patch")
     "\n\n"
     (util/wrap-slack-code
      (rows->string [:stage :duration] rows)))))

(defmethod summary->slack "products" [summary]
  (let [{:keys [rows total-ms]} (products-summary->stage-rows summary)]
    (str
     (util/totals->string total-ms (:benchmark-total-time-ms summary) "ingest")
     "\n\n"
     (util/wrap-slack-code
      (rows->string [:stage :duration] rows)))))

(defmethod summary->slack "ts-devices" [summary]
  (let [{:keys [rows total-ms]} (clickbench-summary->stage-rows summary)]
    (str
     (util/totals->string total-ms (:benchmark-total-time-ms summary) "ingest")
     "\n\n"
     (util/wrap-slack-code
      (rows->string [:stage :duration] rows)))))

(defmethod summary->slack "fusion" [summary]
  (let [{:keys [rows total-ms]} (fusion-summary->stage-rows summary)]
    (str
     (util/totals->string total-ms (:benchmark-total-time-ms summary))
     "\n\n"
     (util/wrap-slack-code
      (rows->string [:stage :duration] rows)))))

;; summary->github-markdown multimethod

(defmulti summary->github-markdown :benchmark-type)

(defmethod summary->github-markdown "tpch" [summary]
  (let [{:keys [rows total-ms]} (tpch-summary->query-rows summary)
        columns [{:key :temp :header "Temp"}
                 {:key :q :header "Query"}
                 {:key :query-name :header "Query Name"}
                 {:key :time-taken-ms :header "Time (ms)"}
                 {:key :duration :header "Duration"}
                 {:key :percent-of-total :header "% of total"}]]
    (str (util/github-table columns rows)
         "\n\n"
         (util/totals->string total-ms (:benchmark-total-time-ms summary)))))

(defmethod summary->github-markdown "yakbench" [summary]
  (let [{:keys [rows total-ms]} (yakbench-summary->query-rows summary)
        columns [{:key :query :header "Query"}
                 {:key :n :header "N"}
                 {:key :p50 :header "P50"}
                 {:key :p90 :header "P90"}
                 {:key :p99 :header "P99"}
                 {:key :mean :header "Mean"}
                 {:key :percent-of-total :header "% of total"}]]
    (str (util/github-table columns rows)
         "\n\n"
         (util/totals->string total-ms (:benchmark-total-time-ms summary)))))

(defmethod summary->github-markdown "readings" [summary]
  (let [{:keys [rows total-ms]} (readings-summary->query-rows summary)
        columns [{:key :query :header "Query"}
                 {:key :time-taken-ms :header "Time (ms)"}
                 {:key :duration :header "Duration"}
                 {:key :percent-of-total :header "% of total"}]]
    (str (util/github-table columns rows)
         "\n\n"
         (util/totals->string total-ms (:benchmark-total-time-ms summary)))))

(defmethod summary->github-markdown "auctionmark" [summary]
  (let [{:keys [benchmark-total-time-ms]} summary]
    (format "Total benchmark time: %s (%s)"
            (or (util/format-duration :millis benchmark-total-time-ms) "N/A")
            (if benchmark-total-time-ms
              (java.time.Duration/ofMillis benchmark-total-time-ms)
              "N/A"))))

(defmethod summary->github-markdown "clickbench" [summary]
  (let [{:keys [rows total-ms]} (clickbench-summary->stage-rows summary)
        columns [{:key :stage :header "Stage"}
                 {:key :time-taken-ms :header "Time (ms)"}
                 {:key :duration :header "Duration"}
                 {:key :percent-of-total :header "% of total"}]]
    (str (util/github-table columns rows)
         "\n\n"
         (util/totals->string total-ms (:benchmark-total-time-ms summary) "ingest"))))

(defmethod summary->github-markdown "ingest-tx-overhead" [summary]
  (let [{:keys [rows total-ms]} (ingest-tx-overhead-summary->stage-rows summary)
        columns [{:key :stage :header "Batch Size"}
                 {:key :time-taken-ms :header "Time (ms)"}
                 {:key :duration :header "Duration"}
                 {:key :percent-of-total :header "% of total"}]]
    (str (util/github-table columns rows)
         "\n\n"
         (util/totals->string total-ms (:benchmark-total-time-ms summary) "ingest"))))

(defmethod summary->github-markdown "patch" [summary]
  (let [{:keys [rows total-ms]} (patch-summary->stage-rows summary)
        columns [{:key :stage :header "Stage"}
                 {:key :time-taken-ms :header "Time (ms)"}
                 {:key :duration :header "Duration"}
                 {:key :percent-of-total :header "% of total"}]]
    (str (util/github-table columns rows)
         "\n\n"
         (util/totals->string total-ms (:benchmark-total-time-ms summary) "patch"))))

(defmethod summary->github-markdown "products" [summary]
  (let [{:keys [rows total-ms]} (products-summary->stage-rows summary)
        columns [{:key :stage :header "Stage"}
                 {:key :time-taken-ms :header "Time (ms)"}
                 {:key :duration :header "Duration"}
                 {:key :percent-of-total :header "% of total"}]]
    (str (util/github-table columns rows)
         "\n\n"
         (util/totals->string total-ms (:benchmark-total-time-ms summary) "ingest"))))

(defmethod summary->github-markdown "ts-devices" [summary]
  (let [{:keys [rows total-ms]} (clickbench-summary->stage-rows summary)
        columns [{:key :stage :header "Stage"}
                 {:key :time-taken-ms :header "Time (ms)"}
                 {:key :duration :header "Duration"}
                 {:key :percent-of-total :header "% of total"}]]
    (str (util/github-table columns rows)
         "\n\n"
         (util/totals->string total-ms (:benchmark-total-time-ms summary) "ingest"))))

(defmethod summary->github-markdown "fusion" [summary]
  (let [{:keys [rows total-ms]} (fusion-summary->stage-rows summary)
        columns [{:key :stage :header "Stage"}
                 {:key :time-taken-ms :header "Time (ms)"}
                 {:key :duration :header "Duration"}
                 {:key :percent-of-total :header "% of total"}]]
    (str (util/github-table columns rows)
         "\n\n"
         (util/totals->string total-ms (:benchmark-total-time-ms summary)))))

;; Render summary

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

(defn tsbs-ingest-duration-ms
  [ingest-stages]
  (reduce + 0 (map :time-taken-ms ingest-stages)))

(defn tsbs-summary->query-rows
  [{:keys [query-stages]}]
  (let [stats-by-type (:stats-by-type (first query-stages))
        rows (mapv (fn [[query-type stats]]
                     (let [{:keys [count p50-ms p90-ms p99-ms mean-ms total-ms]} stats]
                       {:query (name query-type)
                        :n count
                        :p50 (util/format-duration :millis p50-ms)
                        :p90 (util/format-duration :millis p90-ms)
                        :p99 (util/format-duration :millis p99-ms)
                        :mean (util/format-duration :millis mean-ms)
                        :total-ms total-ms}))
                   (sort-by key stats-by-type))
        total-ms (reduce + 0 (map :total-ms rows))
        rows-with-percent (mapv (fn [row]
                                  (let [ms (:total-ms row)
                                        pct (if (pos? total-ms)
                                              (* 100.0 (/ ms total-ms))
                                              0.0)]
                                    (-> row
                                        (assoc :percent-of-total (format "%.2f%%" pct))
                                        (dissoc :total-ms))))
                                rows)]
    {:rows rows-with-percent
     :total-ms total-ms}))

(defmethod summary->table "tsbs-iot" [summary]
  (let [{:keys [benchmark-total-time-ms ingest-stages]} summary
        ingest-time-ms (tsbs-ingest-duration-ms ingest-stages)
        {:keys [rows total-ms]} (tsbs-summary->query-rows summary)]
    (str (format "Ingest: %s" (util/format-duration :millis ingest-time-ms))
         "\n"
         (util/totals->string total-ms benchmark-total-time-ms)
         "\n\n"
         (rows->string [:query :n :p50 :p90 :p99 :mean :percent-of-total] rows))))

(defmethod summary->slack "tsbs-iot" [summary]
  (let [{:keys [benchmark-total-time-ms ingest-stages]} summary
        ingest-time-ms (tsbs-ingest-duration-ms ingest-stages)
        {:keys [rows total-ms]} (tsbs-summary->query-rows summary)]
    (str
     (format "Ingest: %s" (util/format-duration :millis ingest-time-ms))
     "\n"
     (util/totals->string total-ms benchmark-total-time-ms)
     "\n\n"
     (util/wrap-slack-code
      (rows->string [:query :p50 :p99 :mean] rows)))))

(defmethod summary->github-markdown "tsbs-iot" [summary]
  (let [{:keys [benchmark-total-time-ms ingest-stages]} summary
        ingest-time-ms (tsbs-ingest-duration-ms ingest-stages)
        {:keys [rows total-ms]} (tsbs-summary->query-rows summary)
        columns [{:key :query :header "Query"}
                 {:key :n :header "N"}
                 {:key :p50 :header "P50"}
                 {:key :p90 :header "P90"}
                 {:key :p99 :header "P99"}
                 {:key :mean :header "Mean"}
                 {:key :percent-of-total :header "% of total"}]]
    (str
     "### TSBS IoT Benchmark\n\n"
     (format "- **Ingest Time**: %s\n" (util/format-duration :millis ingest-time-ms))
     "\n"
     (util/github-table columns rows)
     "\n\n"
     (util/totals->string total-ms benchmark-total-time-ms))))

(defn render-summary
  [summary {:keys [format]}]
  (let [fmt (normalize-format format)]
    (case fmt
      :table (summary->table summary)
      :slack (summary->slack summary)
      :github (summary->github-markdown summary))))
