(ns xtdb.bench.cloud.scripts.log-parsing
  "Benchmark log parsing for various benchmark types."
  (:require [cheshire.core :as json]
            [clojure.string :as str]))

(defn parse-benchmark-summary
  "Extract benchmark summary from log lines."
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

(defmulti parse-log
  "Parse a benchmark log file. Dispatches on benchmark type."
  (fn [benchmark-type _log-file-path] benchmark-type))

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

(defmethod parse-log "fusion" [_benchmark-type log-file-path]
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
        load-stages (filterv #(contains? #{"init-tables" "ingest-interleaved" "sync" "compact"} (name (:stage %))) stages)
        {:keys [benchmark-total-time-ms benchmark-summary]} (parse-benchmark-summary lines)]
    {:all-stages stages
     :load-stages load-stages
     :benchmark-total-time-ms benchmark-total-time-ms
     :benchmark-summary benchmark-summary}))

(defmethod parse-log "tsbs-iot" [_benchmark-type log-file-path]
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
     :query-stages (filterv #(= "query-stats" (name (:stage %))) stages)
     :ingest-stages (filterv #(contains? #{"gen+submit-docs" "submit-docs" "sync" "finish-block" "compact" "ingest"} (name (:stage %))) stages)
     :benchmark-total-time-ms benchmark-total-time-ms
     :benchmark-summary benchmark-summary}))

(defmethod parse-log "ingest-tx-overhead" [_benchmark-type log-file-path]
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
        batch-stages (filterv (fn [stage]
                                (let [stage-name (name (:stage stage))]
                                  (str/starts-with? stage-name "ingest-batch-")))
                              stages)
        {:keys [benchmark-total-time-ms benchmark-summary]} (parse-benchmark-summary lines)]
    {:all-stages stages
     :batch-stages batch-stages
     :benchmark-total-time-ms benchmark-total-time-ms
     :benchmark-summary benchmark-summary}))

(defmethod parse-log "patch" [_benchmark-type log-file-path]
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
        patch-stages (filterv (fn [stage]
                                (let [stage-name (name (:stage stage))]
                                  (or (str/starts-with? stage-name "patch-")
                                      (contains? #{"patch-existing-docs" "patch-multiple-docs" "patch-non-existing-docs"} stage-name))))
                              stages)
        {:keys [benchmark-total-time-ms benchmark-summary]} (parse-benchmark-summary lines)]
    {:all-stages stages
     :patch-stages patch-stages
     :ingest-stages (filterv #(contains? #{"submit-docs" "sync" "finish-block" "compact" "ingest"} (name (:stage %))) stages)
     :benchmark-total-time-ms benchmark-total-time-ms
     :benchmark-summary benchmark-summary}))

(defmethod parse-log "products" [_benchmark-type log-file-path]
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
     :ingest-stages (filterv #(contains? #{"download" "submit-docs" "sync" "finish-block" "compact" "ingest"} (name (:stage %))) stages)
     :benchmark-total-time-ms benchmark-total-time-ms
     :benchmark-summary benchmark-summary}))

(defmethod parse-log "ts-devices" [_benchmark-type log-file-path]
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
     :ingest-stages (filterv #(contains? #{"download-files" "submit-docs" "sync" "finish-block" "compact" "ingest"} (name (:stage %))) stages)
     :benchmark-total-time-ms benchmark-total-time-ms
     :benchmark-summary benchmark-summary}))

(defn load-summary
  "Load and parse a benchmark log file, returning summary with benchmark type."
  [benchmark-type log-file-path]
  (-> (parse-log benchmark-type log-file-path)
      (assoc :benchmark-type benchmark-type)))
