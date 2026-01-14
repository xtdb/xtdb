(ns xtdb.tsbs
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [cognitect.transit :as transit]
            [jsonista.core :as json]
            [xtdb.time :as time]
            [xtdb.util :as util]
            [xtdb.serde :as serde])
  (:import [java.time Duration Instant]))

;; see https://github.com/timescale/tsbs

(def ^java.io.File tsbs-dir
  (if-let [root (System/getProperty "xtdb.rootDir")]
    (io/file root "modules/datasets/lib/tsbs")
    (io/file "/opt/xtdb/tsbs")))

(defn make-gen []
  (let [bin-dir (io/file tsbs-dir "bin")
        generator-bin (io/file bin-dir "tsbs_generate_data")]
    (if (.exists generator-bin)
      (log/info "TSBS generators already built.")
      (do
        (log/info "Making TSBS generators...")
        (let [{:keys [exit out err]} (sh/sh "make" "generators"
                                            :dir tsbs-dir)]
          (if (zero? exit)
            (log/info "Made TSBS generators.")
            (throw (ex-info "Failed making TSBS generators"
                            {:exit exit, :out out, :err err}))))))))

(defn- parse-tags-row [tags-row]
  ;; tags,col1 type1,col2 type2,...
  (let [[_table-name & cols] (str/split tags-row #",")
        split-cols (map #(str/split % #" ") cols)]
    {:pk (keyword (ffirst split-cols))
     :tag-defs (->> split-cols
                    (into {}
                          (map (fn [[col-name col-type]]
                                 (let [k (keyword col-name)
                                       parse (case (keyword col-type)
                                               :string identity
                                               :float32 parse-double)]
                                   [col-name (fn [v]
                                               [k (parse v)])])))))}))

(defn- parse-table-defs [rows]
  ;; parse other table definitions - table,col1,col2,...
  ;; assumes followed by a blank row

  (loop [[row & more-rows] rows
         tables {}]
    (if (= row "")
      [tables more-rows]

      (let [[table-name & cols] (str/split row #",")]
        (recur more-rows (assoc tables table-name (mapv keyword cols)))))))

(defn- split-by-ts [rows]
  (->> rows
       (partition-by :timestamp)
       (map (fn [[{:keys [timestamp]} :as rows-for-ts]]
              {:tags (->> rows-for-ts
                          (into {} (map (juxt (comp :xt/id :doc) :tags))))
               :rows rows-for-ts
               :timestamp timestamp}))))

(defn- without-unchanged-tags [row-batches]
  (letfn [(without-unchanged-tags* [old-tags row-batches]
            (lazy-seq
             (when-let [[{:keys [tags] :as batch} & more-batches] row-batches]
               (cons (-> batch
                         (assoc :tags
                                (vec (for [[k tags] tags
                                           :when (not= tags (get old-tags k))]
                                       (assoc tags :xt/id k)))))
                     (without-unchanged-tags* tags more-batches)))))]
    (without-unchanged-tags* {} row-batches)))

(defn- split-into-txs [{:keys [metadata-table batch-size], :or {batch-size 1000}} row-batches]
  (->> row-batches
       (mapcat (fn [{:keys [tags rows timestamp]}]
                 (concat (for [tags (->> tags
                                         (partition-all batch-size))]
                           {:ops [(into [:put-docs metadata-table] tags)]
                            :timestamp timestamp})

                         ;; group by entity to simulate all the data for one entity coming in at once
                         (->> (group-by (comp :xt/id :doc) rows)
                              vals
                              (into [] (comp (partition-all batch-size)
                                             (map (fn [batch]
                                                    {:ops (for [[table rows] (->> (into [] cat batch)
                                                                                  (group-by :table))]
                                                            (into [:put-docs table] (map :doc) rows))
                                                     :timestamp timestamp}))))))))

       ((fn with-increasing-sys-time
          ([txs] (with-increasing-sys-time nil txs))
          ([^Instant last-sys-time txs]
           (lazy-seq
            (when-let [[{:keys [ops timestamp]} & more-txs] txs]
              (let [sys-time (if (or (nil? last-sys-time) (neg? (compare last-sys-time timestamp)))
                               timestamp
                               (.plusMillis last-sys-time 1))]
                (cons {:ops ops, :system-time sys-time}
                      (with-increasing-sys-time sys-time more-txs))))))))))

(defn with-generated-data [{:keys [use-case seed ^Duration log-interval, scale, timestamp-start, timestamp-end],
                            :or {seed 123, log-interval #xt/duration "PT1M", scale 1}
                            :as opts}
                           f]
  (let [cmd-args (cond-> ["./bin/tsbs_generate_data"
                          "--use-case" (name use-case)
                          "--format" "cratedb"
                          "--log-interval" (str (.toSeconds log-interval) "s")
                          "--seed" (str seed)
                          "--scale" (str scale)]
                   timestamp-start (conj "--timestamp-start" (str (time/->instant timestamp-start)))
                   timestamp-end (conj "--timestamp-end" (str (time/->instant timestamp-end))))
        _ (log/info "TSBS generator command:" (str/join " " cmd-args))
        proc (-> (Runtime/getRuntime)
                 (.exec ^"[Ljava.lang.String;" (into-array String cmd-args)
                        nil
                        tsbs-dir))]

    (with-open [in (io/reader (.getInputStream proc))]
      (let [[tags-row & rows] (line-seq in)
            {:keys [pk tag-defs]} (parse-tags-row tags-row)
            [tables rows] (parse-table-defs rows)]
        (f (->> (for [row rows
                      :let [[table tags-json ts-str & cells] (str/split row #"\t")
                            tags (->> (json/read-value tags-json)
                                      (into {} (map (fn [[k v]]
                                                      ((get tag-defs k) v)))))
                            timestamp (time/micros->instant (/ (parse-long ts-str) 1000))]]
                  {:table (keyword table)
                   :tags tags
                   :timestamp timestamp
                   :doc (-> (zipmap (get tables table) (map (some-fn parse-long parse-double) cells))
                            (assoc :xt/id (get tags pk), :xt/valid-from timestamp))})

                (split-by-ts)
                (without-unchanged-tags)
                (split-into-txs (-> opts
                                    (assoc :metadata-table (case use-case :iot :trucks, :devops :hosts))))))))))

;; this could go into a util, nothing specific here
(defn txs->file [path txs]
  (with-open [out (io/output-stream (.toFile (util/->path path)))]
    (let [wtr (transit/writer out :json {:handlers serde/transit-write-handler-map})]
      (doseq [txs txs]
        (when (Thread/interrupted) (throw (InterruptedException.)))

        (transit/write wtr txs)
        (.write out (int \newline))))))

(defn with-file-txs [path f]
  (with-open [in (io/input-stream (.toFile (util/->path path)))]
    (f (serde/transit-seq (transit/reader in :json {:handlers serde/transit-read-handler-map})))))

(comment
  (with-generated-data {:use-case :iot, :scale 400
                        :timestamp-start #inst "2020"
                        :timestamp-end #inst "2020-01-04"
                        :log-interval #xt/duration "PT5M"}
    (fn [txs]
      (into [] (take 4) txs)))

  ;; generates about 830MB of Transit. should probably write Arrow :D
  (with-generated-data {:use-case :iot, :scale 400
                        :timestamp-start #inst "2020-01-01"
                        :timestamp-end #inst "2020-02-01"
                        :log-interval #xt/duration "PT5M"}
    (partial txs->file #xt/path "/home/james/tmp/tsbs.transit.json")))
