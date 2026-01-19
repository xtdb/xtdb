(ns xtdb.bench.patch
  (:require [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.bench :as b])
  (:import (java.time Duration)))

(defn patch-existing-docs-stage [{:keys [doc-count patch-count]}]
  {:t :do
   :stage :patch-existing-docs
   :tasks [{:t :call
            :f (fn [{:keys [node] :as worker}]
                 (log/info "Patching" patch-count "existing documents...")
                 (let [times (doall
                              (for [i (range patch-count)]
                                (let [id (rand-int doc-count)
                                      start-time (System/nanoTime)]
                                  (xt/execute-tx node [(format "PATCH INTO foo RECORDS {_id: %s, c:'%s'}" id i)])
                                  (/ (- (System/nanoTime) start-time) 1000000.0))))]
                   (b/log-report worker {:stage "patch-existing-average-ms"
                                         :average-time-ms (/ (reduce + times) (count times))})))}]})

(defn patch-multiple-existing-docs-stage [{:keys [doc-count patch-count]}]
  {:t :do
   :stage :patch-multiple-docs
   :tasks [{:t :call
            :f (fn [{:keys [node] :as worker}]
                 (log/info "Patching" patch-count "multiple documents...")
                 (let [times (doall
                              (for [i (range patch-count)]
                                (let [id-1 (rand-int doc-count)
                                      id-2 (rand-int doc-count)
                                      id-3 (rand-int doc-count)
                                      start-time (System/nanoTime)]
                                  (xt/execute-tx node [(format "PATCH INTO foo RECORDS {_id: %s, c:'%s'}, {_id: %s, c:'%s'}, {_id: %s, c:'%s'}"
                                                               id-1 i
                                                               id-2 i
                                                               id-3 i)])
                                  (/ (- (System/nanoTime) start-time) 1000000.0))))]
                   (b/log-report worker {:stage "patch-multiple-average-ms"
                                         :average-time-ms (/ (reduce + times) (count times))})))}]})

(defn patch-non-existing-docs-stage [{:keys [patch-count]}]
  {:t :do
   :stage :patch-non-existing-docs
   :tasks [{:t :call
            :f (fn [{:keys [node] :as worker}]
                 (log/info "Patching" patch-count "non-existing documents...")
                 (let [times (doall
                              (for [i (range patch-count)]
                                (let [id (str "new-doc-" i)
                                      start-time (System/nanoTime)]
                                  (xt/execute-tx node [(str "PATCH INTO foo RECORDS {_id: '" id "', c: 'c" i "'}")])
                                  (/ (- (System/nanoTime) start-time) 1000000.0))))]
                   (b/log-report worker {:stage "patch-non-existing-average-ms"
                                         :average-time-ms (/ (reduce + times) (count times))})))}]})

(defmethod b/cli-flags :patch [_]
  [["-d" "--doc-count DOC_COUNT" "Number of initial documents to load"
    :parse-fn parse-long
    :default 500000]

   ["-p" "--patch-count PATCH_COUNT" "Number of patches to perform in each stage"
    :parse-fn parse-long
    :default 10]

   ["-h" "--help"]])

(defmethod b/->benchmark :patch [_ {:keys [doc-count patch-count seed no-load?]
                                    :or {doc-count 500000 patch-count 10 seed 0} :as opts}]
  (log/info {:doc-count doc-count :patch-count patch-count :seed seed})

  {:title "PATCH Performance Benchmark"
   :benchmark-type :patch
   :parameters {:doc-count doc-count :patch-count patch-count}
   :seed seed
   :tasks [{:t :do
            :stage :ingest
            :tasks (concat (when-not no-load?
                             [{:t :call
                               :stage :submit-docs
                               :f (fn [{:keys [node] :as worker}]
                                    (log/info "Inserting" doc-count "documents...")
                                    (let [batch-size 500]
                                      (doseq [batch-start (range 0 doc-count batch-size)]
                                        (when (zero? (mod batch-start 10000))
                                          (log/info (format "Batch - %s / %s" (/ batch-start batch-size) (/ doc-count batch-size))))
                                        (xt/submit-tx node (mapv (fn [i]
                                                                   [:put-docs :foo {:xt/id (+ i batch-start)
                                                                                    :a (str "a" (+ i batch-start))
                                                                                    :data (b/random-str worker 100 500)}])
                                                                 (range 0 batch-size)))))
                                    (log/info "Inserted" doc-count "documents"))}])

                           [{:t :do
                             :stage :sync
                             :tasks [{:t :call :f (fn [{:keys [node]}] (b/sync-node node (Duration/ofMinutes 10)))}]}
                            {:t :do
                             :stage :finish-block
                             :tasks [{:t :call :f (fn [{:keys [node]}] (b/finish-block! node))}]}
                            {:t :do
                             :stage :compact
                             :tasks [{:t :call :f (fn [{:keys [node]}] (b/compact! node))}]}])}

           (patch-existing-docs-stage opts)
           (patch-multiple-existing-docs-stage opts)
           (patch-non-existing-docs-stage opts)]})
