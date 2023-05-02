(ns xtdb.bench2.xtdb2
  (:require [clojure.java.io :as io]
            [xtdb.api :as xt]
            [xtdb.api.protocols :as xtp]
            [xtdb.bench2 :as b]
            [xtdb.bench2.measurement :as bm]
            [xtdb.node :as node]
            [xtdb.test-util :as tu])
  (:import (xtdb InstantSource)
           (io.micrometer.core.instrument MeterRegistry Timer)
           (java.io Closeable File)
           (java.nio.file Path)
           (java.time Clock Duration)
           (java.util Random)
           (java.util.concurrent ConcurrentHashMap)
           (java.util.concurrent.atomic AtomicLong)))

(set! *warn-on-reflection* false)

(defn install-tx-fns [worker fns]
  (->> (for [[id fn-def] fns]
         [:put-fn id fn-def])
       (xt/submit-tx (:sut worker))))

(defn generate
  ([worker table f n]
   (let [doc-seq (remove nil? (repeatedly (long n) (partial f worker)))
         partition-count 512]
     (doseq [chunk (partition-all partition-count doc-seq)]
       (xt/submit-tx (:sut worker) (mapv (partial vector :put table) chunk))))))

(defn install-proxy-node-meters!
  [^MeterRegistry meter-reg]
  (let [timer #(-> (Timer/builder %)
                   (.minimumExpectedValue (Duration/ofNanos 1))
                   (.maximumExpectedValue (Duration/ofMinutes 2))
                   (.publishPercentiles (double-array bm/percentiles))
                   (.register meter-reg))]
    {:submit-tx-timer (timer "node.submit-tx")
     :query-timer (timer "node.query")}))

(defmacro reify-protocols-accepting-non-methods
  "On older versions of XT node methods may be missing."
  [& reify-def]
  `(reify ~@(loop [proto nil
                   forms reify-def
                   acc []]
              (if-some [form (first forms)]
                (cond
                  (symbol? form)
                  (if (class? (resolve form))
                    (recur nil (rest forms) (conj acc form))
                    (recur form (rest forms) (conj acc form)))

                  (nil? proto)
                  (recur nil (rest forms) (conj acc form))

                  (list? form)
                  (if-some [{:keys [arglists]} (get (:sigs @(resolve proto)) (keyword (name (first form))))]
                    ;; arity-match
                    (if (some #(= (count %) (count (second form))) arglists)
                      (recur proto (rest forms) (conj acc form))
                      (recur proto (rest forms) acc))
                    (recur proto (rest forms) acc)))
                acc))))

(defn bench-proxy ^Closeable [node ^MeterRegistry meter-reg]
  (let [last-submitted (atom nil)
        last-completed (atom nil)

        submit-counter (AtomicLong.)
        indexed-counter (AtomicLong.)

        _
        (doto meter-reg
          #_(.gauge "node.tx" ^Iterable [(Tag/of "event" "submitted")] submit-counter)
          #_(.gauge "node.tx" ^Iterable [(Tag/of "event" "indexed")] indexed-counter))


        fn-gauge (partial bm/new-fn-gauge meter-reg)

        ;; on-indexed
        ;; (fn [{:keys [submitted-tx, doc-ids, av-count, bytes-indexed] :as event}]
        ;;   (reset! last-completed submitted-tx)
        ;;   (.getAndIncrement indexed-counter)
        ;;   nil)

        compute-lag-nanos #_(partial compute-nanos node last-completed last-submitted)
        (fn []
          (let [{:keys [latest-completed-tx] :as res} (xt/status node)]
            (or
             (when-some [[fut ms] @last-submitted]
               (let [tx-id (:tx-id @fut)]
                 (when-some [{completed-tx-id :tx-id
                              completed-tx-time :system-time} latest-completed-tx]
                   (when (< completed-tx-id tx-id)
                     (* (long 1e6) (- ms (inst-ms completed-tx-time)))))))
             0)))

        compute-lag-abs
        (fn []
          (let [{:keys [latest-completed-tx] :as res} (xt/status node)]
            (or
             (when-some [[fut _] @last-submitted]
               (let [tx-id (:tx-id @fut)]
                 (when-some [{completed-tx-id :tx-id} latest-completed-tx ]
                   (- tx-id completed-tx-id))))
             0)))]


    (fn-gauge "node.tx.lag seconds" (comp #(/ % 1e9) compute-lag-nanos) {:unit "seconds"})
    (fn-gauge "node.tx.lag tx-id" compute-lag-abs )

    (reify
      xtp/PNode
      (open-datalog& [_ query args] (xtp/open-datalog& node query args))
      (open-sql& [_ query query-opts] (xtp/open-sql& node query query-opts))
      (latest-submitted-tx [_] (xtp/latest-submitted-tx node))

      xtp/PStatus
      (status [_]
        (let [{:keys [latest-completed-tx] :as res} (xt/status node)]
          (reset! last-completed latest-completed-tx)
          res))

      xtp/PSubmitNode
      (submit-tx& [_ tx-ops]
        (let [ret (xtp/submit-tx& node tx-ops)]
          (reset! last-submitted [ret (System/currentTimeMillis)])
          ;; (.incrementAndGet submit-counter)
          ret))

      (submit-tx& [_ tx-ops opts]
        (let [ret (xt/submit-tx& node tx-ops opts)]
          (reset! last-submitted [ret (System/currentTimeMillis)])
          ;; (.incrementAndGet submit-counter)
          ret))

      Closeable
      ;; o/w some stage closes the node for later stages
      (close [_] nil #_(.close node)))))

(defn wrap-task [task f]
  (let [{:keys [stage]} task]
    (bm/wrap-task
     task
     (if stage
       (fn instrumented-stage [worker]
         (if bm/*stage-reg*
           (with-open [node-proxy (bench-proxy (:sut worker) bm/*stage-reg*)]
             (f (assoc worker :sut node-proxy)))
           (f worker)))
       f))))

(defn run-benchmark
  [{:keys [node-opts
           benchmark-type
           benchmark-opts]}]
  (let [benchmark
        (case benchmark-type
          :auctionmark
          ((requiring-resolve 'xtdb.bench2.auctionmark/benchmark) benchmark-opts)
          #_#_:tpch
          ((requiring-resolve 'xtdb.bench2.tpch/benchmark) benchmark-opts)
          #_#_:trace (trace benchmark-opts))
        benchmark-fn (b/compile-benchmark
                      benchmark
                      ;; @(requiring-resolve `xtdb.bench.measurement/wrap-task)
                      (fn [task f] (wrap-task task f)))]
    (with-open [node (tu/->local-node node-opts)]
      (benchmark-fn node))))

(defn delete-directory-recursive
  "Recursively delete a directory."
  [^java.io.File file]
  (when (.isDirectory file)
    (run! delete-directory-recursive (.listFiles file)))
  (io/delete-file file))

(defn node-dir->config [^File node-dir]
  (let [^Path path (.toPath node-dir)]
    {:xtdb.log/local-directory-log {:root-path (.resolve path "log")}
     :xtdb.tx-producer/tx-producer {}
     :xtdb.buffer-pool/buffer-pool {:cache-path (.resolve path "buffers")}
     :xtdb.object-store/file-system-object-store {:root-path (.resolve path "objects")}}))

(defn- ->worker [node]
  (let [clock (Clock/systemUTC)
        domain-state (ConcurrentHashMap.)
        custom-state (ConcurrentHashMap.)
        root-random (Random. 112)
        reports (atom [])
        worker (b/->Worker node root-random domain-state custom-state clock reports)]
    worker))

(comment

  ;; ======
  ;; Running in process
  ;; ======

  (def run-duration "PT5S")
  (def run-duration "PT10S")
  (def run-duration "PT30S")
  (def run-duration "PT2M")
  (def run-duration "PT10M")

  (def node-dir (io/file "dev/dev-node"))
  (delete-directory-recursive node-dir)

  ;; comment out the different phases in auctionmark.clj
  ;; load phase is the only one required if testing single point queries
  ;; run-benchmark clears up the node it creates (but not the data),
  ;; hence needing to create a new one to test single point queries

  (def report-core2
    (run-benchmark
     {:node-opts {:node-dir (.toPath node-dir)
                  :instant-src InstantSource/SYSTEM}
      :benchmark-type :auctionmark
      :benchmark-opts {:duration run-duration
                       :scale-factor 0.1 :threads 8}}))

  ;;;;;;;;;;;;;
  ;; Viewing Reports
  ;;;;;;;;;;;;;

  (spit (io/file "core2-30s.edn") report-core2)
  (def report-core2 (clojure.edn/read-string (slurp (io/file "core2-30s.edn"))))

  (require 'xtdb.bench2.report)
  (xtdb.bench2.report/show-html-report
   (xtdb.bench2.report/vs
    "core2"
    report-core2))

  (def report-rocks (clojure.edn/read-string (slurp (io/file "../xtdb/core1-rocks-30s.edn"))))

  (xtdb.bench2.report/show-html-report
   (xtdb.bench2.report/vs
    "core2"
    report-core2
    "rocks"
    report-rocks))

  ;;;;;;;;;;;;;
  ;; testing single point queries
  ;;;;;;;;;;;;;

  (def node (node/start-node (node-dir->config node-dir)))

  (def get-item-query '{:find [i_id i_u_id i_initial_price i_current_price]
                        :in [i_id]
                        :where [[?i :_table :item]
                                [?i :i_id i_id]
                                [?i :i_status :open]
                                [?i :i_u_id i_u_id]
                                [?i :i_initial_price i_initial_price]
                                [?i :i_current_price i_current_price]]})
  ;; ra for the above
  (def ra-query
    '[:scan
      {:table item :for-valid-time [:at :now], :for-system-time nil}
      [{i_status (= i_status :open)}
       i_u_id
       i_current_price
       i_initial_price
       {i_id (= i_id ?i_id)}
       id]])

  (def open-ids (->> (xt/q node '{:find [i]
                                              :where [[i :_table :item]
                                                      [i :i_status :open]
                                                      #_[j :i_status ]]})
                     (map :i)))

  (def rand-seq (shuffle open-ids))

  (def q  (fn [open-id]
            (tu/query-ra ra-query {:node node
                                   :params {'?i_id open-id}})))
  ;; ra query
  (time
   (tu/with-allocator
     #(doseq [id (take (* 1000) rand-seq)]
        (q id))))

  ;; datalog query
  (time
   (doseq [id (take (* 1000 1) (shuffle rand-seq))]
     (xt/q node get-item-query id))))
