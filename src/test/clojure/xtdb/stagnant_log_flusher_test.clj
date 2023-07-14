(ns xtdb.stagnant-log-flusher-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.log :as log]
            [xtdb.node :as node]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]
            [xtdb.vector.reader :as ivr])
  (:import (java.io Closeable)
           (java.util.concurrent Semaphore)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector.ipc ArrowStreamReader)
           (xtdb.indexer Indexer)
           (xtdb.log Log)
           (xtdb.object_store ObjectStore)))

(defonce log-level :error)

(comment
  (def log-level :debug)
  (def log-level :info)
  (def log-level :error)
  )

(defmacro spin-until
  [ms expr]
  `(loop [ret# ~expr
          wait-until# (+ ~ms (System/currentTimeMillis))]
     (cond
       ret# ret#
       (< wait-until# (System/currentTimeMillis)) ret#
       :else (recur ~expr wait-until#))))

(defmacro spin-ensure [ms expr]
  `(loop [ret# ~expr
          wait-until# (+ ~ms (System/currentTimeMillis))]
     (cond
       (not ret#) ret#
       (< wait-until# (System/currentTimeMillis)) ret#
       :else (recur ~expr wait-until#))))

(def ^:dynamic *spin-ms*
  "Change if tolerances change and tests need more time (such as slower CI machines), used for `spin`."
  500)

(defmacro spin [expr] `(spin-until *spin-ms* ~expr))

(defn each-fixture [f]
  (let [log-nses '[xtdb.stagnant-log-flusher
                   xtdb.indexer]
        old-levels (mapv tu/get-log-level! log-nses)]
    (try
      (doseq [ns log-nses] (tu/set-log-level! ns log-level))
      (binding [*spin-ms* *spin-ms*]
        (f))
      (finally
        (doseq [[ns level] (mapv vector log-nses old-levels)]
          (tu/set-log-level! ns level))))))

(t/use-fixtures :each each-fixture)

(defn log-seq [^Log log ^BufferAllocator allocator]
  (letfn [(clj-record [record]
            (with-open [tx-ops-ch (util/->seekable-byte-channel (:record record))
                        sr (ArrowStreamReader. tx-ops-ch allocator)
                        tx-root (.getVectorSchemaRoot sr)]
              (.loadNextBatch sr)
              {:tx (:tx record)
               :record (first (ivr/rel->rows (ivr/<-root tx-root)))}))]
    ((fn ! [offset]
       (lazy-seq
         (when-some [records (seq (.readRecords log (long offset) 100))]
           (concat
             (map clj-record records)
             (! (:tx-id (:tx (last records))))))))
     -1)))

(defn node-log [node]
  (let [log (tu/component node ::log/memory-log)
        alloc (tu/component node :xtdb/allocator)]
    (log-seq log alloc)))

(defn log-indexed? [node]
  (let [^Indexer indexer (tu/component node :xtdb/indexer)]
    (= (:tx (last (node-log node))) (.latestCompletedTx indexer))))

(t/deftest if-log-does-not-get-a-new-msg-in-xx-time-we-submit-a-flush-test
  (with-open [node (node/start-node {:xtdb.stagnant-log-flusher/flusher {:duration #time/duration "PT0.001S"}})]
    (t/testing "sent after a first message"
      (xt/submit-tx node [[:put :foo {:xt/id 42}]])
      (t/is (spin (= 2 (count (node-log node)))))
      (let [[msg1 msg2] (node-log node)]
        (let [flush-tx-id (-> msg2 :record :tx-ops first)]
          (t/is flush-tx-id)
          (t/is (= (:tx-id (:tx msg1)) flush-tx-id)))))

    (t/testing "sent after a second message"
      (xt/submit-tx node [[:put :foo {:xt/id 42}]])
      (t/is (spin (= 4 (count (node-log node)))))
      (println (count (node-log node)))))

  (t/testing "test :duration actually does something"
    (with-open [node (node/start-node {:xtdb.stagnant-log-flusher/flusher {:duration #time/duration "PT1H"}})]
      (xt/submit-tx node [[:put :foo {:xt/id 42}]])
      (t/is (spin (= 1 (count (node-log node)))))
      (t/is (spin-ensure 10 (= 1 (count (node-log node)))))))

  (t/testing "logs receiving messages will stop flushes"
    (let [control (Semaphore. 0)
          control-close (reify Closeable (close [_] (.release control (- Integer/MAX_VALUE 1000))))
          on-heartbeat (fn [_] (.acquire control))
          heartbeat (fn [] (.release control))]
      (with-open [node (node/start-node {:xtdb.stagnant-log-flusher/flusher {:duration #time/duration "PT0.001S" :on-heartbeat on-heartbeat}})
                  _ control-close]
        (let [send-msg (fn [] (xt/submit-tx node [[:put :foo {:xt/id 42}]]))
              check-sync (fn [] (spin (log-indexed? node)))
              check-count (fn [n] (spin (= n (count (node-log node)))))
              check-count-remains (fn [n] (spin-ensure 10 (= n (count (node-log node)))))]
          (t/testing "the first heartbeat does not flush"
            (send-msg)
            (t/is (check-count 1))
            (t/is (check-sync))
            (heartbeat)
            (t/is (check-count-remains 1)))

          (t/testing "the second heartbeat will now flush (once)"
            (check-sync)
            (heartbeat)
            (t/is (check-count 2))
            (t/is (check-count-remains 2)))

          ;; note, right now if another node submits a flush message - that will trigger a new flush, which will herd/cascade.
          ;; however the conditional flush in the indexer **should** stop this being a problem
          (t/testing "the next heartbeat(s) will not flush, as we have just flushed that tx-id"
            (dotimes [_ 100]
              (check-sync)
              (heartbeat))
            (.drainPermits control)
            (t/is (check-count 2))
            (t/is (check-count-remains 2)))

          (t/testing "sending a second message, will not flush - seen a new tx-id"
            (send-msg)
            (t/is (check-sync))
            (heartbeat)
            (t/is (check-count 3))
            (t/is (check-count-remains 3)))

          (t/testing "and a couple more messages, will not flush - seen a new tx-id"
            (send-msg)
            (send-msg)
            (t/is (check-count 5))
            (t/is (check-sync))
            (heartbeat)
            (t/is (check-count-remains 5)))

          (t/testing "no more messages, flush again"
            (heartbeat)
            (t/is (check-count 6))
            (t/is (check-count-remains 6))))))))

(defn chunk-path-seq [node]
  (let [obj (tu/component node :xtdb.object-store/memory-object-store)]
    (filter #(re-matches #"chunk-\p{XDigit}+/temporal\.arrow" %) (.listObjects ^ObjectStore obj))))

(t/deftest indexer-flushes-block-and-chunk-if-flush-op-test
  (with-open [node (node/start-node {:xtdb.stagnant-log-flusher/flusher {:duration #time/duration "PT0.001S"}})]
    (t/is (spin-ensure 10 (= 0 (count (chunk-path-seq node)))))
    (xt/submit-tx node [[:put :foo {:xt/id 42}]])
    (t/is (spin (= 1 (count (chunk-path-seq node))))))

  (with-open [node (node/start-node {:xtdb.stagnant-log-flusher/flusher {:duration #time/duration "PT1H"}})]
    (xt/submit-tx node [[:put :foo {:xt/id 42}]])
    (t/is (spin-ensure 10 (= 0 (count (chunk-path-seq node)))))))
