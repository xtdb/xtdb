(ns xtdb.stagnant-log-flusher-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.log :as xt-log]
            [xtdb.log :as log]
            [xtdb.node :as node]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]
            [xtdb.vector.reader :as ivr])
  (:import (java.io Closeable)
           (java.nio ByteBuffer)
           (java.util.concurrent Semaphore)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector.ipc ArrowStreamReader)
           (xtdb.indexer Indexer)
           (xtdb.log Log)
           (xtdb.object_store ObjectStore)))

(set! *warn-on-reflection* false)

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
  (tu/with-log-levels
    {'xtdb.stagnant-log-flusher log-level
     'xtdb.indexer log-level
     'xtdb.ingester log-level}
    (binding [*spin-ms* *spin-ms*]
      (f))))

(t/use-fixtures :each each-fixture)

(defn log-seq [^Log log ^BufferAllocator allocator]
  (letfn [(clj-record [record]
            (condp = (Byte/toUnsignedInt (.get ^ByteBuffer (:record record) 0))
              xt-log/hb-flush-chunk
              {:header-byte xt-log/hb-flush-chunk
               :flush-tx-id (.getLong ^ByteBuffer (:record record) 1)
               :tx (:tx record)}

              xt-log/hb-user-arrow-transaction
              (with-open [tx-ops-ch (util/->seekable-byte-channel (:record record))
                          sr (ArrowStreamReader. tx-ops-ch allocator)
                          tx-root (.getVectorSchemaRoot sr)]
                (.loadNextBatch sr)
                {:header-byte xt-log/hb-user-arrow-transaction
                 :tx (:tx record)
                 :record (first (ivr/rel->rows (ivr/<-root tx-root)))})
              (throw (Exception. "Unrecognized record header"))))]
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

(defn start-node [flusher-duration & flusher-opts]
  (node/start-node {:xtdb/live-chunk {:rows-per-chunk 1024, :rows-per-block 64}
                    :xtdb.stagnant-log-flusher/flusher (apply hash-map :duration flusher-duration flusher-opts)}))

(t/deftest if-log-does-not-get-a-new-msg-in-xx-time-we-submit-a-flush-test
  (with-open [node (start-node #time/duration "PT0.001S")]
    (t/testing "sent after a first message"
      (xt/submit-tx node [[:put :foo {:xt/id 42}]])
      (t/is (spin (log-indexed? node)))
      (t/is (spin (= 2 (count (node-log node)))))
      (let [[_ msg2] (node-log node)]
        (let [flush-tx-id (:flush-tx-id msg2)]
          (t/is flush-tx-id)
          (t/is (= -1 flush-tx-id)))))

    (t/testing "sent after a second message"
      (xt/submit-tx node [[:put :foo {:xt/id 42}]])
      (t/is (spin (= 4 (count (node-log node)))))
      (let [[_ _ _ msg4] (node-log node)]
        (let [flush-tx-id (:flush-tx-id msg4)]
          (t/is flush-tx-id)
          (t/is (= (:tx-id (.latestCompletedChunkTx (tu/component node :xtdb/indexer))) flush-tx-id))))))


  (t/testing "test :duration actually does something"
    (with-open [node (start-node #time/duration "PT1H")]
      (xt/submit-tx node [[:put :foo {:xt/id 42}]])
      (t/is (spin (= 1 (count (node-log node)))))
      (t/is (spin-ensure 10 (= 1 (count (node-log node)))))))

  (t/testing "logs receiving messages will stop flushes"
    (let [control (Semaphore. 0)
          control-close (reify Closeable (close [_] (.release control (- Integer/MAX_VALUE 1000))))
          on-heartbeat (fn [_] (.acquire control))
          heartbeat (fn [] (.release control))]
      (with-open [node (start-node #time/duration "PT0.001S" :on-heartbeat on-heartbeat)
                  _ control-close]
        (let [send-msg (fn [] (xt/submit-tx node [[:put :foo {:xt/id 42}]]))
              check-sync (fn [] (spin (log-indexed? node)))
              check-count (fn [n] (spin (= n (count (node-log node)))))
              check-count-remains (fn [n] (spin-ensure 10 (= n (count (node-log node)))))]
          (t/testing "the first heartbeat does flush"
            (send-msg)
            (t/is (check-count 1))
            (t/is (check-sync))
            (heartbeat)
            (t/is (check-sync))
            (t/is (check-count 2))
            (t/is (check-count-remains 2)))

          (t/testing "the second heartbeat will not flush, as no new messages"
            (check-sync)
            (heartbeat)
            (t/is (check-count-remains 2)))

          ;; note, right now if another node submits a flush message - that will trigger a new flush msg, which will herd/cascade.
          ;; however the conditional flush in the indexer **should** stop this being a problem
          (t/testing "the next heartbeat(s) will not flush, as we have just flushed that tx-id"
            (dotimes [_ 100]
              (check-sync)
              (heartbeat))
            (.drainPermits control)
            (t/is (check-count 2))
            (t/is (check-count-remains 2)))

          (t/testing "sending a second message, will flush"
            (send-msg)
            (t/is (check-sync))
            (heartbeat)
            (t/is (check-count 4))
            (t/is (check-count-remains 4))))))))

(defn chunk-path-seq [node]
  (let [obj (tu/component node :xtdb.object-store/memory-object-store)]
    (filter #(re-matches #"chunk-\p{XDigit}+/temporal\.arrow" %) (.listObjects ^ObjectStore obj))))

(t/deftest indexer-flushes-block-and-chunk-if-flush-op-test
  (with-open [node (start-node #time/duration "PT0.001S")]
    (t/is (spin-ensure 10 (= 0 (count (chunk-path-seq node)))))
    (xt/submit-tx node [[:put :foo {:xt/id 42}]])
    (t/is (spin (= 1 (count (chunk-path-seq node))))))

  (with-open [node (start-node #time/duration "PT1H")]
    (xt/submit-tx node [[:put :foo {:xt/id 42}]])
    (t/is (spin-ensure 10 (= 0 (count (chunk-path-seq node)))))))
