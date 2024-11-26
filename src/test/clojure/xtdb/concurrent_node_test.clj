(ns xtdb.concurrent-node-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            xtdb.node.impl
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (org.apache.arrow.memory ArrowBuf)
           (java.time InstantSource)
           xtdb.IBufferPool
           xtdb.api.storage.Storage
           xtdb.api.log.Logs))

(defn- random-maps [n]
  (let [nb-ks 5
        ks [:foo :bar :baz :foobar :barfoo]]
    (->> (repeatedly n #(zipmap ks (map str (repeatedly nb-ks random-uuid))))
         (map-indexed #(assoc %2 :xt/id %1)))))

(def ^java.io.File node-dir (io/file "dev/concurrent-node-test"))

(def node-opts {:node-dir (.toPath node-dir)
                :instant-src (InstantSource/system)})

(defn- populate-node [{:keys [node-dir] :as node-opts}]
  (when-not (util/path-exists node-dir)
    (with-open [node (tu/->local-node node-opts)]
      (doseq [tx (->> (random-maps 400000)
                      (map #(do [:put-docs :docs %]))
                      (partition-all 1024))]
        (xt/submit-tx node tx))
      (tu/finish-chunk! node))))

(comment
  (util/delete-dir (.toPath node-dir))
  (populate-node node-opts))

(t/use-fixtures :each tu/with-allocator)

(deftest ^:integration concurrent-buffer-pool-test
  (populate-node node-opts)
  (tu/with-system {:xtdb.metrics/registry nil
                   :xtdb/allocator {}
                   :xtdb/log (Logs/localLog (.resolve (.toPath node-dir) "logs"))
                   :xtdb/buffer-pool (Storage/localStorage (.resolve (.toPath node-dir) "objects"))}
    (fn []
      (let [^IBufferPool buffer-pool (:xtdb/buffer-pool tu/*sys*)
            objs (filter #(= "arrow" (tu/get-extension %)) (.listAllObjects buffer-pool))
            get-item #(with-open [_rb (.getRecordBatch buffer-pool (rand-nth objs) 0)]
                        (Thread/sleep 10))
            f-call #(future
                      (dotimes [_ 300]
                        (get-item)))

            fs (doall (repeatedly 3 f-call))]
        (mapv deref fs)))))

(deftest ^:integration concurrent-node-test
  (populate-node node-opts)
  (with-open [node (tu/->local-node node-opts)]
    (let [open-ids (->> (xt/q node '(from :docs [{:xt/id id}]))
                        (mapv :id))
          get-item #(xt/q node '(from :docs [{:xt/id $id} foo bar baz foobar barfoo])
                          {:args {:id (rand-nth open-ids)}})
          f-call #(future
                    (dotimes [_ 100]
                      (get-item)))
          fs (doall (repeatedly 3 f-call))]
      (mapv deref fs)
      ;; just so deftest doesn't complain about no assertions
      (t/is (true? true)))))
