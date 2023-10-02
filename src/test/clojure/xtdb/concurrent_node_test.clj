(ns xtdb.concurrent-node-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.buffer-pool :as bp]
            [xtdb.object-store :as os]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (org.apache.arrow.memory ArrowBuf)
           (xtdb.buffer_pool IBufferPool)
           (xtdb.object_store ObjectStore)
           (xtdb InstantSource)))

(defn- random-maps [n]
  (let [nb-ks 5
        ks [:foo :bar :baz :foobar :barfoo]]
    (->> (repeatedly n #(zipmap ks (map str (repeatedly nb-ks random-uuid))))
         (map-indexed #(assoc %2 :xt/id %1)))))

(defn q-now [node q+args]
  (xt/q node q+args
        {:basis {:after-tx (:latest-completed-tx (xt/status node))}}))

(def ^java.io.File node-dir (io/file "dev/concurrent-node-test"))
(def node-opts {:node-dir (.toPath node-dir)
                :instant-src InstantSource/SYSTEM})

(defn- populate-node [{:keys [node-dir] :as node-opts}]
  (when-not (util/path-exists node-dir)
    (with-open [node (tu/->local-node node-opts)]
      (doseq [tx (->> (random-maps 400000)
                      (map #(vector :put :docs %))
                      (partition-all 1024))]
        (xt/submit-tx node tx))
      (tu/finish-chunk! node))))

(comment
  (util/delete-dir (.toPath node-dir))
  (populate-node node-opts))

(t/use-fixtures :each tu/with-allocator)

(deftest ^:integration concurrent-buffer-pool-test
  (populate-node node-opts)
  (tu/with-system {:xtdb/allocator {}
                   :xtdb.buffer-pool/buffer-pool {:cache-path (.resolve (.toPath node-dir) "buffers")}
                   :xtdb.object-store/file-system-object-store {:root-path (.resolve (.toPath node-dir) "objects")}}
    (fn []
      (let [^IBufferPool buffer-pool (::bp/buffer-pool tu/*sys*)
            ^ObjectStore object-store (::os/file-system-object-store tu/*sys*)
            objs (.listObjects object-store)
            get-item #(with-open [^ArrowBuf _buf (deref (.getBuffer buffer-pool (rand-nth objs)))]
                        (Thread/sleep 10))
            f-call #(future
                      (dotimes [_ 300]
                        (get-item)))

            fs (doall (repeatedly 3 f-call))]
        (t/is (not (nil? object-store)))
        (mapv deref fs)))))

(deftest ^:integration concurrent-node-test
  (populate-node node-opts)
  (with-open [node (tu/->local-node node-opts)]
    (let [open-ids (->> (xt/q node '{:find [id] :where [(match :docs {:xt/id id})]})
                        (mapv :id))
          q '{:find [id foo bar baz foobar barfoo]
              :in [id]
              :where [(match :docs [{:xt/id id} foo bar baz foobar barfoo])]}
          get-item #(q-now node [q (rand-nth open-ids)])
          f-call #(future
                    (dotimes [_ 100]
                      (get-item)))
          fs (doall (repeatedly 3 f-call))]
      (mapv deref fs)
      ;; just so deftest doesn't complain about no assertions
      (t/is (true? true)))))
