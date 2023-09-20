(ns xtdb.buffer-pool-test
  (:require [clojure.test :as t]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.buffer-pool :as bp])
  (:import (java.nio ByteBuffer)
           (org.apache.arrow.memory ArrowBuf)
           (xtdb.buffer_pool IBufferPool)
           (xtdb.object_store ObjectStore)))

(set! *warn-on-reflection* false)

;; loads :xtdb/allocator ig/init-key
(require 'xtdb.node)

(defn buffer-sys []
  (-> {:xtdb/allocator {}
       :xtdb.object-store/memory-object-store {}
       ::bp/buffer-pool {:object-store (ig/ref :xtdb.object-store/memory-object-store)}}
      (doto ig/load-namespaces)
      ig/prep
      ig/init))

(defn object-store ^ObjectStore [sys] (:xtdb.object-store/memory-object-store sys))

(defn buffer-pool ^IBufferPool [sys] (::bp/buffer-pool sys))

(defn byte-seq [^ArrowBuf buf]
  (let [arr (byte-array (.capacity buf))]
    (.getBytes buf 0 arr)
    (seq arr)))

(t/deftest range-test
  (let [sys (buffer-sys)
        os (object-store sys)
        bp (buffer-pool sys)

        ;; return a new key, having the given cache state
        ;; by putting an object and warming the cache
        ;; e.g :full (whole object cached)
        ;;     :cold (nothing cached)
        ;;     [start, len] (range cached)
        setup-key
        (fn [cache]
          (let [k (str (random-uuid))]
            @(.putObject os k (ByteBuffer/wrap (byte-array (range 10))))
            (cond
              (= :cold cache) nil
              (= :full cache) (.close @(.getBuffer bp k))
              (vector? cache) (.close @(.getRangeBuffer bp k (cache 0) (cache 1)))
              :else (throw (IllegalArgumentException. "Buffer pool setup param should be a range vector, :full or :cold")))
            k))

        key-cache-states
        [:cold
         :full
         [2 4]
         [4 3]]]
    (try

      (doseq [cache key-cache-states]
        (t/testing (str "with cache:" cache)
          (t/testing "get full"
            (with-open [^ArrowBuf buf @(.getBuffer bp (setup-key cache))]
              (t/is (= 10 (.capacity buf)))
              (t/is (= (range 10) (byte-seq buf)))))

          (t/testing "full via range"
            (with-open [^ArrowBuf buf @(.getRangeBuffer bp (setup-key cache) 0 10)]
              (t/is (= 10 (.capacity buf)))
              (t/is (= (range 10) (byte-seq buf)))))

          (t/testing "partial range"
            (with-open [^ArrowBuf buf @(.getRangeBuffer bp (setup-key cache) 2 4)]
              (t/is (= 4 (.capacity buf)))
              (t/is (= (range 2 6) (byte-seq buf)))))

          (t/testing "oob"
            (let [close-if-no-ex (fn [f] (let [ret (f)] (.close ret) ret))
                  rq (fn [start len] (close-if-no-ex (fn [] @(.getRangeBuffer bp (setup-key cache) start len))))]
              (->> "sanity check"
                   (t/is (any? (rq 0 4))))

              (->> "negative index should oob"
                   (t/is (thrown? Exception (rq -1 1))))

              (->> "zero len should oob"
                   (t/is (thrown? Exception (rq 0 0))))

              (->> "max is ok"
                   (t/is (any? (rq 9 1))))

              (->> "max+1 at zero len should oob"
                   (t/is (thrown? Exception (rq 10 0))))))))

      (finally
        (ig/halt! sys)))))

(t/deftest cache-counter-test
  (let [sys (buffer-sys)
        os (object-store sys)
        bp (buffer-pool sys)]
    (bp/clear-cache-counters)
    (t/is (= 0 (.get bp/cache-hit-byte-counter)))
    (t/is (= 0 (.get bp/cache-miss-byte-counter)))
    (t/is (= 0N @bp/io-wait-nanos-counter))
    @(.putObject os "foo" (ByteBuffer/wrap (.getBytes "hello")))
    @(.getBuffer bp "foo")
    (t/is (pos? (.get bp/cache-miss-byte-counter)))
    (t/is (= 0 (.get bp/cache-hit-byte-counter)))
    (t/is (pos? @bp/io-wait-nanos-counter))
    @(.getBuffer bp "foo")
    (t/is (pos? (.get bp/cache-hit-byte-counter)))
    (t/is (= (.get bp/cache-hit-byte-counter) (.get bp/cache-miss-byte-counter)))
    (let [ch (.get bp/cache-hit-byte-counter)]
      @(.getRangeBuffer bp "foo" 2 1)
      (t/is (= (inc ch) (.get bp/cache-hit-byte-counter))))
    (bp/clear-cache-counters)
    (t/is (= 0 (.get bp/cache-hit-byte-counter)))
    (t/is (= 0 (.get bp/cache-miss-byte-counter)))
    (t/is (= 0N @bp/io-wait-nanos-counter))))
