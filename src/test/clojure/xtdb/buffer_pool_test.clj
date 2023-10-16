(ns xtdb.buffer-pool-test
  (:require [clojure.test :as t]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.buffer-pool :as bp]
            xtdb.node
            [xtdb.object-store-test :as ost]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (java.nio ByteBuffer)
           (org.apache.arrow.memory ArrowBuf RootAllocator)
           (xtdb.util ArrowBufLRU)))

(def ^:dynamic *bp-type* nil)
(def ^:dynamic ^xtdb.IBufferPool *buffer-pool* nil)

(defn- with-bp [opts f]
  (tu/with-system (into {:xtdb/allocator {}} opts)
    (fn []
      (binding [*buffer-pool* (some-> (ig/find-derived tu/*sys* :xtdb/buffer-pool) first val)]
        (f)))))

(t/use-fixtures :each
  (fn with-each-bp [f]
    (t/testing "memory"
      (binding [*bp-type* :memory]
        (with-bp {::bp/in-memory {}} f)))

    (t/testing "local"
      (tu/with-tmp-dirs #{path}
        (binding [*bp-type* :local]
          (with-bp {::bp/local {:path path}} f))))

    (t/testing "remote"
      (binding [*bp-type* :remote]
        (with-bp {::bp/remote {}
                  ::ost/memory-object-store {}}
          f)))))

(defn byte-seq [^ArrowBuf buf]
  (let [arr (byte-array (.capacity buf))]
    (.getBytes buf 0 arr)
    (seq arr)))

(t/deftest range-test
;; return a new key, having the given cache state
        ;; by putting an object and warming the cache
        ;; e.g :full (whole object cached)
        ;;     :cold (nothing cached)
        ;;     [start, len] (range cached)
  (letfn [(setup-key [cache]
            (let [k (str (random-uuid))]
              @(.putObject *buffer-pool* k (ByteBuffer/wrap (byte-array (range 10))))
              (cond
                (= :cold cache) nil
                (= :full cache) (util/close @(.getBuffer *buffer-pool* k))
                (vector? cache) (util/close @(.getRangeBuffer *buffer-pool* k (cache 0) (cache 1)))
                :else (throw (IllegalArgumentException. "Buffer pool setup param should be a range vector, :full or :cold")))
              k))]

    (doseq [cache [:cold :full [2 4] [4 3]]]
      (t/testing (str "with cache:" cache)
        (t/testing "get full"
          (with-open [^ArrowBuf buf @(.getBuffer *buffer-pool* (setup-key cache))]
            (t/is (= 10 (.capacity buf)))
            (t/is (= (range 10) (byte-seq buf)))))

        (t/testing "full via range"
          (with-open [^ArrowBuf buf @(.getRangeBuffer *buffer-pool* (setup-key cache) 0 10)]
            (t/is (= 10 (.capacity buf)))
            (t/is (= (range 10) (byte-seq buf)))))

        (t/testing "partial range"
          (with-open [^ArrowBuf buf @(.getRangeBuffer *buffer-pool* (setup-key cache) 2 4)]
            (t/is (= 4 (.capacity buf)))
            (t/is (= (range 2 6) (byte-seq buf)))))

        (t/testing "oob"
          (let [close-if-no-ex (fn [f] (let [ret (f)] (util/close ret) ret))
                rq (fn [start len] (close-if-no-ex (fn [] @(.getRangeBuffer *buffer-pool* (setup-key cache) start len))))]
            (->> "sanity check"
                 (t/is (any? (rq 0 4))))

            (->> "negative index should oob"
                 (t/is (thrown? Exception (rq -1 1))))

            (->> "zero len should oob"
                 (t/is (thrown? Exception (rq 0 0))))

            (->> "max is ok"
                 (t/is (any? (rq 9 1))))

            (->> "max+1 at zero len should oob"
                 (t/is (thrown? Exception (rq 10 0))))))))))

(t/deftest cache-counter-test
  (when (= *bp-type* :remote)
    (bp/clear-cache-counters)
    (t/is (= 0 (.get bp/cache-hit-byte-counter)))
    (t/is (= 0 (.get bp/cache-miss-byte-counter)))
    (t/is (= 0N @bp/io-wait-nanos-counter))

    @(.putObject *buffer-pool* "foo" (ByteBuffer/wrap (.getBytes "hello")))
    (with-open [^ArrowBuf _buf @(.getBuffer *buffer-pool* "foo")])

    (t/is (pos? (.get bp/cache-miss-byte-counter)))
    (t/is (= 0 (.get bp/cache-hit-byte-counter)))
    (t/is (pos? @bp/io-wait-nanos-counter))

    (with-open [^ArrowBuf _buf @(.getBuffer *buffer-pool* "foo")])

    (t/is (pos? (.get bp/cache-hit-byte-counter)))
    (t/is (= (.get bp/cache-hit-byte-counter) (.get bp/cache-miss-byte-counter)))

    (let [ch (.get bp/cache-hit-byte-counter)]
      (with-open [^ArrowBuf _buf @(.getRangeBuffer *buffer-pool* "foo" 2 1)])
      (t/is (= (inc ch) (.get bp/cache-hit-byte-counter))))

    (bp/clear-cache-counters)

    (t/is (= 0 (.get bp/cache-hit-byte-counter)))
    (t/is (= 0 (.get bp/cache-miss-byte-counter)))
    (t/is (= 0N @bp/io-wait-nanos-counter))))

(t/deftest arrow-buf-lru-test
  (t/testing "max size restriction"
    (let [allocator (RootAllocator.)
          lru (ArrowBufLRU. 1 2 128)
          buf1 (util/->arrow-buf-view allocator (ByteBuffer/wrap (byte-array (range 16))))
          buf2 (util/->arrow-buf-view allocator (ByteBuffer/wrap (byte-array (range 16))))
          buf3 (util/->arrow-buf-view allocator (ByteBuffer/wrap (byte-array (range 16))))]
      (.put lru "buf1" buf1)
      (.put lru "buf2" buf2)
      (t/is (= buf1 (.get lru "buf1")))
      (t/is (= buf2 (.get lru "buf2")))
      (.put lru "buf3" buf3)
      (t/is (nil? (.get lru "buf1")))
      (t/is (= buf3 (.get lru "buf3")))))

  (t/testing "max byte size restriction"
    (let [allocator (RootAllocator.)
          lru (ArrowBufLRU. 1 5 32)
          buf1 (util/->arrow-buf-view allocator (ByteBuffer/wrap (byte-array (range 16))))
          buf2 (util/->arrow-buf-view allocator (ByteBuffer/wrap (byte-array (range 16))))
          buf3 (util/->arrow-buf-view allocator (ByteBuffer/wrap (byte-array (range 16))))]
      (.put lru "buf1" buf1)
      (.put lru "buf2" buf2)
      (t/is (= buf1 (.get lru "buf1")))
      (t/is (= buf2 (.get lru "buf2")))
      (.put lru "buf3" buf3)
      (t/is (nil? (.get lru "buf1")))
      (t/is (= buf3 (.get lru "buf3"))))))
