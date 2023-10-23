(ns xtdb.buffer-pool-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.buffer-pool :as bp]
            xtdb.node
            [xtdb.object-store :as os]
            [xtdb.object-store-test :as os-test]
            [xtdb.test-util :as tu]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import (clojure.lang IDeref)
           (java.io Closeable)
           (java.nio ByteBuffer)
           (java.nio.file Files Path)
           (java.nio.file.attribute FileAttribute)
           (java.util Map TreeMap)
           (java.util.concurrent CompletableFuture)
           (org.apache.arrow.memory ArrowBuf)
           (org.apache.arrow.vector IntVector VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo Schema)
           (xtdb.object_store IMultipartUpload ObjectStore SupportsMultipart)
           (xtdb.util ArrowBufLRU)))

(set! *warn-on-reflection* false)

(defn closeable [x close-fn] (reify IDeref (deref [_] x) Closeable (close [_] (close-fn x))))

(defonce tmp-dirs (atom []))

(defn create-tmp-dir [] (peek (swap! tmp-dirs conj (Files/createTempDirectory "bp-test" (make-array FileAttribute 0)))))

(defn each-fixture [f]
  (try
    (f)
    (finally
      (run! util/delete-dir @tmp-dirs)
      (reset! tmp-dirs []))))

(defn once-fixture [f] (tu/with-allocator f))

(t/use-fixtures :each #'each-fixture)
(t/use-fixtures :once #'once-fixture)

(defn byte-seq [^ArrowBuf buf]
  (let [arr (byte-array (.capacity buf))]
    (.getBytes buf 0 arr)
    (seq arr)))

(t/deftest cache-counter-test
  (with-open [os (os-test/->InMemoryObjectStore (TreeMap.))
              bp (bp/->remote {:allocator tu/*allocator*
                               :object-store os
                               :data-dir (create-tmp-dir)})]
    (bp/clear-cache-counters)
    (t/is (= 0 (.get bp/cache-hit-byte-counter)))
    (t/is (= 0 (.get bp/cache-miss-byte-counter)))
    (t/is (= 0N @bp/io-wait-nanos-counter))

    @(.putObject ^ObjectStore os "foo" (ByteBuffer/wrap (.getBytes "hello")))

    (with-open [^ArrowBuf _buf @(bp/get-buffer bp "foo")])

    (t/is (pos? (.get bp/cache-miss-byte-counter)))
    (t/is (= 0 (.get bp/cache-hit-byte-counter)))
    (t/is (pos? @bp/io-wait-nanos-counter))

    (with-open [^ArrowBuf _buf @(bp/get-buffer bp "foo")])

    (t/is (pos? (.get bp/cache-hit-byte-counter)))
    (t/is (= (.get bp/cache-hit-byte-counter) (.get bp/cache-miss-byte-counter)))

    (bp/clear-cache-counters)

    (t/is (= 0 (.get bp/cache-hit-byte-counter)))
    (t/is (= 0 (.get bp/cache-miss-byte-counter)))
    (t/is (= 0N @bp/io-wait-nanos-counter))))

(t/deftest arrow-buf-lru-test
  (t/testing "max size restriction"
    (with-open [lru (closeable (ArrowBufLRU. 1 2 128) #'bp/free-memory)]
      (let [^Map lru @lru
            buf1 (util/->arrow-buf-view tu/*allocator* (ByteBuffer/wrap (byte-array (range 16))))
            buf2 (util/->arrow-buf-view tu/*allocator* (ByteBuffer/wrap (byte-array (range 16))))
            buf3 (util/->arrow-buf-view tu/*allocator* (ByteBuffer/wrap (byte-array (range 16))))]
        (.put lru "buf1" buf1)
        (.put lru "buf2" buf2)
        (t/is (= buf1 (.get lru "buf1")))
        (t/is (= buf2 (.get lru "buf2")))
        (.put lru "buf3" buf3)
        (t/is (nil? (.get lru "buf1")))
        (t/is (= buf3 (.get lru "buf3"))))))

  (t/testing "max byte size restriction"
    (with-open [lru (closeable (ArrowBufLRU. 1 5 32) #'bp/free-memory)]
      (let [^Map lru @lru
            buf1 (util/->arrow-buf-view tu/*allocator* (ByteBuffer/wrap (byte-array (range 16))))
            buf2 (util/->arrow-buf-view tu/*allocator* (ByteBuffer/wrap (byte-array (range 16))))
            buf3 (util/->arrow-buf-view tu/*allocator* (ByteBuffer/wrap (byte-array (range 16))))]
        (.put lru "buf1" buf1)
        (.put lru "buf2" buf2)
        (t/is (= buf1 (.get lru "buf1")))
        (t/is (= buf2 (.get lru "buf2")))
        (.put lru "buf3" buf3)
        (t/is (nil? (.get lru "buf1")))
        (t/is (= buf3 (.get lru "buf3")))))))

(defn copy-byte-buffer ^ByteBuffer [^ByteBuffer buf]
  (-> (ByteBuffer/allocate (.remaining buf))
      (.put buf)
      (.flip)))

(defn concat-byte-buffers ^ByteBuffer [buffers]
  (let [n (reduce + (map #(.remaining ^ByteBuffer %) buffers))
        dst (ByteBuffer/allocate n)]
    (doseq [^ByteBuffer src buffers]
      (.put dst src))
    (.flip dst)))

(defn utf8-buf [s] (ByteBuffer/wrap (.getBytes (str s) "utf-8")))

(defn arrow-buf-bytes ^bytes [^ArrowBuf arrow-buf]
  (let [n (.capacity arrow-buf)
        barr (byte-array n)]
    (.getBytes arrow-buf 0 barr)
    barr))

(defn arrow-buf-utf8 [arrow-buf]
  (String. (arrow-buf-bytes arrow-buf) "utf-8"))

(defn arrow-buf->nio [arrow-buf]
  ;; todo get .nioByteBuffer to work
  (ByteBuffer/wrap (arrow-buf-bytes arrow-buf)))

(defn evict-buffer [bp k] (.close (.remove (:memory-store bp) k)))

(defn test-get-object [bp ^String k ^ByteBuffer expected]
  (let [{:keys [^Path disk-store, object-store]} bp]

    (t/testing "immediate get from buffers map produces correct buffer"
      (with-open [buf @(bp/get-buffer bp k)]
        (t/is (= 0 (util/compare-nio-buffers-unsigned expected (arrow-buf->nio buf))))))

    (when disk-store
      (t/testing "expect a file to exist under our :data-dir"
        (t/is (util/path-exists (.resolve disk-store k)))
        (t/is (= 0 (util/compare-nio-buffers-unsigned expected (util/->mmap-path (.resolve disk-store k))))))

      (t/testing "if the buffer is evicted, it is loaded from disk"
        (evict-buffer bp k)
        (with-open [buf @(bp/get-buffer bp k)]
          (t/is (= 0 (util/compare-nio-buffers-unsigned expected (arrow-buf->nio buf)))))))

    (when object-store
      (t/testing "if the buffer is evicted and deleted from disk, it is delivered from object storage"
        (evict-buffer bp k)
        (util/delete-file (.resolve disk-store k))
        (with-open [buf @(bp/get-buffer bp k)]
          (t/is (= 0 (util/compare-nio-buffers-unsigned expected (arrow-buf->nio buf)))))))))

(defrecord SimulatedObjectStore [calls buffers]
  ObjectStore
  (getObject [_ k] (CompletableFuture/completedFuture (get @buffers k)))
  (getObject [_ k path]
    (if-some [^ByteBuffer nio-buf (get @buffers k)]
      (let [barr (byte-array (.remaining nio-buf))]
        (.get (.duplicate nio-buf) barr)
        (io/copy barr (.toFile path))
        (CompletableFuture/completedFuture path))
      (CompletableFuture/failedFuture (os/obj-missing-exception k))))
  (putObject [_ k buf]
    (swap! buffers assoc k buf)
    (swap! calls conj :put)
    (CompletableFuture/completedFuture nil))
  SupportsMultipart
  (startMultipart [_ k]
    (let [parts (atom [])]
      (CompletableFuture/completedFuture
        (reify IMultipartUpload
          (uploadPart [_ buf]
            (swap! calls conj :upload)
            (swap! parts conj (copy-byte-buffer buf))
            (CompletableFuture/completedFuture nil))
          (complete [_]
            (swap! calls conj :complete)
            (swap! buffers assoc k (concat-byte-buffers @parts))
            (CompletableFuture/completedFuture nil))
          (abort [_]
            (swap! calls conj :abort)
            (CompletableFuture/completedFuture nil)))))))

(defn remote-test-buffer-pool []
  (bp/->remote {:allocator tu/*allocator*
                :object-store (->SimulatedObjectStore (atom []) (atom {}))
                :data-dir (create-tmp-dir)}))

(defn get-remote-calls [test-bp]
  @(:calls (:remote-store test-bp)))

(defn put-buf [bp k nio-buf]
  (with-open [channel (bp/open-channel bp k)]
    (when (.hasRemaining nio-buf)
      (.write channel nio-buf))))

(t/deftest below-min-size-put-test
  (with-open [bp (remote-test-buffer-pool)]
    (t/testing "if <= min part size, putObject is used"
      (with-redefs [bp/min-multipart-part-size 2]
        (put-buf bp "min-part-put" (utf8-buf "12"))
        (t/is (= [:put] (get-remote-calls bp)))
        (test-get-object bp "min-part-put" (utf8-buf "12"))))))

(t/deftest above-min-size-multipart-test
  (with-open [bp (remote-test-buffer-pool)]
    (t/testing "if above min part size, multipart is used"
      (with-redefs [bp/min-multipart-part-size 2]
        (put-buf bp "min-part-multi" (utf8-buf "1234"))
        (t/is (= [:upload :upload :complete] (get-remote-calls bp)))
        (test-get-object bp "min-part-multi" (utf8-buf "1234"))))))

(t/deftest small-end-part-test
  (with-open [bp (remote-test-buffer-pool)]
    (t/testing "multipart, smaller end part"
      (with-redefs [bp/min-multipart-part-size 2]
        (put-buf bp "min-part-multi2" (utf8-buf "123"))
        (t/is (= [:upload :upload :complete] (get-remote-calls bp)))
        (test-get-object bp "min-part-multi2" (utf8-buf "123"))))))

(t/deftest arrow-ipc-test
  (with-open [bp (remote-test-buffer-pool)]
    (t/testing "multipart, arrow ipc"
      (let [schema (Schema. [(types/col-type->field "a" :i32)])
            upload-arrow-ipc-file-multipart bp/upload-arrow-ipc-file-multipart
            multipart-branch-taken (atom false)]
        (with-redefs [bp/min-multipart-part-size 320
                      bp/upload-arrow-ipc-file-multipart
                      (fn [& args]
                        (reset! multipart-branch-taken true)
                        (apply upload-arrow-ipc-file-multipart args))]
          (with-open [vsr (VectorSchemaRoot/create schema tu/*allocator*)
                      w (bp/open-arrow-file-writer bp "aw" vsr)]
            (let [^IntVector v (.getVector vsr "a")]
              (.setValueCount v 10)
              (dotimes [x 10] (.set v x x))
              (.start w)
              (.writeBatch w)
              (.end w))))

        (t/is @multipart-branch-taken true)
        (t/is (= [:upload :upload :complete] (get-remote-calls bp)))))))
