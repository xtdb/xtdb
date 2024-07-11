(ns xtdb.buffer-pool-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.api :as xt]
            [xtdb.buffer-pool :as bp]
            [xtdb.node :as xtn]
            [xtdb.object-store :as os]
            [xtdb.test-util :as tu]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import (java.nio ByteBuffer)
           (java.nio.file Files Path)
           (java.nio.file.attribute FileAttribute)
           (java.util.concurrent CompletableFuture)
           (org.apache.arrow.memory ArrowBuf)
           (org.apache.arrow.vector IntVector VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo Schema)
           (xtdb.api.storage ObjectStore ObjectStoreFactory Storage)
           xtdb.buffer_pool.RemoteBufferPool
           xtdb.IBufferPool
           (xtdb.multipart IMultipartUpload SupportsMultipart)))

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

(t/deftest test-remote-buffer-pool-setup
  (util/with-tmp-dirs #{path}
    (util/with-open [node (xtn/start-node {:storage [:remote {:object-store [:in-memory {}]
                                                              :local-disk-cache path}]})]
      (xt/submit-tx node [[:put-docs :foo {:xt/id :foo}]])

      (t/is (= [{:xt/id :foo}]
               (xt/q node '(from :foo [xt/id]))))

      (tu/finish-chunk! node)

      (let [{:keys [^ObjectStore object-store] :as buffer-pool} (val (first (ig/find-derived (:system node) :xtdb/buffer-pool)))]
        (t/is (instance? RemoteBufferPool buffer-pool))

        (t/is (seq (.listAllObjects object-store)))))))

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

(defn arrow-buf->nio [arrow-buf]
  ;; todo get .nioByteBuffer to work
  (ByteBuffer/wrap (arrow-buf-bytes arrow-buf)))

(defn test-get-object [^IBufferPool bp, ^Path k, ^ByteBuffer expected]
  (let [{:keys [^Path disk-store, object-store]} bp]

    (t/testing "immediate get from buffers map produces correct buffer"
      (util/with-open [buf @(.getBuffer bp k)]
        (t/is (= 0 (util/compare-nio-buffers-unsigned expected (arrow-buf->nio buf))))))

    (when disk-store
      (t/testing "expect a file to exist under our :disk-store"
        (t/is (util/path-exists (.resolve disk-store k)))
        (t/is (= 0 (util/compare-nio-buffers-unsigned expected (util/->mmap-path (.resolve disk-store k))))))

      (t/testing "if the buffer is evicted, it is loaded from disk"
        (bp/evict-cached-buffer! bp k)
        (util/with-open [buf @(.getBuffer bp k)]
          (t/is (= 0 (util/compare-nio-buffers-unsigned expected (arrow-buf->nio buf)))))))

    (when object-store
      (t/testing "if the buffer is evicted and deleted from disk, it is delivered from object storage"
        (bp/evict-cached-buffer! bp k)
        (when disk-store
          (util/delete-file (.resolve disk-store k)))
        (util/with-open [buf @(.getBuffer bp k)]
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

(def simulated-obj-store-factory
  (reify ObjectStoreFactory
    (openObjectStore [_]
      (->SimulatedObjectStore (atom []) (atom {})))))

(defn remote-test-buffer-pool ^xtdb.IBufferPool []
  (bp/open-remote-storage tu/*allocator*
                          (Storage/remoteStorage simulated-obj-store-factory (create-tmp-dir))))

(defn get-remote-calls [test-bp]
  @(:calls (:object-store test-bp)))

(t/deftest below-min-size-put-test
  (with-open [bp (remote-test-buffer-pool)]
    (t/testing "if <= min part size, putObject is used"
      (with-redefs [bp/min-multipart-part-size 2]
        @(.putObject bp (util/->path "min-part-put") (utf8-buf "12"))
        (t/is (= [:put] (get-remote-calls bp)))
        (test-get-object bp (util/->path "min-part-put") (utf8-buf "12"))))))

(t/deftest above-min-size-multipart-test
  (with-open [bp (remote-test-buffer-pool)]
    (t/testing "if above min part size, multipart is used"
      (with-redefs [bp/min-multipart-part-size 2]
        @(.putObject bp (util/->path "min-part-multi") (utf8-buf "1234"))
        (t/is (= [:upload :upload :complete] (get-remote-calls bp)))
        (test-get-object bp (util/->path "min-part-multi") (utf8-buf "1234"))))))

(t/deftest small-end-part-test
  (with-open [bp (remote-test-buffer-pool)]
    (t/testing "multipart, smaller end part"
      (with-redefs [bp/min-multipart-part-size 2]
        @(.putObject bp (util/->path "min-part-multi2") (utf8-buf "123"))
        (t/is (= [:upload :upload :complete] (get-remote-calls bp)))
        (test-get-object bp (util/->path "min-part-multi2") (utf8-buf "123"))))))

(t/deftest arrow-ipc-test
  (with-open [bp (remote-test-buffer-pool)]
    (t/testing "multipart, arrow ipc"
      (let [schema (Schema. [(types/col-type->field "a" :i32)])
            upload-multipart-buffers @#'bp/upload-multipart-buffers
            multipart-branch-taken (atom false)]
        (with-redefs [bp/min-multipart-part-size 320
                      bp/upload-multipart-buffers
                      (fn [& args]
                        (reset! multipart-branch-taken true)
                        (apply upload-multipart-buffers args))]
          (with-open [vsr (VectorSchemaRoot/create schema tu/*allocator*)
                      w (.openArrowWriter bp (util/->path "aw") vsr)]
            (let [^IntVector v (.getVector vsr "a")]
              (.setValueCount v 10)
              (dotimes [x 10] (.set v x x))
              (.writeBatch w)
              (.end w))))

        (t/is @multipart-branch-taken true)
        (t/is (= [:upload :upload :complete] (get-remote-calls bp)))
        (util/with-open [buf @(.getBuffer bp (util/->path "aw"))]
          (let [{:keys [root]} (util/read-arrow-buf buf)]
            (util/close root)))))))
