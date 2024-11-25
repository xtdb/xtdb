(ns xtdb.buffer-pool-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [integrant.core :as ig]
            [xtdb.api :as xt]
            [xtdb.buffer-pool :as bp]
            [xtdb.node :as xtn]
            [xtdb.object-store :as os]
            [xtdb.test-util :as tu]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import (com.github.benmanes.caffeine.cache AsyncCache)
           (io.micrometer.core.instrument.simple SimpleMeterRegistry)
           (java.io File)
           (java.nio ByteBuffer)
           [java.nio.charset StandardCharsets]
           (java.nio.file Files Path)
           (java.nio.file.attribute FileAttribute)
           (java.util.concurrent CompletableFuture)
           (org.apache.arrow.memory ArrowBuf)
           (org.apache.arrow.vector.types.pojo Schema)
           xtdb.api.log.FileListCache
           (xtdb.api.storage ObjectStore ObjectStore$Factory Storage)
           xtdb.arrow.Relation
           xtdb.buffer_pool.RemoteBufferPool
           xtdb.cache.DiskCache
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
    (util/with-open [node (xtn/start-node (merge tu/*node-opts* {:storage [:remote {:object-store [:in-memory {}]
                                                                                    :local-disk-cache path}]}))]
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
  (let [{:keys [^Path disk-store, object-store, ^DiskCache disk-cache]} bp
        root-path (.getRootPath disk-cache)]

    (t/testing "immediate get from buffers map produces correct buffer"
      (util/with-open [buf (.getBuffer bp k)]
        (t/is (= 0 (util/compare-nio-buffers-unsigned expected (arrow-buf->nio buf))))))

    (when root-path
      (t/testing "expect a file to exist under our :disk-store"
        (t/is (util/path-exists (.resolve root-path k)))
        (t/is (= 0 (util/compare-nio-buffers-unsigned expected (util/->mmap-path (.resolve root-path k))))))

      (t/testing "if the buffer is evicted, it is loaded from disk"
        (bp/evict-cached-buffer! bp k)
        (util/with-open [buf (.getBuffer bp k)]
          (t/is (= 0 (util/compare-nio-buffers-unsigned expected (arrow-buf->nio buf)))))))

    (when object-store
      (t/testing "if the buffer is evicted and deleted from disk, it is delivered from object storage"
        (bp/evict-cached-buffer! bp k)
        ;; Evicted from map and deleted from disk (ie, replicating effects of 'eviction' here)
        (-> (.asMap (.getCache (.getPinningCache disk-cache)))
            (.remove k))
        (util/delete-file (.resolve root-path k))
        ;; Will fetch from object store again
        (util/with-open [buf (.getBuffer bp k)]
          (t/is (= 0 (util/compare-nio-buffers-unsigned expected (arrow-buf->nio buf)))))))))

(defrecord SimulatedObjectStore [!calls !buffers]
  ObjectStore
  (getObject [_ k] (CompletableFuture/completedFuture (get @!buffers k)))

  (getObject [_ k path]
    (if-some [^ByteBuffer nio-buf (get @!buffers k)]
      (let [barr (byte-array (.remaining nio-buf))]
        (.get (.duplicate nio-buf) barr)
        (io/copy barr (.toFile path))
        (CompletableFuture/completedFuture path))
      (CompletableFuture/failedFuture (os/obj-missing-exception k))))

  (putObject [_ k buf]
    (swap! !buffers assoc k buf)
    (swap! !calls conj :put)
    (CompletableFuture/completedFuture nil))

  (listAllObjects [_]
    (->> @!buffers
         (mapv (fn [[^Path k, ^ByteBuffer buf]]
                 (os/->StoredObject k (.capacity buf))))))

  SupportsMultipart
  (startMultipart [_ k]
    (let [parts (atom [])]
      (CompletableFuture/completedFuture
        (reify IMultipartUpload
          (uploadPart [_ buf]
            (swap! !calls conj :upload)
            (swap! parts conj (copy-byte-buffer buf))
            (CompletableFuture/completedFuture nil))

          (complete [_]
            (swap! !calls conj :complete)
            (swap! !buffers assoc k (concat-byte-buffers @parts))
            (CompletableFuture/completedFuture nil))

          (abort [_]
            (swap! !calls conj :abort)
            (CompletableFuture/completedFuture nil)))))))

(defn simulated-obj-store-factory []
  (let [!buffers (atom {})]
    (reify ObjectStore$Factory
      (openObjectStore [_]
        (->SimulatedObjectStore (atom []) !buffers)))))

(defn remote-test-buffer-pool ^xtdb.IBufferPool []
  (bp/open-remote-storage tu/*allocator*
                          (Storage/remoteStorage (simulated-obj-store-factory) (create-tmp-dir))
                          FileListCache/SOLO
                          (SimpleMeterRegistry.)))

(defn get-remote-calls [test-bp]
  @(:!calls (:object-store test-bp)))

(t/deftest below-min-size-put-test
  (with-open [bp (remote-test-buffer-pool)]
    (t/testing "if <= min part size, putObject is used"
      (with-redefs [bp/min-multipart-part-size 2]
        (.putObject bp (util/->path "min-part-put") (utf8-buf "12"))
        (t/is (= [:put] (get-remote-calls bp)))
        (test-get-object bp (util/->path "min-part-put") (utf8-buf "12"))))))

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
          (with-open [rel (Relation. tu/*allocator* schema)
                      w (.openArrowWriter bp (util/->path "aw") rel)]
            (let [v (.get rel "a")]
              (dotimes [x 10]
                (.writeInt v x))
              (.writeBatch w)
              (.end w))))

        (t/is @multipart-branch-taken true)
        (t/is (= [:upload :upload :complete] (get-remote-calls bp)))
        (util/with-open [buf (.getBuffer bp (util/->path "aw"))]
          (let [{:keys [root]} (util/read-arrow-buf buf)]
            (util/close root)))))))

(defn file-info [^Path dir]
  (let [files (filter #(.isFile ^File %) (file-seq (.toFile dir)))]
    {:file-count (count files) :file-names (set (map #(.getName ^File %) files))}))

(defn insert-utf8-to-local-cache [^IBufferPool bp k len]
  (.putObject bp k (utf8-buf (apply str (repeat len "a"))))
  ;; Add to local disk cache
  (with-open [^ArrowBuf _buf (.getBuffer bp k)]))

(t/deftest local-disk-cache-max-size
  (util/with-tmp-dirs #{local-disk-cache}
    (with-open [bp (bp/open-remote-storage
                    tu/*allocator*
                    (-> (Storage/remoteStorage (simulated-obj-store-factory) local-disk-cache)
                        (.maxDiskCacheBytes 10)
                        (.maxCacheBytes 12))
                    FileListCache/SOLO
                    (SimpleMeterRegistry.))]
      (t/testing "staying below max size - all elements available"
        (insert-utf8-to-local-cache bp (util/->path "a") 4)
        (insert-utf8-to-local-cache bp (util/->path "b") 4)
        (t/is (= {:file-count 2 :file-names #{"a" "b"}} (file-info local-disk-cache))))

      (t/testing "going above max size - all entries still present in memory cache (which isn't above limit) - should return all elements"
        (insert-utf8-to-local-cache bp (util/->path "c") 4)
        (t/is (= {:file-count 3 :file-names #{"a" "b" "c"}} (file-info local-disk-cache))))

      (t/testing "entries unpinned (cleared from memory cache by new entries) - should evict entries since above size limit"
        (insert-utf8-to-local-cache bp (util/->path "d") 4)
        (Thread/sleep 100)
        (t/is (= 3 (:file-count (file-info local-disk-cache))))

        (insert-utf8-to-local-cache bp (util/->path "e") 4)
        (Thread/sleep 100)
        (t/is (= 3 (:file-count (file-info local-disk-cache))))))))

(t/deftest local-disk-cache-with-previous-values
  (let [obj-store-factory (simulated-obj-store-factory)]
    (util/with-tmp-dirs #{local-disk-cache}
      ;; Writing files to buffer pool & local-disk-cache
      (with-open [bp (bp/open-remote-storage
                      tu/*allocator*
                      (-> (Storage/remoteStorage obj-store-factory local-disk-cache)
                          (.maxDiskCacheBytes 10)
                          (.maxCacheBytes 12))
                      FileListCache/SOLO
                      (SimpleMeterRegistry.))]
        (insert-utf8-to-local-cache bp (util/->path "a") 4)
        (insert-utf8-to-local-cache bp (util/->path "b") 4)
        (t/is (= {:file-count 2 :file-names #{"a" "b"}} (file-info local-disk-cache))))

      ;; Starting a new buffer pool - should load buffers correctly from disk (can be sure its grabbed from disk since using a memory cache and memory object store)
      (with-open [bp (bp/open-remote-storage
                      tu/*allocator*
                      (-> (Storage/remoteStorage obj-store-factory local-disk-cache)
                          (.maxDiskCacheBytes 10)
                          (.maxCacheBytes 12))
                      FileListCache/SOLO
                      (SimpleMeterRegistry.))]
        (with-open [^ArrowBuf buf (.getBuffer bp (util/->path "a"))]
          (t/is (= 0 (util/compare-nio-buffers-unsigned (utf8-buf "aaaa") (arrow-buf->nio buf)))))

        (with-open [^ArrowBuf buf (.getBuffer bp (util/->path "b"))]
          (t/is (= 0 (util/compare-nio-buffers-unsigned (utf8-buf "aaaa") (arrow-buf->nio buf)))))))))

(t/deftest local-buffer-pool
  (tu/with-tmp-dirs #{tmp-dir}
    (with-open [bp (bp/open-local-storage tu/*allocator* (Storage/localStorage tmp-dir) (SimpleMeterRegistry.))]
      (t/testing "empty buffer pool"
        (t/is (= [] (.listAllObjects bp)))
        (t/is (= [] (.listObjects bp (.toPath (io/file "foo")))))))))

(t/deftest dont-list-temporary-objects-3544
  (tu/with-tmp-dirs #{tmp-dir}
    (let [schema (Schema. [(types/col-type->field "a" :i32)])]
      (with-open [bp (bp/open-local-storage tu/*allocator* (Storage/localStorage tmp-dir) (SimpleMeterRegistry.))
                  rel (Relation. tu/*allocator* schema)
                  _arrow-writer (.openArrowWriter bp (.toPath (io/file "foo")) rel)]
        (t/is (= [] (.listAllObjects bp)))))))

(defn fetch-buffer-pool-from-node
  [node]
  (val (first (ig/find-derived (:system node) :xtdb/buffer-pool))))

(defn put-edn [^IBufferPool buffer-pool ^Path k obj]
  (let [^ByteBuffer buf (.encode StandardCharsets/UTF_8 (pr-str obj))]
    (.putObject buffer-pool k buf)))

(defn test-list-objects [^IBufferPool buffer-pool]
  (put-edn buffer-pool (util/->path "bar/alice") :alice)
  (put-edn buffer-pool (util/->path "foo/alan") :alan)
  (put-edn buffer-pool (util/->path "bar/bob") :bob)
  (put-edn buffer-pool (util/->path "bar/baz/dan") :dan)
  (put-edn buffer-pool (util/->path "bar/baza/james") :james)
  (Thread/sleep 1000)

  (t/is (= (mapv util/->path ["bar/alice" "bar/baz/dan" "bar/baza/james" "bar/bob" "foo/alan"])
           (.listAllObjects buffer-pool)))

  (t/is (= (mapv util/->path ["foo/alan"])
           (.listObjects buffer-pool (util/->path "foo"))))

  (t/testing "call listObjects with a prefix ended with a slash - should work the same"
    (t/is (= (mapv util/->path ["foo/alan"])
             (.listObjects buffer-pool (util/->path "foo/")))))

  (t/testing "calling listObjects with prefix on directory with subdirectories - should only return top level keys"
    (t/is (= (mapv util/->path ["bar/alice" "bar/baz" "bar/baza" "bar/bob"])
             (.listObjects buffer-pool (util/->path "bar")))))

  (t/testing "calling listObjects with prefix with common prefix - should only return that which is a complete match against a directory "
    (t/is (= (mapv util/->path ["bar/baz/dan"])
             (.listObjects buffer-pool (util/->path "bar/baz")))))

  (t/testing "objectSize"
    (t/is (= 6 (.objectSize buffer-pool (util/->path "bar/alice"))))
    (t/is (= 5 (.objectSize buffer-pool (util/->path "foo/alan"))))
    (t/is (= 4 (.objectSize buffer-pool (util/->path "bar/bob"))))
    (t/is (= 4 (.objectSize buffer-pool (util/->path "bar/baz/dan"))))
    (t/is (= 6 (.objectSize buffer-pool (util/->path "bar/baza/james"))))))

(t/deftest test-memory-list-objs
  (with-open [bp (bp/open-in-memory-storage tu/*allocator* (SimpleMeterRegistry.))]
    (test-list-objects bp)))

(t/deftest test-local-list-objs
  (tu/with-tmp-dirs #{tmp-dir}
    (with-open [bp (bp/open-local-storage tu/*allocator* (Storage/localStorage tmp-dir) (SimpleMeterRegistry.))]
      (test-list-objects bp))))
