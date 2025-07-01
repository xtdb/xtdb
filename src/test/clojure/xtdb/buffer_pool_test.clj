(ns xtdb.buffer-pool-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.buffer-pool :as bp]
            [xtdb.node :as xtn]
            [xtdb.object-store :as os]
            [xtdb.test-util :as tu]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import (io.micrometer.core.instrument Counter)
           (io.micrometer.core.instrument.simple SimpleMeterRegistry)
           (java.io File)
           (java.nio ByteBuffer)
           [java.nio.charset StandardCharsets]
           (java.nio.file Path)
           (org.apache.arrow.vector.types.pojo Schema)
           (xtdb BufferPool BufferPoolKt)
           (xtdb.api.storage ObjectStore ObjectStore$Factory Storage Storage$Factory)
           (xtdb.api.storage SimulatedObjectStore StoreOperation)
           xtdb.arrow.Relation
           (xtdb.buffer_pool LocalBufferPool MemoryBufferPool RemoteBufferPool)
           (xtdb.cache DiskCache MemoryCache)))

(t/use-fixtures :each tu/with-allocator)

(def ^:dynamic *mem-cache* nil)
(def ^:dynamic ^java.nio.file.Path *disk-cache-dir* nil)
(def ^:dynamic *disk-cache* nil)

(defmacro with-caches [opts & body]
  `(util/with-tmp-dir* "xtdb-cache"
     (fn [^Path dir#]
       (let [{mem-bytes# :mem-bytes, disk-bytes# :disk-bytes} ~opts]
         (with-open [mem-cache# (-> (MemoryCache/factory)
                                    (cond-> mem-bytes# (.maxSizeBytes mem-bytes#))
                                    (.open tu/*allocator* nil))]
           (binding [*mem-cache* mem-cache#
                     *disk-cache-dir* dir#
                     *disk-cache* (-> (DiskCache/factory dir#)
                                      (cond-> disk-bytes# (.maxSizeBytes disk-bytes#))
                                      (.build nil))]
             ~@body))))))

(defn- open-storage ^xtdb.BufferPool [^Storage$Factory factory]
  (.open factory tu/*allocator* *mem-cache* *disk-cache* nil Storage/VERSION))

(t/deftest test-remote-buffer-pool-setup
  (util/with-tmp-dirs #{path}
    (util/with-open [node (xtn/start-node (merge tu/*node-opts* {:storage [:remote {:object-store [:in-memory {}]}]
                                                                 :disk-cache {:path path}
                                                                 :compactor {:threads 0}}))]
      (xt/submit-tx node [[:put-docs :foo {:xt/id :foo}]])

      (t/is (= [{:xt/id :foo}]
               (xt/q node '(from :foo [xt/id]))))

      (tu/finish-block! node)

      (let [^RemoteBufferPool buffer-pool (bp/<-node node)
            object-store (.getObjectStore buffer-pool)]
        (t/is (seq (.listAllObjects object-store)))))))

(defn utf8-buf [s] (ByteBuffer/wrap (.getBytes (str s) "utf-8")))

(defn test-get-object [^RemoteBufferPool bp, ^Path k, ^ByteBuffer expected]
  (let [object-store (.getObjectStore bp)
        ^DiskCache disk-cache (.getDiskCache bp)
        root-path (.getRootPath disk-cache)]

    (t/testing "immediate get from buffers map produces correct buffer"
      (t/is (= 0 (util/compare-nio-buffers-unsigned expected (ByteBuffer/wrap (.getByteArray bp k))))))

    (when root-path
      (t/testing "expect a file to exist under our :disk-store"
        (t/is (util/path-exists (.resolve root-path k)))
        (t/is (= 0 (util/compare-nio-buffers-unsigned expected (util/->mmap-path (.resolve root-path k))))))

      (t/testing "if the buffer is evicted, it is loaded from disk"
        (.evictCachedBuffer bp k)
        (t/is (= 0 (util/compare-nio-buffers-unsigned expected (ByteBuffer/wrap (.getByteArray bp k)))))))

    (when object-store
      (t/testing "if the buffer is evicted and deleted from disk, it is delivered from object storage"
        (.evictCachedBuffer bp k)
        ;; Evicted from map and deleted from disk (ie, replicating effects of 'eviction' here)
        (-> (.asMap (.getCache (.getPinningCache disk-cache)))
            (.remove k))
        (util/delete-file (.resolve root-path k))
        ;; Will fetch from object store again
        (t/is (= 0 (util/compare-nio-buffers-unsigned expected (ByteBuffer/wrap (.getByteArray bp k)))))))))

(defn simulated-obj-store-factory []
  (reify ObjectStore$Factory
    (openObjectStore [_ _storage-root]
      (SimulatedObjectStore.))))

(defn get-remote-calls [^RemoteBufferPool test-bp]
  (.getCalls ^SimulatedObjectStore (.getObjectStore test-bp)))

(t/deftest below-min-size-put-test
  (with-caches {}
    (with-open [bp (-> (Storage/remoteStorage (simulated-obj-store-factory))
                       (.open tu/*allocator* *mem-cache* *disk-cache* nil Storage/VERSION))]
      (t/testing "if <= min part size, putObject is used"
        (.putObject bp (util/->path "min-part-put") (utf8-buf "12"))
        (t/is (= [StoreOperation/PUT] (get-remote-calls bp)))
        (test-get-object bp (util/->path "min-part-put") (utf8-buf "12"))))))

(defn file-info [^Path dir]
  (let [files (filter #(.isFile ^File %) (file-seq (.toFile dir)))]
    {:file-count (count files) :file-names (set (map #(.getName ^File %) files))}))

(defn insert-utf8-to-local-cache [^BufferPool bp k len]
  (.putObject bp k (utf8-buf (apply str (repeat len "a"))))
  ;; Add to local disk cache
  (.getByteArray bp k))

(t/deftest local-disk-cache-max-size
  (with-caches {:mem-bytes 12, :disk-bytes 10}
    (with-open [bp (-> (Storage/remoteStorage (simulated-obj-store-factory))
                       (.open tu/*allocator* *mem-cache* *disk-cache* nil Storage/VERSION))]
      (t/testing "staying below max size - all elements available"
        (insert-utf8-to-local-cache bp (util/->path "a") 4)
        (insert-utf8-to-local-cache bp (util/->path "b") 4)
        (t/is (= {:file-count 2 :file-names #{"a" "b"}} (file-info *disk-cache-dir*))))

      (t/testing "going above max size - all entries still present in memory cache (which isn't above limit) - should return all elements"
        (insert-utf8-to-local-cache bp (util/->path "c") 4)
        (t/is (= {:file-count 3 :file-names #{"a" "b" "c"}} (file-info *disk-cache-dir*))))

      (t/testing "entries unpinned (cleared from memory cache by new entries) - should evict entries since above size limit"
        (insert-utf8-to-local-cache bp (util/->path "d") 4)
        (Thread/sleep 100)
        (t/is (= 3 (:file-count (file-info *disk-cache-dir*))))

        (insert-utf8-to-local-cache bp (util/->path "e") 4)
        (Thread/sleep 100)
        (t/is (= 3 (:file-count (file-info *disk-cache-dir*))))))))

(defn no-op-object-store-factory []
  (reify ObjectStore$Factory
    (openObjectStore [_ _storage-root]
      (reify ObjectStore
        (getObject [_ _k] (throw (UnsupportedOperationException. "foo")))
        (getObject [_ _ _k] (throw (UnsupportedOperationException. "foo")))
        (putObject [_ _k _v] (throw (UnsupportedOperationException. "foo")))
        (listAllObjects [_] (throw (UnsupportedOperationException. "foo")))
        (listAllObjects [_ _] (throw (UnsupportedOperationException. "foo")))
        (deleteIfExists [_ _k] (throw (UnsupportedOperationException. "foo")))))))

(t/deftest local-disk-cache-with-previous-values
  (let [obj-store-factory (simulated-obj-store-factory)
        path-a (util/->path "a")
        path-b (util/->path "b")]
    (with-caches {:mem-bytes 64, :disk-bytes 64}
      ;; Writing files to buffer pool & local-disk-cache
      (with-open [bp (-> (Storage/remoteStorage obj-store-factory)
                         (.open tu/*allocator* *mem-cache* *disk-cache* nil Storage/VERSION))]
        (insert-utf8-to-local-cache bp path-a 4)
        (insert-utf8-to-local-cache bp path-b 4)
        (t/is (= {:file-count 2 :file-names #{"a" "b"}} (file-info *disk-cache-dir*))))

      ;; Starting a new buffer pool - should load buffers correctly from disk (can be sure its grabbed from disk since using a memory cache and memory object store)
      ;; passing a no-op object store to ensure objects are not loaded from object store and instead only the cache is under test.
      (with-open [bp (-> (Storage/remoteStorage (no-op-object-store-factory))
                         (.open tu/*allocator* *mem-cache* *disk-cache* nil Storage/VERSION))]
        (t/is (= 0 (util/compare-nio-buffers-unsigned (utf8-buf "aaaa") (ByteBuffer/wrap (.getByteArray bp path-a)))))
        (t/is (= 0 (util/compare-nio-buffers-unsigned (utf8-buf "aaaa") (ByteBuffer/wrap (.getByteArray bp path-b)))))))))

(t/deftest local-buffer-pool
  (tu/with-tmp-dirs #{tmp-dir}
    (with-caches {}
      (with-open [bp (open-storage (Storage/localStorage tmp-dir))]
        (t/testing "empty buffer pool"
          (t/is (= [] (.listAllObjects bp)))
          (t/is (= [] (.listAllObjects bp (.toPath (io/file "foo"))))))))))

(t/deftest dont-list-temporary-objects-3544
  (tu/with-tmp-dirs #{tmp-dir}
    (let [schema (Schema. [(types/col-type->field "a" :i32)])]
      (with-caches {}
        (with-open [bp (open-storage (Storage/localStorage tmp-dir))
                    rel (Relation/open tu/*allocator* schema)
                    _arrow-writer (.openArrowWriter bp (.toPath (io/file "foo")) rel)]
          (t/is (= [] (.listAllObjects bp))))))))

(defn put-edn [^BufferPool buffer-pool ^Path k obj]
  (let [^ByteBuffer buf (.encode StandardCharsets/UTF_8 (pr-str obj))]
    (.putObject buffer-pool k buf)))

(defn test-list-objects [^BufferPool buffer-pool]
  (put-edn buffer-pool (util/->path "bar/alice") :alice)
  (put-edn buffer-pool (util/->path "foo/alan") :alan)
  (put-edn buffer-pool (util/->path "bar/bob") :bob)
  (put-edn buffer-pool (util/->path "bar/baz/dan") :dan)
  (put-edn buffer-pool (util/->path "bar/baza/james") :james)
  (Thread/sleep 1000)

  (t/is (= [(os/->StoredObject "bar/alice" 6)
            (os/->StoredObject "bar/baz/dan" 4)
            (os/->StoredObject "bar/baza/james" 6)
            (os/->StoredObject "bar/bob" 4)
            (os/->StoredObject "foo/alan" 5)]
           (vec (.listAllObjects buffer-pool))))

  (t/is (= [(os/->StoredObject "foo/alan" 5)]
           (vec (.listAllObjects buffer-pool (util/->path "foo")))))

  (t/testing "call listAllObjects with a prefix ended with a slash - should work the same"
    (t/is (= [(os/->StoredObject "foo/alan" 5)]
             (vec (.listAllObjects buffer-pool (util/->path "foo/"))))))

  (t/testing "calling listAllObjects with prefix on directory with subdirectories - still lists recursively, and relative to the root"
    (t/is (= [(os/->StoredObject "bar/alice" 6)
              (os/->StoredObject "bar/baz/dan" 4)
              (os/->StoredObject "bar/baza/james" 6)
              (os/->StoredObject "bar/bob" 4)]
             (vec (.listAllObjects buffer-pool (util/->path "bar"))))))

  (t/testing "calling listAllObjects with prefix with common prefix - should only return that which is a complete match against a directory "
    (t/is (= [(os/->StoredObject "bar/baz/dan" 4)]
             (vec (.listAllObjects buffer-pool (util/->path "bar/baz")))))))

(t/deftest test-memory-list-objs
  (with-caches {}
    (with-open [bp (open-storage (Storage/inMemoryStorage))]
      (test-list-objects bp))))

(t/deftest test-local-list-objs
  (tu/with-tmp-dirs #{tmp-dir}
    (with-caches {}
      (with-open [bp (open-storage (Storage/localStorage tmp-dir))]
        (test-list-objects bp)))))

(t/deftest test-latest-available-block
  (tu/with-tmp-dirs #{tmp-dir}
    (with-open [node1 (xtn/start-node {:storage [:local {:path tmp-dir}]
                                       :compactor {:threads 0}})
                node2 (xtn/start-node {:storage [:local {:path tmp-dir}]
                                       :compactor {:threads 0}})]
      (let [bp1 (bp/<-node node1)
            bp2 (bp/<-node node2)]
        (t/is (= -1 (BufferPoolKt/getLatestAvailableBlockIndex bp1)))
        (t/is (= -1 (BufferPoolKt/getLatestAvailableBlockIndex bp2)))

        (xt/execute-tx node1 [[:put-docs :foo {:xt/id :foo}]])
        (tu/finish-block! node1)

        (t/testing "cached"
          (t/is (= -1 (BufferPoolKt/getLatestAvailableBlockIndex bp1)))
          (t/is (= -1 (BufferPoolKt/getLatestAvailableBlockIndex bp2))))

        (t/testing "live"
          (t/is (= 0 (BufferPoolKt/getLatestAvailableBlockIndex0 bp1)))
          (t/is (= 0 (BufferPoolKt/getLatestAvailableBlockIndex0 bp2))))

        (xt/execute-tx node1 [[:put-docs :foo {:xt/id :bar}]])
        (tu/finish-block! node1)

        (t/testing "live"
          (t/is (= 1 (BufferPoolKt/getLatestAvailableBlockIndex0 bp1)))
          (t/is (= 1 (BufferPoolKt/getLatestAvailableBlockIndex0 bp2))))))))

(defn byte-buffer->byte-array ^bytes [^java.nio.ByteBuffer buf]
  (let [copy (.duplicate buf)
        _ (.clear copy)
        arr (byte-array (.remaining copy))]
    (.get copy arr)
    arr))

(t/deftest network-counter-test
  (let [registry (SimpleMeterRegistry.)]
    (with-caches {}
      (with-open [bp (-> (Storage/remoteStorage (simulated-obj-store-factory))
                         (.open tu/*allocator* *mem-cache* *disk-cache* registry Storage/VERSION))]
        (let [k1 (util/->path "a")
              k2 (util/->path "b")
              ^ByteBuffer v1 (utf8-buf "aaa")
              ^ByteBuffer v2 (utf8-buf "bbb")]
          (t/testing "read side"
            (.putObject bp k1 v1)

            (t/is (= 0.0 (.count ^Counter (.counter (.find registry "buffer-pool.network.read")))))

            (t/is (java.util.Arrays/equals (byte-buffer->byte-array v1) (.getByteArray bp k1)))

            (t/is (= (double (.capacity v1)) (.count ^Counter (.counter (.find registry "buffer-pool.network.read")))))

            (t/is (java.util.Arrays/equals (byte-buffer->byte-array v1) (.getByteArray bp k1)))

            (t/is (= (double (.capacity v1)) (.count ^Counter (.counter (.find registry "buffer-pool.network.read")))) "counter should have not gone up"))

          (t/testing "write side"

            (t/is (= 3.0 (.count ^Counter (.counter (.find registry "buffer-pool.network.write")))))

            (.putObject bp k2 v2)

            (t/is (= 6.0 (.count ^Counter (.counter (.find registry "buffer-pool.network.write")))))))))))
