(ns xtdb.buffer-pool-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [integrant.core :as ig]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]
            [xtdb.object-store :as os]
            [xtdb.test-util :as tu]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import (io.micrometer.core.instrument.simple SimpleMeterRegistry)
           (java.io File)
           (java.nio ByteBuffer)
           [java.nio.charset StandardCharsets]
           (java.nio.file Files Path)
           (java.nio.file.attribute FileAttribute)
           (org.apache.arrow.vector.types.pojo Schema)
           (xtdb.api.storage ObjectStore$Factory Storage)
           (xtdb.api.storage SimulatedObjectStore StoreOperation)
           xtdb.arrow.Relation
           (xtdb.buffer_pool LocalBufferPool MemoryBufferPool RemoteBufferPool)
           xtdb.BufferPool
           xtdb.cache.DiskCache))

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
                                                                                                      :local-disk-cache path}]
                                                                                   :compactor {:threads 0}}))]
                                                        (xt/submit-tx node [[:put-docs :foo {:xt/id :foo}]])

                                                        (t/is (= [{:xt/id :foo}]
                                                                 (xt/q node '(from :foo [xt/id]))))

                                                        (tu/finish-block! node)

                                                        (let [^RemoteBufferPool buffer-pool (val (first (ig/find-derived (:system node) :xtdb/buffer-pool)))
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
    (openObjectStore [_]
      (SimulatedObjectStore.))))

(defn remote-test-buffer-pool ^xtdb.BufferPool []
  (-> (Storage/remoteStorage (simulated-obj-store-factory) (create-tmp-dir))
      (.open tu/*allocator* (SimpleMeterRegistry.))))

(defn get-remote-calls [^RemoteBufferPool test-bp]
  (.getCalls ^SimulatedObjectStore (.getObjectStore test-bp)))

(t/deftest below-min-size-put-test
  (with-open [bp (remote-test-buffer-pool)]
    (t/testing "if <= min part size, putObject is used"
      (.putObject bp (util/->path "min-part-put") (utf8-buf "12"))
      (t/is (= [StoreOperation/PUT] (get-remote-calls bp)))
      (test-get-object bp (util/->path "min-part-put") (utf8-buf "12")))))

(defn file-info [^Path dir]
  (let [files (filter #(.isFile ^File %) (file-seq (.toFile dir)))]
    {:file-count (count files) :file-names (set (map #(.getName ^File %) files))}))

(defn insert-utf8-to-local-cache [^BufferPool bp k len]
  (.putObject bp k (utf8-buf (apply str (repeat len "a"))))
  ;; Add to local disk cache
  (.getByteArray bp k))

(t/deftest local-disk-cache-max-size
  (util/with-tmp-dirs #{local-disk-cache}
    (with-open [bp (-> (Storage/remoteStorage (simulated-obj-store-factory) local-disk-cache)
                       (.maxDiskCacheBytes 10)
                       (.maxCacheBytes 12)
                       (.open tu/*allocator* (SimpleMeterRegistry.)))]
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
  (let [obj-store-factory (simulated-obj-store-factory)
        path-a (util/->path "a")
        path-b (util/->path "b")]
    (util/with-tmp-dirs #{local-disk-cache}
      ;; Writing files to buffer pool & local-disk-cache
      (with-open [bp (-> (Storage/remoteStorage obj-store-factory local-disk-cache)
                         (.maxDiskCacheBytes 10)
                         (.maxCacheBytes 12)
                         (.open tu/*allocator* (SimpleMeterRegistry.)))]
        (insert-utf8-to-local-cache bp path-a 4)
        (insert-utf8-to-local-cache bp path-b 4)
        (t/is (= {:file-count 2 :file-names #{"a" "b"}} (file-info local-disk-cache))))

      ;; Starting a new buffer pool - should load buffers correctly from disk (can be sure its grabbed from disk since using a memory cache and memory object store)
      (with-open [bp (-> (Storage/remoteStorage obj-store-factory local-disk-cache)
                         (.maxDiskCacheBytes 10)
                         (.maxCacheBytes 12)
                         (.open tu/*allocator* (SimpleMeterRegistry.)))]
        (t/is (= 0 (util/compare-nio-buffers-unsigned (utf8-buf "aaaa") (ByteBuffer/wrap (.getByteArray bp path-a)))))

        (t/is (= 0 (util/compare-nio-buffers-unsigned (utf8-buf "aaaa") (ByteBuffer/wrap (.getByteArray bp path-b)))))))))

(t/deftest local-buffer-pool
  (tu/with-tmp-dirs #{tmp-dir}
    (with-open [bp (LocalBufferPool. tu/*allocator* (Storage/localStorage tmp-dir) (SimpleMeterRegistry.))]
      (t/testing "empty buffer pool"
        (t/is (= [] (.listAllObjects bp)))
        (t/is (= [] (.listAllObjects bp (.toPath (io/file "foo")))))))))

(t/deftest dont-list-temporary-objects-3544
  (tu/with-tmp-dirs #{tmp-dir}
    (let [schema (Schema. [(types/col-type->field "a" :i32)])]
      (with-open [bp (LocalBufferPool. tu/*allocator* (Storage/localStorage tmp-dir) (SimpleMeterRegistry.))
                  rel (Relation. tu/*allocator* schema)
                  _arrow-writer (.openArrowWriter bp (.toPath (io/file "foo")) rel)]
        (t/is (= [] (.listAllObjects bp)))))))

(defn fetch-buffer-pool-from-node [node]
  (util/component node :xtdb/buffer-pool))

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
  (with-open [bp (MemoryBufferPool. tu/*allocator* (SimpleMeterRegistry.))]
    (test-list-objects bp)))

(t/deftest test-local-list-objs
  (tu/with-tmp-dirs #{tmp-dir}
    (with-open [bp (LocalBufferPool. tu/*allocator* (Storage/localStorage tmp-dir) (SimpleMeterRegistry.))]
      (test-list-objects bp))))
