(ns xtdb.object-store-test
  (:require [clojure.edn :as edn]
            [clojure.test :as t]
            [xtdb.buffer-pool :as bp]
            [xtdb.object-store :as os]
            [xtdb.util :as util])
  (:import [java.lang AutoCloseable]
           [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]
           [java.nio.file Files Path]
           [java.nio.file.attribute FileAttribute]
           [java.util NavigableMap]
           [java.util.concurrent CompletableFuture ConcurrentSkipListMap]
           [xtdb.api.storage ObjectStore ObjectStore$Factory]))

(defn- get-edn [^ObjectStore obj-store, ^Path k]
  (-> (let [^ByteBuffer buf @(.getObject obj-store k)]
        (edn/read-string (str (.decode StandardCharsets/UTF_8 buf))))
      (util/rethrowing-cause)))

(defn put-edn [^ObjectStore obj-store ^Path k obj]
  (let [^ByteBuffer buf (.encode StandardCharsets/UTF_8 (pr-str obj))]
    @(.putObject obj-store k buf)))

(defn generate-random-byte-buffer ^ByteBuffer [buffer-size]
  (let [random (java.util.Random.)
        byte-buffer (ByteBuffer/allocate buffer-size)]
    (loop [i 0]
      (if (< i buffer-size)
        (do
          (.put byte-buffer (byte (.nextInt random 128)))
          (recur (inc i)))
        (.flip byte-buffer)))))

(deftype InMemoryObjectStore [^NavigableMap os]
  ObjectStore
  (getObject [_this k]
    (CompletableFuture/completedFuture
     (let [{:keys [^ByteBuffer buf]} (or (.get os k)
                                         (throw (os/obj-missing-exception k)))]
       (.slice buf))))

  (getObject [_this k out-path]
    (CompletableFuture/supplyAsync
     (fn []
       (let [{:keys [^ByteBuffer buf]} (or (.get os k)
                                           (throw (os/obj-missing-exception k)))]
         (with-open [ch (util/->file-channel out-path util/write-truncate-open-opts)]
           (.write ch buf)
           out-path)))))

  (putObject [_this k buf]
    (.putIfAbsent os k (.slice buf))
    (CompletableFuture/completedFuture nil))

  ;; these two should be returning StoredObjects, but for some reason this seems fine
  (listAllObjects [_this] (vec (.keySet os)))

  (listAllObjects [_ prefix]
    (->> (.tailMap os prefix)
         (take-while #(-> ^Path (key %) (.startsWith prefix)))
         (map key)))
  
  (deleteObject [_this k]
    (.remove os k)
    (CompletableFuture/completedFuture nil))

  AutoCloseable
  (close [_]
    (.clear os)))

(defmethod bp/->object-store-factory ::memory-object-store [_ _]
  (reify ObjectStore$Factory
    (openObjectStore [_]
      (->InMemoryObjectStore (ConcurrentSkipListMap.)))))

(defn test-put-delete [^ObjectStore obj-store]
  (let [alice {:xt/id :alice, :name "Alice"}
        alice-key (util/->path "alice")]
    (put-edn obj-store alice-key alice)

    (t/is (= alice (get-edn obj-store alice-key)))

    (t/is (thrown? IllegalStateException (get-edn obj-store (util/->path "bob"))))

    (t/testing "doesn't override if present"
      (put-edn obj-store alice-key {:xt/id :alice, :name "Alice", :version 2})
      (t/is (= alice (get-edn obj-store alice-key))))

    (let [temp-path @(.getObject obj-store alice-key
                                 (doto (Files/createTempFile "alice" ".edn"
                                                             (make-array FileAttribute 0))
                                   Files/delete))]
      (t/is (= alice (read-string (Files/readString temp-path)))))

    @(.deleteObject obj-store alice-key)

    (t/is (thrown? IllegalStateException (get-edn obj-store alice-key)))))

