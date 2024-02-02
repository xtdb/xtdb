(ns xtdb.object-store-test
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.test :as t]
            [xtdb.buffer-pool :as bp]
            [xtdb.object-store :as os]
            [xtdb.util :as util])
  (:import [java.io Closeable]
           [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]
           [java.nio.file Files Path]
           [java.nio.file.attribute FileAttribute]
           [java.util NavigableMap]
           [java.util.concurrent CompletableFuture ConcurrentSkipListMap]
           [java.util.function Supplier]
           [xtdb.api.storage ObjectStore ObjectStoreFactory]))

(defn- get-edn [^ObjectStore obj-store, ^Path k]
  (-> (let [^ByteBuffer buf @(.getObject obj-store k)]
        (edn/read-string (str (.decode StandardCharsets/UTF_8 buf))))
      (util/rethrowing-cause)))

(defn put-edn [^ObjectStore obj-store ^Path k obj]
  (let [^ByteBuffer buf (.encode StandardCharsets/UTF_8 (pr-str obj))]
    @(.putObject obj-store k buf)))

(defn put-bytes [^ObjectStore obj-store ^Path k bytes]
  @(.putObject obj-store k (ByteBuffer/wrap bytes)))

(defn byte-buf->bytes [^ByteBuffer buf]
  (let [arr (byte-array (.remaining buf))
        pos (.position buf)]
    (.get buf arr)
    (.position buf pos)
    arr))

(defn get-bytes [^ObjectStore obj-store ^Path k start len]
  (byte-buf->bytes @(.getObjectRange obj-store k start len)))

;; Generates a byte buffer of random characters
(defn generate-random-byte-buffer [buffer-size]
  (let [random         (java.util.Random.)
        byte-buffer    (ByteBuffer/allocate buffer-size)]
    (loop [i 0]
      (if (< i buffer-size)
        (do
          (.put byte-buffer (byte (.nextInt random 128)))
          (recur (inc i)))
        byte-buffer))))

(deftype InMemoryObjectStore [^NavigableMap os]
  ObjectStore
  (getObject [_this k]
    (CompletableFuture/completedFuture
     (let [^ByteBuffer buf (or (.get os k)
                               (throw (os/obj-missing-exception k)))]
       (.slice buf))))

  (getObject [_this k out-path]
    (CompletableFuture/supplyAsync
     (reify Supplier
       (get [_]
         (let [^ByteBuffer buf (or (.get os k)
                                   (throw (os/obj-missing-exception k)))]
           (with-open [ch (util/->file-channel out-path util/write-truncate-open-opts)]
             (.write ch buf)
             out-path))))))

  (getObjectRange [_this k start len]
    (os/ensure-shared-range-oob-behaviour start len)
    (CompletableFuture/completedFuture
      (let [^ByteBuffer buf (or (.get os k) (throw (os/obj-missing-exception k)))
            new-pos (+ (.position buf) (int start))]
        (.slice buf new-pos (int (max 1 (min (- (.remaining buf) new-pos) len)))))))

  (putObject [_this k buf]
    (.putIfAbsent os k (.slice buf))
    (CompletableFuture/completedFuture nil))

  (listAllObjects [_this]
    (vec (.keySet os)))

  (listObjects [_this prefix]
    (->> (.keySet (.tailMap os prefix))
         (into [] (take-while #(str/starts-with? % prefix)))))

  (deleteObject [_this k]
    (.remove os k)
    (CompletableFuture/completedFuture nil))

  Closeable
  (close [_]
    (.clear os)))

(defmethod bp/->object-store-factory ::memory-object-store [_ _]
  (reify ObjectStoreFactory
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

(defn test-list-objects
  [^ObjectStore obj-store]
  (put-edn obj-store (util/->path "bar/alice") :alice)
  (put-edn obj-store (util/->path "foo/alan") :alan)
  (put-edn obj-store (util/->path "bar/bob") :bob)
  (put-edn obj-store (util/->path "bar/baz/dan") :dan)
  (put-edn obj-store (util/->path "bar/baza/james") :james)

  (t/is (= (mapv util/->path ["bar/alice" "bar/baz/dan" "bar/baza/james" "bar/bob" "foo/alan"])
           (.listAllObjects obj-store)))

  (t/is (= (mapv util/->path ["foo/alan"])
           (.listObjects obj-store (util/->path "foo"))))

  (t/testing "call listObjects with a prefix ended with a slash - should work the same"
    (t/is (= (mapv util/->path ["foo/alan"])
             (.listObjects obj-store (util/->path "foo/")))))

  (t/testing "calling listObjects with prefix on directory with subdirectories - should only return top level keys"
    (t/is (= (mapv util/->path ["bar/alice" "bar/baz" "bar/baza" "bar/bob"])
             (.listObjects obj-store (util/->path "bar")))))

  (t/testing "calling listObjects with prefix with common prefix - should only return that which is a complete match against a directory "
    (t/is (= (mapv util/->path ["bar/baz/dan"])
             (.listObjects obj-store (util/->path "bar/baz")))))

     ;; Delete an object
  @(.deleteObject obj-store (util/->path "bar/alice"))

  (t/is (= (mapv util/->path ["bar/baz" "bar/baza" "bar/bob"])
           (.listObjects obj-store (util/->path "bar")))))

(defn test-range [^ObjectStore obj-store]
  (let [digits-key (util/->path "digits")]
    (put-bytes obj-store digits-key (byte-array (range 10)))

    (t/is (= [0] (vec (get-bytes obj-store digits-key 0 1))))
    (t/is (= [4 5 6 7] (vec (get-bytes obj-store digits-key 4 4))))
    (t/is (= [9] (vec (get-bytes obj-store digits-key 9 1))))

    (t/is (= (vec (range 10)) (vec (get-bytes obj-store digits-key 0 20))))
    (t/is (= [9] (vec (get-bytes obj-store digits-key 9 2))))

    ;; OOB behaviour is 'any exception', just whatever is thrown by impl
    (->> "len 0 is not permitted due to potential ambiguities that arise in the bounds behaviour for len 0 across impls"
         (t/is (thrown? Exception (get-bytes obj-store digits-key 0 0))))

    (->> "OOB for negative index"
         (t/is (thrown? Exception (get-bytes obj-store digits-key -1 3))))

    (->> "OOB for negative len"
         (t/is (thrown? Exception (get-bytes obj-store digits-key 0 -1))))

  (->> "OOB for negative len"
       (t/is (thrown? Exception (get-bytes obj-store "digits" 0 -1))))

  ;; Causes issues due to different error types being thrown
  ;; (->> "IllegalStateException thrown if object does not exist"
  ;;      (t/is (thrown? IllegalStateException (get-bytes obj-store "does-not-exist" 0 1))))

  ))
