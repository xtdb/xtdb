(ns xtdb.object-store-test
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.test :as t]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.object-store :as os]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import java.io.Closeable
           java.nio.ByteBuffer
           java.nio.charset.StandardCharsets
           java.nio.file.attribute.FileAttribute
           java.nio.file.Files
           java.nio.file.OpenOption
           [java.util.concurrent CompletableFuture ConcurrentSkipListMap]
           [java.util.function Supplier]
           java.util.NavigableMap
           xtdb.object_store.ObjectStore))

(defn- get-edn [^ObjectStore obj-store, k]
  (-> (let [^ByteBuffer buf @(.getObject obj-store (name k))]
        (edn/read-string (str (.decode StandardCharsets/UTF_8 buf))))
      (util/rethrowing-cause)))

(defn put-edn [^ObjectStore obj-store k obj]
  (let [^ByteBuffer buf (.encode StandardCharsets/UTF_8 (pr-str obj))]
    @(.putObject obj-store (name k) buf)))

(defn put-bytes [^ObjectStore obj-store k bytes]
  @(.putObject obj-store k (ByteBuffer/wrap bytes)))

(defn byte-buf->bytes [^ByteBuffer buf]
  (let [arr (byte-array (.remaining buf))
        pos (.position buf)]
    (.get buf arr)
    (.position buf pos)
    arr))

(defn get-bytes [^ObjectStore obj-store k start len]
  (byte-buf->bytes @(.getObjectRange obj-store k start len)))

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

  (listObjects [_this]
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

(defmethod ig/init-key ::memory-object-store [_ _]
  (->InMemoryObjectStore (ConcurrentSkipListMap.)))

(defmethod ig/halt-key! ::memory-object-store [_ ^InMemoryObjectStore os]
  (.close os))

(derive ::memory-object-store :xtdb/object-store)

(defn test-put-delete [^ObjectStore obj-store]
  (let [alice {:xt/id :alice, :name "Alice"}]
    (put-edn obj-store :alice alice)

    (t/is (= alice (get-edn obj-store :alice)))

    (t/is (thrown? IllegalStateException (get-edn obj-store :bob)))

    (t/testing "doesn't override if present"
      (put-edn obj-store :alice {:xt/id :alice, :name "Alice", :version 2})
      (t/is (= alice (get-edn obj-store :alice))))

    (let [temp-path @(.getObject obj-store (name :alice)
                                 (doto (Files/createTempFile "alice" ".edn"
                                                             (make-array FileAttribute 0))
                                   Files/delete))]
      (t/is (= alice (read-string (Files/readString temp-path)))))

    @(.deleteObject obj-store (name :alice))

    (t/is (thrown? IllegalStateException (get-edn obj-store :alice)))))

(defn test-list-objects
  [^ObjectStore obj-store]
  (put-edn obj-store "bar/alice" :alice)
  (put-edn obj-store "foo/alan" :alan)
  (put-edn obj-store "bar/bob" :bob)

  (t/is (= ["bar/alice" "bar/bob" "foo/alan"] (.listObjects obj-store)))
  (t/is (= ["bar/alice" "bar/bob"] (.listObjects obj-store "bar")))

     ;; Delete an object
  @(.deleteObject obj-store "bar/alice")

  (t/is (= ["bar/bob"] (.listObjects obj-store "bar"))))

(defn test-range [^ObjectStore obj-store]

  (put-bytes obj-store "digits" (byte-array (range 10)))

  (t/is (= [0] (vec (get-bytes obj-store "digits" 0 1))))
  (t/is (= [4 5 6 7] (vec (get-bytes obj-store "digits" 4 4))))
  (t/is (= [9] (vec (get-bytes obj-store "digits" 9 1))))

  (t/is (= (vec (range 10)) (vec (get-bytes obj-store "digits" 0 20))))
  (t/is (= [9] (vec (get-bytes obj-store "digits" 9 2))))

  ;; OOB behaviour is 'any exception', just whatever is thrown by impl
  (->> "len 0 is not permitted due to potential ambiguities that arise in the bounds behaviour for len 0 across impls"
       (t/is (thrown? Exception (get-bytes obj-store "digits" 0 0))))

  (->> "OOB for negative index"
       (t/is (thrown? Exception (get-bytes obj-store "digits" -1 3))))

  (->> "OOB for negative len"
       (t/is (thrown? Exception (get-bytes obj-store "digits" 0 -1))))
  
  (->> "IllegalStateException thrown if object does not exist"
       (t/is (thrown? IllegalStateException (get-bytes obj-store "does-not-exist" 0 1)))))
