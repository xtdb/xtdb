(ns xtdb.object-store-test
  (:require [clojure.edn :as edn]
            [clojure.test :as t]
            [xtdb.buffer-pool :as bp]
            [xtdb.object-store :as os]
            [xtdb.util :as util])
  (:import [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]
           [java.nio.file Files Path]
           [java.nio.file.attribute FileAttribute]
           [xtdb.api.storage InMemoryObjectStore ObjectStore ObjectStore$Factory ObjectStoreSync]))

(defn- get-edn [^ObjectStore obj-store, ^Path k]
  (-> (let [^ByteBuffer buf (ObjectStoreSync/getObject obj-store k)]
        (edn/read-string (str (.decode StandardCharsets/UTF_8 buf))))
      (util/rethrowing-cause)))

(defn put-edn [^ObjectStore obj-store ^Path k obj]
  (let [^ByteBuffer buf (.encode StandardCharsets/UTF_8 (pr-str obj))]
    (ObjectStoreSync/putObject obj-store k buf)))

(defn generate-random-byte-buffer ^ByteBuffer [buffer-size]
  (let [random (java.util.Random.)
        byte-buffer (ByteBuffer/allocate buffer-size)]
    (loop [i 0]
      (if (< i buffer-size)
        (do
          (.put byte-buffer (byte (.nextInt random 128)))
          (recur (inc i)))
        (.flip byte-buffer)))))

(defmethod bp/->object-store-factory ::memory-object-store [_ _]
  (reify ObjectStore$Factory
    (openObjectStore [_ _storage-root _remotes]
      (InMemoryObjectStore.))))

(defn test-put-delete [^ObjectStore obj-store]
  (let [alice {:xt/id :alice, :name "Alice"}
        alice-key (util/->path "alice")]
    (put-edn obj-store alice-key alice)

    (t/is (= alice (get-edn obj-store alice-key)))

    (t/is (thrown? IllegalStateException (get-edn obj-store (util/->path "bob"))))

    (t/testing "doesn't override if present"
      (put-edn obj-store alice-key {:xt/id :alice, :name "Alice", :version 2})
      (t/is (= alice (get-edn obj-store alice-key))))

    (let [temp-path (ObjectStoreSync/getObject obj-store alice-key
                                 (doto (Files/createTempFile "alice" ".edn"
                                                             (make-array FileAttribute 0))
                                   Files/delete))]
      (t/is (= alice (read-string (Files/readString temp-path)))))

    (ObjectStoreSync/deleteIfExists obj-store alice-key)

    (t/is (thrown? IllegalStateException (get-edn obj-store alice-key)))))

(defn test-list-after [^ObjectStore obj-store]
  (put-edn obj-store (util/->path "bar/a") :a)
  (put-edn obj-store (util/->path "bar/b") :b)
  (put-edn obj-store (util/->path "bar/c") :c)
  (put-edn obj-store (util/->path "bar/d") :d)
  (put-edn obj-store (util/->path "foo/e") :e)

  (t/testing "lists only objects after the marker"
    (t/is (= #{(os/->StoredObject "bar/c" 2) (os/->StoredObject "bar/d" 2)}
             (set (.listAfter obj-store (util/->path "bar") (util/->path "bar/b"))))))

  (t/testing "marker before all objects returns all in dir"
    (t/is (= #{(os/->StoredObject "bar/a" 2) (os/->StoredObject "bar/b" 2)
               (os/->StoredObject "bar/c" 2) (os/->StoredObject "bar/d" 2)}
             (set (.listAfter obj-store (util/->path "bar") (util/->path "bar/"))))))

  (t/testing "marker at last object returns empty"
    (t/is (empty? (.listAfter obj-store (util/->path "bar") (util/->path "bar/d"))))))

