(ns xtdb.object-store-test
  (:require [clojure.edn :as edn]
            [clojure.test :as t]
            [xtdb.object-store :as os]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import xtdb.object_store.ObjectStore
           java.io.Closeable
           java.nio.ByteBuffer
           java.nio.charset.StandardCharsets
           java.nio.file.attribute.FileAttribute
           java.nio.file.Files
           (clojure.lang IDeref)))

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

(defn- in-memory ^Closeable []
  (->> (ig/prep-key ::os/memory-object-store {})
       (ig/init-key ::os/memory-object-store)))

(defn- fs ^Closeable [path]
  (->> (ig/prep-key ::os/file-system-object-store {:root-path path, :pool-size 2})
       (ig/init-key ::os/file-system-object-store)))

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
  ([^ObjectStore obj-store]
   (test-list-objects obj-store 1000))
  ([^ObjectStore obj-store wait-time-ms]
   (put-edn obj-store "bar/alice" :alice)
   (put-edn obj-store "foo/alan" :alan)
   (put-edn obj-store "bar/bob" :bob)

  ;; Allow some time for files to get processed and added to the list
   (Thread/sleep wait-time-ms)

   (t/is (= ["bar/alice" "bar/bob" "foo/alan"] (.listObjects obj-store)))
   (t/is (= ["bar/alice" "bar/bob"] (.listObjects obj-store "bar")))))

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

;; ---
;; memory-object-store
;; ---

(t/deftest in-memory-put-delete-test
  (with-open [obj-store (in-memory)]
    (test-put-delete obj-store)))

(t/deftest in-memory-list-test
  (with-open [obj-store (in-memory)]
    (test-list-objects obj-store)))

(t/deftest in-memory-range-test
  (with-open [obj-store (in-memory)]
    (test-range obj-store)))

;; ---
;; file-system-object-store
;; ---

(t/deftest fs-put-delete-test
  (tu/with-tmp-dirs #{path}
    (with-open [obj-store (fs path)]
      (test-put-delete obj-store))))

(t/deftest fs-list-test
  (tu/with-tmp-dirs #{path}
    (with-open [obj-store (fs path)]
      (test-list-objects obj-store))))

(t/deftest fs-range-test
  (tu/with-tmp-dirs #{path}
    (with-open [obj-store (fs path)]
      (test-range obj-store))))
