(ns core2.object-store-test
  (:require [clojure.test :as t]
            [core2.object-store :as os]
            [core2.test-util :as tu]
            [integrant.core :as ig])
  (:import core2.object_store.ObjectStore
           java.nio.ByteBuffer
           java.nio.charset.StandardCharsets
           java.nio.file.attribute.FileAttribute
           java.nio.file.Files
           java.util.concurrent.ExecutionException))

(defn- get-object [^ObjectStore obj-store, k]
  (try
    (let [^ByteBuffer buf @(.getObject obj-store (name k))]
      (read-string (str (.decode StandardCharsets/UTF_8 buf))))
    (catch ExecutionException e
      (throw (.getCause e)))))

(defn- put-object [^ObjectStore obj-store k obj]
  (let [^ByteBuffer buf (.encode StandardCharsets/UTF_8 (pr-str obj))]
    @(.putObject obj-store (name k) buf)))

(defn ^::os-test test-put-delete [^ObjectStore obj-store]
  (let [alice {:_id :alice, :name "Alice"}]
    (put-object obj-store :alice alice)

    (t/is (= alice (get-object obj-store :alice)))

    (t/is (thrown? IllegalStateException (get-object obj-store :bob)))

    (t/testing "doesn't override if present"
      (put-object obj-store :alice {:_id :alice, :name "Alice", :version 2})
      (t/is (= alice (get-object obj-store :alice))))

    (let [temp-path @(.getObject obj-store (name :alice)
                                 (doto (Files/createTempFile "alice" ".edn"
                                                             (make-array FileAttribute 0))
                                   Files/delete))]
      (t/is (= alice (read-string (Files/readString temp-path)))))

    @(.deleteObject obj-store (name :alice))

    (t/is (thrown? IllegalStateException (get-object obj-store :alice)))))

(defn ^::os-test test-list-objects [^ObjectStore obj-store]
  (put-object obj-store "alice" :alice)
  (put-object obj-store "alan" :alan)
  (put-object obj-store "bob" :bob)

  (t/is (= ["alan" "alice" "bob"] (.listObjects obj-store)))
  (t/is (= ["alan" "alice"] (.listObjects obj-store "al"))))

(def os-tests
  (->> (ns-interns (create-ns 'core2.object-store-test))
       (into {} (filter (comp ::os-test meta val)))))

(defmacro def-obj-store-tests [sym [binding] & body]
  `(do
     ~@(for [test-name (keys os-tests)]
         `(t/deftest ~(symbol (str sym "-" test-name))
            (let [~binding (get os-tests '~test-name)]
              ~@body)))
     '~sym))

(def-obj-store-tests in-mem [f]
  (f (ig/init-key ::os/memory-object-store {})))

(def-obj-store-tests fs [f]
  (tu/with-tmp-dirs #{os-path}
    (f (ig/init-key ::os/file-system-object-store {:root-path os-path, :pool-size 2}))))
