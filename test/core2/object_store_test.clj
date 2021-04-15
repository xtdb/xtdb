(ns core2.object-store-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [core2.object-store :as os]
            [core2.test-util :as tu])
  (:import core2.object_store.ObjectStore
           java.nio.ByteBuffer
           java.nio.file.attribute.FileAttribute
           java.nio.file.Files
           java.util.concurrent.ExecutionException))

(defn- get-object [^ObjectStore obj-store, k]
  ;; TODO maybe we could have getObject put into a buffer somewhere, save this tmp-file dance
  (let [tmp-file (Files/createTempFile "obj-" "" (make-array FileAttribute 0))]
    (Files/delete tmp-file)
    (try
      @(.getObject obj-store (name k) tmp-file)
      (read-string (String. (Files/readAllBytes tmp-file)))
      (catch ExecutionException e
        (throw (.getCause e))))))

(defn- put-object [^ObjectStore obj-store k obj]
  @(.putObject obj-store (name k) (ByteBuffer/wrap (.getBytes (pr-str obj)))))

(defn ^::os-test test-put-delete [^ObjectStore obj-store]
  (let [alice {:_id :alice, :name "Alice"}]
    (put-object obj-store :alice alice)

    (t/is (= alice (get-object obj-store :alice)))

    (t/is (thrown? IllegalStateException (get-object obj-store :bob)))

    (t/testing "doesn't override if present"
      (put-object obj-store :alice {:_id :alice, :name "Alice", :version 2})
      (t/is (= alice (get-object obj-store :alice))))

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
  (f (os/->object-store {})))

(def-obj-store-tests fs [f]
  (tu/with-tmp-dirs #{os-path}
    (f (os/->file-system-object-store {:root-path os-path, :pool-size 2}))))
