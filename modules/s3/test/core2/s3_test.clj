(ns core2.s3-test
  (:require [clojure.test :as t]
            [core2.s3 :as s3]
            [core2.system :as sys])
  (:import core2.object_store.ObjectStore
           java.nio.ByteBuffer
           java.nio.file.attribute.FileAttribute
           java.nio.file.Files
           java.util.UUID
           software.amazon.awssdk.services.s3.model.NoSuchKeyException))

(def bucket
  (or (System/getProperty "core2.s3-test.bucket")
      #_"jms-crux-test"))

(def ^:dynamic ^ObjectStore *obj-store*)

(t/use-fixtures :each
  (fn [f]
    (if-not bucket
      (t/is true)

      (with-open [sys (-> (sys/prep-system {::obj-store {:core2/module `s3/->object-store
                                                         :bucket bucket
                                                         :prefix (str "core2.s3-test." (UUID/randomUUID))}})
                          (sys/start-system))]
        (binding [*obj-store* (::obj-store sys)]
          (f))))))

(defn get-object [k]
  ;; TODO maybe we could have getObject put into a buffer somewhere, save this tmp-file dance
  (let [tmp-file (Files/createTempFile "obj-" "" (make-array FileAttribute 0))]
    (Files/delete tmp-file)
    @(.getObject *obj-store* (name k) tmp-file)
    (read-string (String. (Files/readAllBytes tmp-file)))))

(defn put-object [k obj]
  @(.putObject *obj-store* (name k) (ByteBuffer/wrap (.getBytes (pr-str obj)))))

(t/deftest test-s3-obj-store
  (let [alice {:_id :alice, :name "Alice"}]
    (put-object :alice alice)

    (t/is (= alice (get-object :alice)))

    ;; TODO we should probably have this throw a more generic object-store error, or return nil
    (t/is (thrown? NoSuchKeyException
                   (try
                     (get-object :bob)
                     (catch Exception e
                       (throw (.getCause e))))))

    (let [alice-v2 {:_id :alice, :name "Alice", :version 2}]
      (put-object :alice alice-v2)
      (t/is (= alice-v2 (get-object :alice))))

    @(.deleteObject *obj-store* (name :alice))

    (t/is (thrown? NoSuchKeyException
                   (try
                     (get-object :alice)
                     (catch Exception e
                       (throw (.getCause e))))))))
