(ns crux.object-store-test
  (:require [clojure.test :as t]
            [crux.db :as db]
            [crux.object-store :as os])
  (:import java.io.Closeable))

(defrecord InMemDocumentStore [docs]
  Closeable
  (close [_])

  db/DocumentStore
  (fetch-docs [this ids]
    (into {}
          (for [id ids]
            [id (get @docs id)])))

  (submit-docs [this id-and-docs]
    (doseq [[content-hash doc] id-and-docs]
      (swap! docs assoc content-hash doc))))

(t/deftest test-wrap-document-object-store
  (let [ds (InMemDocumentStore. (atom {}))
        os (os/->DocumentStoreBackedObjectStore ds)]
    (db/submit-docs ds [[:a {:id :foo}]])
    (t/is (= {:id :foo} (db/get-single-object os nil :a)))
    (t/is (= {:a {:id :foo}} (db/get-objects os nil [:a])))
    (t/is (= #{:b} (db/missing-keys os nil [:a :b])))))
