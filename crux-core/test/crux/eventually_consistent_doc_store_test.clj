(ns crux.eventually-consistent-doc-store-test
  (:require [crux.api :as crux]
            [clojure.test :as t]
            [crux.fixtures :as fix :refer [*api*]]
            [crux.db :as db]
            [crux.document :as doc])
  (:import (java.time Instant Duration)))

(defrecord ECDocStore [!docs]
  db/DocumentStore
  (submit-docs [_ new-docs]
    (swap! !docs (fn [docs]
                   (-> (merge docs new-docs)
                       (vary-meta update ::insert-times
                                  merge (zipmap (keys new-docs) (repeat (Instant/now))))))))

  (-fetch-docs [_ ids]
    (let [docs @!docs
          insert-times (::insert-times (meta docs))
          now (Instant/now)]
      (select-keys docs (->> ids
                             (filter (fn [id]
                                       (when-let [^Instant insert-time (get insert-times id)]
                                         (.isBefore (.plus insert-time (Duration/ofMillis 250)) now)))))))))

(t/use-fixtures :each
  (fix/with-opts {:crux/document-store {:crux/module (fn [_]
                                                       (->ECDocStore (atom {})))}})
  fix/with-node)

(t/deftest test-eventually-consistent-doc-store
  (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo}]])
  (t/is (= (doc/->Document {:crux.db/id :foo})
           (crux/entity (crux/db *api*) :foo))))
