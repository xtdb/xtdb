(ns xtdb.replay-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.db :as db]
            [xtdb.fixtures :as fix :refer [*api*]]
            [xtdb.rocksdb :as rocks]))

(def ^:private ^:dynamic *event-log-dir*)

(defn- with-cluster* [f]
  (fix/with-tmp-dir "event-log-dir" [event-log-dir]
    (binding [*event-log-dir* event-log-dir]
      (f))))

(defmacro with-cluster [& body]
  `(with-cluster* (fn [] ~@body)))

(defn- with-cluster-node* [f]
  (fix/with-tmp-dir "db-dir" [db-dir]
    (with-open [node (xt/start-node {:xtdb/document-store {:kv-store {:xtdb/module `rocks/->kv-store,
                                                                        :db-dir (io/file *event-log-dir* "doc-store")}}
                                       :xtdb/tx-log {:kv-store {:xtdb/module `rocks/->kv-store,
                                                                :db-dir (io/file *event-log-dir* "tx-log")}}
                                       :xtdb/index-store {:kv-store {:xtdb/module `rocks/->kv-store,
                                                                     :db-dir db-dir}}})]
      (binding [*api* node]
        (xt/sync node)
        (f)))))

(defmacro with-cluster-node [& body]
  `(with-cluster-node* (fn [] ~@body)))

(t/deftest drop-db
  (with-cluster
    (with-cluster-node
      (fix/submit+await-tx [[:xt/put {:xt/id :hello}]]))

    (with-cluster-node
      (t/is (= {:xt/tx-id 0}
               (xt/latest-submitted-tx *api*)))
      (t/is (= {:xt/id :hello}
               (xt/entity (xt/db *api*) :hello))))))

(t/deftest test-more-txs
  (let [n 1000]
    (with-cluster
      (with-cluster-node
        (dotimes [x n]
          (fix/submit+await-tx  [[:xt/put {:xt/id (str "id-" x)}]])))

      (with-cluster-node
        (t/is (= {:xt/tx-id (dec n)}
                 (xt/latest-submitted-tx *api*)))
        (t/is (= n
                 (count (xt/q (xt/db *api*) '{:find [?e]
                                                  :where [[?e :xt/id]]}))))))))

(t/deftest replaces-tx-fn-arg-docs
  (with-cluster
    (with-cluster-node
      (fix/submit+await-tx [[:xt/put {:xt/id :put-ivan
                                           :crux.db/fn '(fn [ctx doc]
                                                          [[:xt/put (assoc doc :xt/id :ivan)]])}]])

      (fix/submit+await-tx [[:xt/fn :put-ivan {:name "Ivan"}]])

      (t/is (= {:xt/id :ivan, :name "Ivan"}
               (xt/entity (xt/db *api*) :ivan))))

    (with-cluster-node
      (t/is (= {:xt/id :ivan, :name "Ivan"}
               (xt/entity (xt/db *api*) :ivan)))))

  (t/testing "replaces fn with no args"
    (with-cluster
      (with-cluster-node
        (fix/submit+await-tx [[:xt/put {:xt/id :no-args
                                             :crux.db/fn '(fn [ctx]
                                                            [[:xt/put {:xt/id :no-fn-args-doc}]])}]])
        (fix/submit+await-tx [[:xt/fn :no-args]])

        (t/is (= {:xt/id :no-fn-args-doc}
                 (xt/entity (xt/db *api*) :no-fn-args-doc))))

      (with-cluster-node
        (t/is (= {:xt/id :no-fn-args-doc}
                 (xt/entity (xt/db *api*) :no-fn-args-doc))))))

  (t/testing "nested tx-fn"
    (with-cluster
      (with-cluster-node
        (fix/submit+await-tx [[:xt/put {:xt/id :put-ivan
                                             :crux.db/fn '(fn [ctx doc]
                                                            [[:xt/put (assoc doc :xt/id :ivan)]])}]])

        (fix/submit+await-tx [[:xt/put {:xt/id :put-bob-and-ivan
                                             :crux.db/fn '(fn [ctx bob ivan]
                                                            [[:xt/put (assoc bob :xt/id :bob)]
                                                             [:xt/fn :put-ivan ivan]])}]])

        (fix/submit+await-tx [[:xt/fn :put-bob-and-ivan {:name "Bob"} {:name "Ivan2"}]])

        (t/is (= {:xt/id :ivan, :name "Ivan2"}
                 (xt/entity (xt/db *api*) :ivan)))

        (t/is (= {:xt/id :bob, :name "Bob"}
                 (xt/entity (xt/db *api*) :bob))))

      (with-cluster-node
        (t/is (= {:xt/id :ivan, :name "Ivan2"}
                 (xt/entity (xt/db *api*) :ivan)))

        (t/is (= {:xt/id :bob, :name "Bob"}
                 (xt/entity (xt/db *api*) :bob))))))

  (t/testing "failed tx-fn"
    (with-cluster
      (with-cluster-node
        (fix/submit+await-tx [[:xt/fn :put-petr {:name "Petr"}]])

        (t/is (nil? (xt/entity (xt/db *api*) :petr)))

        (fix/submit+await-tx [[:xt/put {:xt/id :foo}]])

        (t/is (= {:xt/id :foo}
                 (xt/entity (xt/db *api*) :foo))))

      (with-cluster-node
        (t/is (nil? (xt/entity (xt/db *api*) :petr)))
        (t/is (= {:xt/id :foo}
                 (xt/entity (xt/db *api*) :foo)))))))
