(ns crux.fixtures.kv
  (:require [clojure.java.io :as io]
            [crux.fixtures :as fix]
            [crux.system :as sys]))

(def ^:dynamic *kv-opts* {})

(defn with-kv-store* [f]
  (fix/with-tmp-dir "kv" [db-dir]
    (with-open [sys (-> (sys/prep-system
                         {:kv-store (merge (when-let [db-dir-suffix (:db-dir-suffix *kv-opts*)]
                                             {:db-dir (io/file db-dir db-dir-suffix)})
                                           *kv-opts*)})
                        (sys/start-system))]
      (f (:kv-store sys)))))

(defmacro with-kv-store [bindings & body]
  `(with-kv-store* (fn [~@bindings] ~@body)))

(defn with-each-kv-store* [f]
  (doseq [kv-opts [{:crux/module `crux.kv.memdb/->kv-store}
                   {:crux/module `crux.kv.rocksdb/->kv-store
                    :db-dir-suffix "rocksdb"}
                   {:crux/module `crux.kv.rocksdb.jnr/->kv-store
                    :db-dir-suffix "rocksdb-jnr"}
                   {:crux/module `crux.kv.lmdb/->kv-store
                    :db-dir-suffix "lmdb"
                    :env-mapsize 4096}
                   {:crux/module `crux.kv.lmdb.jnr/->kv-store
                    :db-dir-suffix "lmdb-jnr"
                    :env-mapsize 4096}]]
    (binding [*kv-opts* (merge *kv-opts* kv-opts)]
      (f))))

(defmacro with-each-kv-store [& body]
  `(with-each-kv-store* (fn [] ~@body)))
