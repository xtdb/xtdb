(ns xtdb.fixtures.kv
  (:require [clojure.java.io :as io]
            [xtdb.fixtures :as fix]
            [xtdb.system :as sys]))

(def ^:dynamic *kv-opts* {})

(defn with-kv-store* [f]
  (fix/with-tmp-dirs #{db-dir}
    (with-open [sys (-> (sys/prep-system
                         {:kv-store (merge (when-let [db-dir-suffix (:db-dir-suffix *kv-opts*)]
                                             {:db-dir (io/file db-dir db-dir-suffix)})
                                           *kv-opts*)})
                        (sys/start-system))]
      (f (:kv-store sys)))))

(defmacro with-kv-store [bindings & body]
  `(with-kv-store* (fn [~@bindings] ~@body)))

(def rocks-dep {:xt/module 'xtdb.rocksdb/->kv-store, :db-dir-suffix "rocksdb"})
(def lmdb-dep {:xt/module 'xtdb.lmdb/->kv-store, :db-dir-suffix "lmdb", :env-mapsize 4096})
(def memkv-dep {:xt/module 'xtdb.mem-kv/->kv-store})
(def mutablekv-dep {:xt/module 'xtdb.kv.mutable-kv/->mutable-kv-store})

(defn with-each-kv-store* [f]
  (doseq [kv-opts [memkv-dep
                   mutablekv-dep
                   rocks-dep
                   {:xt/module `xtdb.rocksdb.jnr/->kv-store
                    :db-dir-suffix "rocksdb-jnr"}
                   lmdb-dep
                   {:xt/module `xtdb.lmdb.jnr/->kv-store
                    :db-dir-suffix "lmdb-jnr"
                    :env-mapsize 4096}]]
    (binding [*kv-opts* (merge *kv-opts* kv-opts)]
      (f))))

(defmacro with-each-kv-store [& body]
  `(with-each-kv-store* (fn [] ~@body)))

(defn with-kv-store-opts* [kv-opts f]
  (fix/with-tmp-dirs #{db-dir}
    (letfn [(->kv-opts [module]
              (merge (when-let [db-dir-suffix (:db-dir-suffix kv-opts)]
                       {:db-dir (io/file db-dir db-dir-suffix module)})
                     kv-opts))]
      (fix/with-opts {:xt/tx-log {:kv-store (->kv-opts "tx-log")}
                      :xt/document-store {:kv-store (->kv-opts "doc-store")}
                      :xt/index-store {:kv-store (->kv-opts "index-store")}}
        f))))

(defmacro with-kv-store-opts [kv-dep & body]
  `(with-kv-store-opts* ~kv-dep (fn [] ~@body)))
