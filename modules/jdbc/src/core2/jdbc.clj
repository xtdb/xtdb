(ns core2.jdbc
  (:require [clojure.java.data :as jd]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [core2.object-store :as os]
            [core2.system :as sys]
            [core2.util :as util]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc.connection :as jdbcc]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc.result-set :as jdbcr])
  (:import [com.zaxxer.hikari HikariConfig HikariDataSource]
           core2.object_store.ObjectStore
           java.io.Closeable
           java.nio.ByteBuffer
           java.nio.file.Files
           java.util.concurrent.CompletableFuture
           java.util.function.Supplier))

(defprotocol Dialect
  (db-type [dialect])
  (setup-object-store-schema! [dialect pool])
  (upsert-object-sql [dialect]))

(defrecord HikariConnectionPool [^HikariDataSource pool dialect]
  Closeable
  (close [_]
    (util/try-close pool)))

(defn ->connection-pool {::sys/deps {:dialect nil}
                         ::sys/args {:pool-opts {:doc "Extra camelCase options to be set on HikariConfig"
                                                 :spec (s/map-of ::sys/keyword any?)}
                                     :db-spec {:doc "db-spec to be passed to next.jdbc"
                                               :spec (s/map-of ::sys/keyword any?)
                                               :required? true}}}
  [{:keys [pool-opts dialect db-spec]}]
  (let [jdbc-url (-> (jdbcc/jdbc-url (merge {:dbtype (name (db-type dialect))} db-spec))
                     ;; mssql doesn't like trailing '?'
                     (str/replace #"\?$" ""))
        pool-opts (merge pool-opts {:jdbcUrl jdbc-url})
        pool (HikariDataSource. (jd/to-java HikariConfig pool-opts))]

    (->HikariConnectionPool pool dialect)))

(defn- get-object ^bytes [pool k]
  (or (-> (jdbc/execute-one! pool
                             ["SELECT blob FROM objects WHERE key = ?" k])
          :objects/blob)
      (throw (os/obj-missing-exception k))))

(defrecord JDBCObjectStore [pool dialect]
  ObjectStore
  (getObject [_ k]
    (CompletableFuture/supplyAsync
     (reify Supplier
       (get [_]
         (ByteBuffer/wrap (get-object pool k))))))

  (getObject [_ k out-path]
    (CompletableFuture/supplyAsync
     (reify Supplier
       (get [_]
         (Files/write out-path (get-object pool k)
                      ^"[Ljava.nio.file.OpenOption;" util/write-new-file-opts)))))

  (putObject [_ k buf]
    (CompletableFuture/runAsync
     (fn []
       (jdbc/execute! pool
                      [(upsert-object-sql dialect)
                       k
                       (if (.hasArray buf)
                         (.array buf)
                         (let [ba (byte-array (.remaining buf))]
                           (.get buf ba)
                           ba))]))))

  (listObjects [_]
    (->> (jdbc/execute! pool
                        ["SELECT key FROM objects ORDER BY key"]
                        {:builder-fn jdbcr/as-unqualified-kebab-maps})
         (mapv :key)))

  (listObjects [_ obj-prefix]
    (->> (jdbc/execute! pool
                        ["SELECT key FROM objects WHERE key LIKE ? ORDER BY key"
                         (str obj-prefix "%")]
                        {:builder-fn jdbcr/as-unqualified-kebab-maps})
         (mapv :key)))

  (deleteObject [_ k]
    (CompletableFuture/completedFuture
     (jdbc/execute! pool ["DELETE FROM objects WHERE key = ?" k]))))

(defn ->object-store {::sys/deps {:connection-pool `->connection-pool}}
  [{{:keys [pool dialect]} :connection-pool}]
  (setup-object-store-schema! dialect pool)
  (->JDBCObjectStore pool dialect))
