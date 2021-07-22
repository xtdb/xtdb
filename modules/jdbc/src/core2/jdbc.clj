(ns core2.jdbc
  (:require [clojure.java.data :as jd]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [core2.object-store :as os]
            [core2.util :as util]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc.connection :as jdbcc]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc.result-set :as jdbcr]
            [integrant.core :as ig])
  (:import [com.zaxxer.hikari HikariConfig HikariDataSource]
           core2.object_store.ObjectStore
           java.nio.ByteBuffer
           java.nio.file.Files
           java.util.concurrent.CompletableFuture
           java.util.function.Supplier))

(defprotocol Dialect
  (db-type [dialect])
  (setup-object-store-schema! [dialect pool])
  (upsert-object-sql [dialect]))

(s/def ::dialect #(satisfies? Dialect %))
(s/def ::db-spec (s/map-of keyword? any?))
(s/def ::pool-opts (s/map-of keyword? any?))

(derive ::default-pool ::connection-pool)

(defmethod ig/prep-key ::default-pool [_ opts]
  (merge {:dialect (ig/ref ::dialect)}
         opts))

(defmethod ig/pre-init-spec ::default-pool [_]
  (s/keys :req-un [::dialect ::db-spec]
          :opt-un [::pool-opts]))

(defmethod ig/init-key ::default-pool [_ {:keys [pool-opts dialect db-spec]}]
  (let [jdbc-url (-> (jdbcc/jdbc-url (merge {:dbtype (name (db-type dialect))} db-spec))
                     ;; mssql doesn't like trailing '?'
                     (str/replace #"\?$" ""))
        pool-opts (merge pool-opts {:jdbcUrl jdbc-url})]
    (HikariDataSource. (jd/to-java HikariConfig pool-opts))))

(defmethod ig/halt-key! ::default-pool [_ pool]
  (util/try-close pool))

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

(defmethod ig/prep-key ::object-store [_ opts]
  (merge {:connection-pool (ig/ref ::connection-pool)
          :dialect (ig/ref ::dialect)}
         opts))

(defmethod ig/init-key ::object-store [_ {:keys [connection-pool dialect]}]
  (setup-object-store-schema! dialect connection-pool)
  (->JDBCObjectStore connection-pool dialect))
