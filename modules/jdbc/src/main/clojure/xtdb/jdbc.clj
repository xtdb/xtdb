(ns xtdb.jdbc
  (:require [clojure.java.data :as jd]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc.connection :as jdbcc]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc.result-set :as jdbcr]
            [xtdb.object-store :as os]
            [xtdb.util :as util])
  (:import [com.zaxxer.hikari HikariConfig HikariDataSource]
           java.nio.ByteBuffer
           (java.nio.file Files OpenOption Path)
           java.util.concurrent.CompletableFuture
           (java.util.function Function)
           java.util.function.Supplier
           xtdb.IObjectStore))

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

(defn ->sorted-path-list [object-list]
  (let [path-list (map #(util/->path (:key %)) object-list)]
    (sort path-list)))

(defn- get-object ^bytes [pool ^Path k]
  (or (-> (jdbc/execute-one! pool
                             ["SELECT blob FROM objects WHERE key = ?" (str k)])
          :objects/blob)
      (throw (os/obj-missing-exception k))))

(defrecord JDBCObjectStore [pool dialect]
  IObjectStore
  (getObject [_ k]
    (CompletableFuture/supplyAsync
     (reify Supplier
       (get [_]
         (ByteBuffer/wrap (get-object pool k))))))

  (getObjectRange [os k start len]
    (os/ensure-shared-range-oob-behaviour start len)
    (.thenApply
      (.getObject os k)
      (reify Function
        (apply [_ buf]
          (let [^ByteBuffer buf buf]
            (.slice buf (int start) (int (max 1 (min len (- (.remaining buf) start))))))))))

  (getObject [_ k out-path]
    (CompletableFuture/supplyAsync
     (reify Supplier
       (get [_]
         (Files/write out-path (get-object pool k)
                      ^"[Ljava.nio.file.OpenOption;"(into-array OpenOption (map util/standard-open-options util/write-truncate-open-opts)))))))

  (putObject [_ k buf]
    (CompletableFuture/runAsync
     (fn []
       (jdbc/execute! pool
                      [(upsert-object-sql dialect)
                       (str k)
                       (if (.hasArray buf)
                         (.array buf)
                         (let [ba (byte-array (.remaining buf))]
                           (.get buf ba)
                           ba))]))))

  (listObjects [_]
    (->> (jdbc/execute! pool
                        ["SELECT key FROM objects"]
                        {:builder-fn jdbcr/as-unqualified-kebab-maps})
         (->sorted-path-list)
         (vec)))

  (listObjects [_ obj-prefix]
    (let [prefix-depth (.getNameCount obj-prefix)]
      (->> (jdbc/execute! pool
                          ["SELECT key FROM objects WHERE key LIKE ?"
                           (str obj-prefix "%")]
                          {:builder-fn jdbcr/as-unqualified-kebab-maps})
           (->sorted-path-list)
           (take-while #(.startsWith ^Path % obj-prefix))
           (keep (fn [^Path path]
                   (when (> (.getNameCount path) prefix-depth)
                     (.subpath path 0 (inc prefix-depth)))))
           (distinct)
           (vec))))

  (deleteObject [_ k]
    (CompletableFuture/completedFuture
     (jdbc/execute! pool ["DELETE FROM objects WHERE key = ?" (str k)]))))

(defmethod ig/prep-key ::object-store [_ opts]
  (merge {:connection-pool (ig/ref ::connection-pool)
          :dialect (ig/ref ::dialect)}
         opts))

(defmethod ig/init-key ::object-store [_ {:keys [connection-pool dialect]}]
  (setup-object-store-schema! dialect connection-pool)
  (->JDBCObjectStore connection-pool dialect))

(derive ::object-store :xtdb/object-store)
