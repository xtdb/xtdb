(ns xtdb.multi-tenant
  (:require [xtdb.api :as xt]
            [xtdb.node :as xtn]
            [xtdb.util :as util]
            [next.jdbc :as jdbc])
  (:import (java.nio.file Path Files StandardOpenOption OpenOption)
           java.nio.file.attribute.FileAttribute
           (java.nio ByteBuffer)))

(defn tmp-db [user]
  (let [path (.resolve (util/tmp-dir "xtdb-multi-tenant") (str user ".db"))]
    (Files/createFile path (make-array FileAttribute 0))))

(def opts (doto (make-array OpenOption 2)
            (aset 0 StandardOpenOption/CREATE)
            (aset 1 StandardOpenOption/TRUNCATE_EXISTING)))

(defn spit-db [^Path path ^ByteBuffer bb]
  (Files/write path (.array bb) opts))

(defn slurp-db [path]
  (ByteBuffer/wrap (Files/readAllBytes path)))

(defn create-tenant [node user]
  (let [path (tmp-db user)
        _datasource (jdbc/get-datasource {:dbtype "sqlite"
                                          :dbname (str path)})]
    (xt/submit-tx node [[:put-docs :dbs {:xt/id user
                                         :db (slurp-db path)}]])))

(def multi-tenant-fn '(fn [user tx-ops]
                        (let [bb (:db (first (q "SELECT db FROM dbs WHERE _id = ?" {:args [user]})))
                              path (tmp-db user)
                              _ (spit-db path bb)
                              ds (jdbc/get-datasource {:dbtype "sqlite"
                                                       :dbname (str path)})]
                          (doseq [tx-op tx-ops]
                            (jdbc/execute! ds [tx-op]))
                          [[:put-docs :dbs {:xt/id user
                                            :db (slurp-db path)}]])))


(defn submit-tx [node user tx-ops]
  (xt/submit-tx node [[:call :multi-tenant-ops user tx-ops]]))

(defn q [node user query]
  (let [bb (:db (first (xt/q node "SELECT db FROM dbs WHERE _id = ?" {:args [user]})))
        path (tmp-db user)
        _ (spit-db path bb)
        ds (jdbc/get-datasource {:dbtype "sqlite"
                                 :dbname (str path)})]
    (jdbc/execute! ds [query])))

(comment
  (def node (xtn/start-node {:server {:port 0}}))
  (.close node)

  (xt/submit-tx node [[:put-fn :multi-tenant-ops multi-tenant-fn]])

  (create-tenant node "alice")

  (submit-tx node "alice" ["CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)"
                           "INSERT INTO people (id, name) VALUES (1, 'turing')"
                           "INSERT INTO people (id, name) VALUES (2, 'lovelace')"
                           ])

  (q node "alice" "SELECT * FROM foo"))
