(ns xtdb.postgres
  "Clojure config surface for the Postgres external source — registers a
   `->remote-factory` method so a `:remotes` entry in node config can name a
   Postgres upstream (parallel to how xtdb.kafka registers a log cluster)."
  (:require [xtdb.remote :as remote])
  (:import [xtdb.postgres PostgresRemote$Factory]))

(defmethod remote/->remote-factory ::remote
  [_ {:keys [host port database username password] :or {port 5432}}]
  (PostgresRemote$Factory. host port database username password))
