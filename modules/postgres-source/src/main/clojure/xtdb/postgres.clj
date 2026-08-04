(ns xtdb.postgres
  "Clojure config surface for the Postgres external source — registers a
   `->remote-factory` method so a `:remotes` entry in node config can name a
   Postgres upstream (parallel to how xtdb.kafka registers a log cluster)."
  (:require [clojure.java.io :as io]
            [xtdb.remote :as remote])
  (:import (xtdb.postgres FailoverSlot PostgresRemote$Factory)))

(defmethod remote/->remote-factory ::remote
  [_ {:keys [host port database username password] :or {port 5432}}]
  (PostgresRemote$Factory. host port database username password))

(defn enable-slot-failover!
  "Reachable from `xtdb.main` via `requiring-resolve`; the work is all in `FailoverSlot`."
  [config-file remote-alias slot-name]
  (println (FailoverSlot/report (FailoverSlot/enable (.toPath (io/file config-file)) remote-alias slot-name))))
