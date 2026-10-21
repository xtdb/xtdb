(ns xtdb.mysql
  "Clojure config surface for the MySQL external source — registers a
   `->remote-factory` method so a `:remotes` entry in node config can name a
   MySQL upstream (parallel to `xtdb.postgres`)."
  (:require [xtdb.remote :as remote])
  (:import [xtdb.mysql MySqlRemote$Factory]))

(defmethod remote/->remote-factory ::remote
  [_ {:keys [host port username password] :or {port 3306}}]
  (MySqlRemote$Factory. host port username password))
