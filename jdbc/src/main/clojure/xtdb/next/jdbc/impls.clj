(ns xtdb.next.jdbc.impls
  (:require [next.jdbc.protocols :as njp])
  (:import xtdb.api.Xtdb))

(extend-protocol njp/Connectable
  Xtdb
  (get-connection [this opts]
    (njp/get-connection {:dbtype "xtdb"
                         :classname "xtdb.jdbc.XtdbDriver"
                         :dbname (:dbname opts "xtdb")
                         :host "localhost"
                         :port (.getServerPort this)
                         :options (:conn-opts opts "-c fallback_output_format=transit")}
                        opts)))

(extend-protocol njp/Executable
  Xtdb
  (-execute ^clojure.lang.IReduceInit [this sql-params opts]
    (with-open [conn (njp/get-connection this opts)]
      (njp/-execute conn sql-params opts)))

  (-execute-one [this sql-params opts]
    (with-open [conn (njp/get-connection this opts)]
      (njp/-execute-one conn sql-params opts)))

  (-execute-all [this sql-params opts]
    (with-open [conn (njp/get-connection this opts)]
      (njp/-execute-all conn sql-params opts))))
