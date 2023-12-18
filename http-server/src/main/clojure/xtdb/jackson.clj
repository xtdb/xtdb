(ns xtdb.jackson
  (:require [jsonista.core :as json])
  (:import (java.io Reader)
           (xtdb.jackson JsonLdModule XtdbMapper)
           (xtdb.tx Tx)
           (xtdb.query QueryRequest)))

#_
(defn decode-throwable [{:xtdb.error/keys [message class data] :as _err}]
  (case class
    "xtdb.IllegalArgumentException" (err/illegal-arg (:xtdb.error/error-key data) data)
    "xtdb.RuntimeException" (err/runtime-err (:xtdb.error/error-key data) data)
    (ex-info message data)))

(def ^com.fasterxml.jackson.databind.ObjectMapper json-ld-mapper
  (json/object-mapper {:encode-key-fn true
                       :decode-key-fn true
                       :modules [(JsonLdModule.)]}))

(defn read-tx ^Tx [^Reader rdr]
  (.readValue XtdbMapper/TX_OP_MAPPER rdr Tx))

(defn read-query-request ^QueryRequest [^Reader rdr]
  (.readValue XtdbMapper/QUERY_MAPPER rdr QueryRequest))
