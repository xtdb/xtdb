(ns xtdb.jackson
  (:require [jsonista.core :as json])
  (:import (java.io Reader)
           (xtdb.api.query QueryRequest)
           (xtdb.api.tx TxRequest)
           (xtdb.jackson JsonLdModule XtdbMapper)))

(def ^com.fasterxml.jackson.databind.ObjectMapper json-ld-mapper
  (json/object-mapper {:encode-key-fn true
                       :decode-key-fn true
                       :modules [(JsonLdModule.)]}))

(defn read-tx-req ^TxRequest [^Reader rdr]
  (.readValue XtdbMapper/TX_OP_MAPPER rdr TxRequest))

(defn read-query-req ^QueryRequest [^Reader rdr]
  (.readValue XtdbMapper/QUERY_MAPPER rdr QueryRequest))
