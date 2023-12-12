(ns xtdb.jackson
  (:require [jsonista.core :as json])
  (:import (com.fasterxml.jackson.databind.module SimpleModule)
           (xtdb.jackson JsonLdModule OpsDeserializer PutDeserializer DeleteDeserializer EraseDeserializer
                         TxDeserializer CallDeserializer)
           (xtdb.tx Ops Put Delete Erase Tx Call)
           (xtdb.query Query OutSpec Query$From Query$Limit Query$Offset Query$OrderBy
                       Query$QueryTail Query$Unify Query$UnifyClause Query$Pipeline Query$Return
                       Query$UnnestCol Query$UnnestVar Expr
                       TransactionKey ArgSpec ColSpec VarSpec Basis QueryMap
                       QueryDeserializer OutSpecDeserializer FromDeserializer
                       LimitDeserializer OffsetDeserializer OrderByDeserializer
                       UnnestColDeserializer ReturnDeserializer QueryTailDeserializer
                       VarSpecDeserializer UnnestVarDeserializer UnifyDeserializer UnifyClauseDeserializer
                       PipelineDeserializer TxKeyDeserializer ArgSpecDeserializer ColSpecDeserializer
                       BasisDeserializer QueryMapDeserializer ExprDeserializer)))

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

(def ^com.fasterxml.jackson.databind.ObjectMapper tx-op-mapper
  (json/object-mapper {:encode-key-fn true
                       :decode-key-fn true
                       :modules [(JsonLdModule.)
                                 (doto (SimpleModule. "xtdb.tx")
                                   (.addDeserializer Ops (OpsDeserializer.))
                                   (.addDeserializer Put (PutDeserializer.))
                                   (.addDeserializer Delete (DeleteDeserializer.))
                                   (.addDeserializer Erase (EraseDeserializer.))
                                   (.addDeserializer Call (CallDeserializer.))
                                   (.addDeserializer Tx (TxDeserializer.)))]}))

(def ^com.fasterxml.jackson.databind.ObjectMapper query-mapper
  (json/object-mapper {:encode-key-fn true
                       :decode-key-fn true
                       :modules [(JsonLdModule.)
                                 (doto (SimpleModule. "xtdb.query")
                                   (.addDeserializer QueryMap (QueryMapDeserializer.))
                                   (.addDeserializer Query (QueryDeserializer.))
                                   (.addDeserializer Query$QueryTail (QueryTailDeserializer.))
                                   (.addDeserializer Query$Unify (UnifyDeserializer.))
                                   (.addDeserializer Query$UnifyClause (UnifyClauseDeserializer.))
                                   (.addDeserializer Query$Pipeline (PipelineDeserializer.))
                                   (.addDeserializer Query$From (FromDeserializer.))
                                   (.addDeserializer Query$Limit (LimitDeserializer.))
                                   (.addDeserializer Query$Offset (OffsetDeserializer.))
                                   (.addDeserializer Query$OrderBy (OrderByDeserializer.))
                                   (.addDeserializer Query$Return (ReturnDeserializer.))
                                   (.addDeserializer Query$UnnestCol (UnnestColDeserializer.))
                                   (.addDeserializer Query$UnnestVar (UnnestVarDeserializer.))
                                   (.addDeserializer OutSpec (OutSpecDeserializer.))
                                   (.addDeserializer ColSpec (ColSpecDeserializer.))
                                   (.addDeserializer ArgSpec (ArgSpecDeserializer.))
                                   (.addDeserializer VarSpec (VarSpecDeserializer.))
                                   (.addDeserializer TransactionKey (TxKeyDeserializer.))
                                   (.addDeserializer Basis (BasisDeserializer.))
                                   (.addDeserializer Expr (ExprDeserializer.)))]}))
