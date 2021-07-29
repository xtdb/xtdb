(ns core2.data-source
  (:require [core2.expression.temporal :as expr.temp]
            [core2.indexer :as idx]
            [core2.operator.scan :as scan]
            [core2.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import core2.indexer.IChunkManager
           core2.tx.TransactionInstant))

(definterface IDataSource
  (^core2.ICursor scan [^java.util.List #_<String> colNames,
                        metadataPred
                        ^java.util.Map #_#_<String, IColumnSelector> colPreds
                        ^longs temporalMinRange
                        ^longs temporalMaxRange]))

(definterface IDataSourceFactory
  (^core2.data_source.IDataSource getDataSource [^core2.tx.TransactionInstant tx]))

(deftype DataSource [metadata-mgr temporal-mgr buffer-pool ^IChunkManager indexer,
                     ^TransactionInstant tx]
  IDataSource
  (scan [_ col-names metadata-pred col-preds temporal-min-range temporal-max-range]
    (when-let [tx-time (.tx-time tx)]
      (expr.temp/apply-constraint temporal-min-range temporal-max-range
                                  '<= "_tx-time-start" tx-time)

      (when-not (or (contains? col-preds "_tx-time-start")
                    (contains? col-preds "_tx-time-end"))
        (expr.temp/apply-constraint temporal-min-range temporal-max-range
                                    '> "_tx-time-end" tx-time)))

    (let [watermark (.getWatermark indexer)]
      (try
        (-> (scan/->scan-cursor metadata-mgr temporal-mgr buffer-pool
                                watermark col-names metadata-pred col-preds
                                temporal-min-range temporal-max-range)
            (util/and-also-close watermark))
        (catch Throwable t
          (util/try-close watermark)
          (throw t))))))

(defmethod ig/prep-key ::data-source-factory [_ opts]
  (merge {:indexer (ig/ref ::idx/indexer)
          :metadata-mgr (ig/ref :core2.metadata/metadata-manager)
          :temporal-mgr (ig/ref :core2.temporal/temporal-manager)
          :buffer-pool (ig/ref :core2.buffer-pool/buffer-pool)}
         opts))

(defmethod ig/init-key ::data-source-factory [_ {:keys [indexer metadata-mgr temporal-mgr buffer-pool]}]
  (reify
    IDataSourceFactory
    (getDataSource [_ tx]
      (DataSource. metadata-mgr temporal-mgr buffer-pool indexer tx))))
