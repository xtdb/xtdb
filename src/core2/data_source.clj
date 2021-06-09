(ns core2.data-source
  (:require [core2.expression.temporal :as expr.temp]
            [core2.operator.scan :as scan]
            [core2.system :as sys]
            core2.tx
            [core2.util :as util])
  (:import core2.tx.TransactionInstant
           java.io.Closeable
           java.util.Date))

(definterface IQueryDataSource
  (^core2.ICursor scan [^java.util.List #_<String> colNames,
                        metadataPred
                        ^java.util.Map #_#_<String, IColumnSelector> colPreds
                        ^longs temporalMinRange
                        ^longs temporalMaxRange]))

(definterface IDataSourceFactory
  (^core2.data_source.IQueryDataSource openDataSource [^core2.tx.Watermark watermark
                                                       ^core2.tx.TransactionInstant tx
                                                       ^java.util.Date validTime]))

(deftype QueryDataSource [metadata-mgr temporal-mgr buffer-pool watermark
                          ^TransactionInstant tx, ^Date valid-time]
  IQueryDataSource
  (scan [_ col-names metadata-pred col-preds temporal-min-range temporal-max-range]
    (when-let [tx-time (.tx-time tx)]
      (expr.temp/apply-constraint temporal-min-range temporal-max-range
                                  '<= "_tx-time-start" tx-time)

      (when-not (or (contains? col-preds "_tx-time-start")
                    (contains? col-preds "_tx-time-end"))
        (expr.temp/apply-constraint temporal-min-range temporal-max-range
                                    '> "_tx-time-end" tx-time)))

    (when-not (or (contains? col-preds "_valid-time-start")
                  (contains? col-preds "_valid-time-end"))
      (expr.temp/apply-constraint temporal-min-range temporal-max-range
                                  '<= "_valid-time-start" valid-time)
      (expr.temp/apply-constraint temporal-min-range temporal-max-range
                                  '> "_valid-time-end" valid-time))

    (scan/->scan-cursor metadata-mgr temporal-mgr buffer-pool
                        watermark col-names metadata-pred col-preds
                        temporal-min-range temporal-max-range))

  Closeable
  (close [_]
    (util/try-close watermark)))

(defn ->data-source-factory {::sys/deps {:metadata-mgr :core2/metadata-manager
                                         :temporal-mgr :core2/temporal-manager
                                         :buffer-pool :core2/buffer-pool}}
  [{:keys [metadata-mgr temporal-mgr buffer-pool]}]
  (reify
    IDataSourceFactory
    (openDataSource [_ watermark tx vt]
      (QueryDataSource. metadata-mgr temporal-mgr buffer-pool watermark tx vt))))
