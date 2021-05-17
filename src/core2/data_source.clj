(ns core2.data-source
  (:require [core2.operator.scan :as scan]
            [core2.system :as sys]
            [core2.util :as util])
  (:import java.io.Closeable))

(definterface IQueryDataSource
  (^core2.ICursor scan [^java.util.List #_<String> colNames,
                        metadataPred
                        ^java.util.Map #_#_<String, IColumnSelector> colPreds
                        ^longs temporalMinRange
                        ^longs temporalMaxRange]))

(definterface IDataSourceFactory
  (^core2.data_source.IQueryDataSource openDataSource [^core2.tx.Watermark watermark]))

(deftype QueryDataSource [metadata-mgr temporal-mgr buffer-pool watermark]
  IQueryDataSource
  (scan [_ col-names metadata-pred col-preds temporal-min-range temporal-max-range]
    (scan/->scan-cursor metadata-mgr temporal-mgr buffer-pool
                        watermark col-names metadata-pred col-preds temporal-min-range temporal-max-range))

  Closeable
  (close [_]
    (util/try-close watermark)))

(defn ->data-source-factory {::sys/deps {:metadata-mgr :core2/metadata-manager
                                         :temporal-mgr :core2/temporal-manager
                                         :buffer-pool :core2/buffer-pool}}
  [{:keys [metadata-mgr temporal-mgr buffer-pool]}]
  (reify
    IDataSourceFactory
    (openDataSource [_ watermark]
      (QueryDataSource. metadata-mgr temporal-mgr buffer-pool watermark))))
