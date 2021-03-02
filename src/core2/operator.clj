(ns core2.operator
  (:require [core2.scan :as scan])
  (:import core2.buffer_pool.BufferPool
           core2.metadata.IMetadataManager
           org.apache.arrow.memory.BufferAllocator))

(definterface IOperatorFactory
  (^core2.ICursor scan [^core2.tx.Watermark watermark
                        ^java.util.List #_<String> colNames,
                        metadataPred
                        ^java.util.Map #_#_<String, IVectorPredicate> colPreds]))

(defn ->operator-factory ^core2.operator.IOperatorFactory [^BufferAllocator allocator
                                                           ^IMetadataManager metadata-mgr
                                                           ^BufferPool buffer-pool]
  (reify IOperatorFactory
    (scan [_ watermark col-names metadata-pred col-preds]
      (scan/->scan-cursor allocator metadata-mgr buffer-pool
                          watermark col-names metadata-pred col-preds))))
