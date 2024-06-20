(ns xtdb.query-ra
  (:require [xtdb.indexer :as idx]
            [xtdb.protocols :as xtp]
            [xtdb.query :as q]
            [xtdb.serde :as serde]
            [xtdb.time :as time]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (java.util.function Consumer)
           (xtdb ICursor)
           (xtdb.api TransactionKey)
           xtdb.api.query.IKeyFn
           xtdb.indexer.IIndexer
           (xtdb.query IQuerySource PreparedQuery)
           (xtdb.vector RelationReader)))

(defn- <-cursor
  ([^ICursor cursor] (<-cursor cursor #xt/key-fn :kebab-case-keyword))
  ([^ICursor cursor ^IKeyFn key-fn]
   (let [!res (volatile! (transient []))]
     (.forEachRemaining cursor
                        (reify Consumer
                          (accept [_ rel]
                            (vswap! !res conj! (vr/rel->rows rel key-fn)))))
     (persistent! @!res))))

(defn query-ra
  ([query] (query-ra query {}))
  ([query {:keys [allocator node params preserve-blocks? with-col-types? key-fn] :as query-opts
           :or {key-fn (serde/read-key-fn :kebab-case-keyword)}}]
   (let [^IIndexer indexer (util/component node :xtdb/indexer)
         query-opts (cond-> query-opts
                      node (-> (time/after-latest-submitted-tx node)
                               (update :after-tx time/max-tx (get-in query-opts [:basis :at-tx]))
                               (doto (-> :after-tx (idx/await-tx node)))))
         allocator (or allocator (util/component node :xtdb/allocator) )]

     (with-open [^RelationReader
                 params-rel (if params
                              (vw/open-params allocator params)
                              vw/empty-params)]
       (let [^PreparedQuery pq (if node
                                 (let [^IQuerySource q-src (util/component node ::q/query-source)]
                                   (.prepareRaQuery q-src query indexer query-opts))
                                 (q/prepare-ra query))
             bq (.bind pq (-> (select-keys query-opts [:basis :after-tx :table-args :default-tz :default-all-valid-time?])
                              (assoc :params params-rel)))]
         (util/with-open [res (.openCursor bq)]
           (let [rows (-> (<-cursor res (serde/read-key-fn key-fn))
                          (cond->> (not preserve-blocks?) (into [] cat)))]
             (if with-col-types?
               {:res rows, :col-types (update-vals (into {} (.columnFields bq)) types/field->col-type)}
               rows))))))))
