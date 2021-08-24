(ns ^:no-doc crux.tx.conform
  (:require [crux.codec :as c]
            [crux.db :as db]
            [crux.error :as err]
            [crux.io :as cio]
            [clojure.set :as set])
  (:import [java.util Date UUID]))

(defn- check-eid [eid]
  (when-not (and (some? eid) (c/valid-id? eid))
    (throw (ex-info "invalid entity id" {:eid eid})))
  eid)

(defn- check-doc [doc {::keys [->doc], :or {->doc identity}}]
  (let [doc (->doc doc)]
    (when-not (and (map? doc)
                   (every? keyword? (keys doc)))
      (throw (ex-info "invalid doc" {:doc doc})))

    (-> doc
        (update :xt/id check-eid))))

(defn- check-valid-time [valid-time {::keys [->valid-time], :or {->valid-time identity}}]
  (let [valid-time (->valid-time valid-time)]
    (when-not (instance? Date valid-time)
      (throw (ex-info "invalid valid-time" {:valid-time valid-time})))
    valid-time))

(declare conform-tx-op)

(defmulti ^:private conform-tx-op-type
  (fn [op decoders]
    (first op))
  :default ::default)

(defmethod conform-tx-op-type ::default [op _decoders]
  (throw (ex-info "Invalid tx op" {:op op})))

(defmethod conform-tx-op-type :crux.tx/put [[_ doc start-valid-time end-valid-time :as _op] decoders]
  (let [doc (check-doc doc decoders)
        doc-id (c/hash-doc doc)]
    {:op :crux.tx/put
     :eid (:xt/id doc)
     :doc-id doc-id
     :docs {doc-id doc}
     :start-valid-time (some-> start-valid-time (check-valid-time decoders))
     :end-valid-time (some-> end-valid-time (check-valid-time decoders))}))

(defmethod conform-tx-op-type :crux.tx/delete [[_ eid start-valid-time end-valid-time :as _op] decoders]
  {:op :crux.tx/delete
   :eid (check-eid eid)
   :start-valid-time (some-> start-valid-time (check-valid-time decoders))
   :end-valid-time (some-> end-valid-time (check-valid-time decoders))})

(defmethod conform-tx-op-type :crux.tx/cas [[_ old-doc new-doc at-valid-time :as op] decoders]
  (let [old-doc (some-> old-doc (check-doc decoders))
        new-doc (some-> new-doc (check-doc decoders))
        old-doc-id (some-> old-doc c/hash-doc)
        new-doc-id (some-> new-doc c/hash-doc)]
    (when-not (or (nil? (:xt/id old-doc))
                  (nil? (:xt/id new-doc))
                  (= (:xt/id old-doc) (:xt/id new-doc)))
      (throw (ex-info "CaS document IDs don't match" {:old-doc old-doc, :new-doc new-doc, :op op})))

    {:op :crux.tx/cas
     :eid (or (:xt/id old-doc) (:xt/id new-doc))
     :old-doc-id old-doc-id
     :new-doc-id new-doc-id
     :docs (into {} (filter val) {old-doc-id old-doc, new-doc-id new-doc})
     :at-valid-time (some-> at-valid-time (check-valid-time decoders))}))

(defmethod conform-tx-op-type :crux.tx/match [[_ eid doc at-valid-time :as op] decoders]
  (let [doc (some-> doc (check-doc decoders))
        doc-id (c/hash-doc doc)]
    {:op :crux.tx/match
     :eid (check-eid eid)
     :at-valid-time (some-> at-valid-time (check-valid-time decoders))
     :doc-id doc-id
     :docs (when doc
             {doc-id doc})}))

(defmethod conform-tx-op-type :crux.tx/evict [[_ eid :as _op] _decoders]
  {:op :crux.tx/evict
   :eid (check-eid eid)})

(defmethod conform-tx-op-type :crux.tx/fn [[_ fn-eid & args :as _op] _decoders]
  (let [arg-doc {:xt/id (UUID/randomUUID)
                 :crux.db.fn/args args}
        arg-doc-id (c/hash-doc arg-doc)]
    (into {:op :crux.tx/fn
           :fn-eid (check-eid fn-eid)
           :arg-doc-id arg-doc-id
           :docs {arg-doc-id arg-doc}}
          (when (= 1 (count args))
            (when-let [tx-ops (:crux.api/tx-ops (first args))]
              {:crux.api/tx-ops (mapv conform-tx-op tx-ops)})))))

(defn conform-tx-op
  ([op] (conform-tx-op op {}))
  ([op decoders]
   (try
     (when-not (vector? op)
       (throw (ex-info "tx-op must be a vector" {:op op})))

     (conform-tx-op-type op decoders)
     (catch Exception e
       (throw (err/illegal-arg :invalid-tx-op
                               {::err/message (str "invalid tx-op: " (.getMessage e))
                                :op op}
                               e))))))

(defmulti ->tx-op :op :default ::default)

(defmethod ->tx-op :crux.tx/put [{:keys [op doc-id start-valid-time end-valid-time docs]}]
  (cond-> [op (get docs doc-id)]
    start-valid-time (conj start-valid-time)
    end-valid-time (conj end-valid-time)))

(defmethod ->tx-op :crux.tx/delete [{:keys [op eid start-valid-time end-valid-time]}]
  (cond-> [op eid]
    start-valid-time (conj start-valid-time)
    end-valid-time (conj end-valid-time)))

(defmethod ->tx-op :crux.tx/match [{:keys [op eid docs doc-id at-valid-time]}]
  (cond-> [op eid (get docs doc-id)]
    at-valid-time (conj at-valid-time)))

(defmethod ->tx-op :crux.tx/cas [{:keys [op docs old-doc-id new-doc-id at-valid-time]}]
  (cond-> [op (get docs old-doc-id) (get docs new-doc-id)]
    at-valid-time (conj at-valid-time)))

(defmethod ->tx-op :crux.tx/evict [{:keys [op eid]}]
  [op eid])

(defmethod ->tx-op :crux.tx/fn [{:keys [op fn-eid docs arg-doc-id]}]
  (cond-> [op fn-eid]
    arg-doc-id (into (get-in docs [arg-doc-id :crux.db.fn/args]))))

(defmethod ->tx-op ::default [tx-op]
  (throw (err/illegal-arg :invalid-op
                          {::err/message (str "invalid op: " (pr-str tx-op))})))

(defmulti ->tx-event :op :default ::default)

(defmethod ->tx-event :crux.tx/put [{:keys [op eid doc-id start-valid-time end-valid-time]}]
  (cond-> [op (c/new-id eid) doc-id]
    start-valid-time (conj start-valid-time)
    end-valid-time (conj end-valid-time)))

(defmethod ->tx-event :crux.tx/delete [{:keys [op eid start-valid-time end-valid-time]}]
  (cond-> [op (c/new-id eid)]
    start-valid-time (conj start-valid-time)
    end-valid-time (conj end-valid-time)))

(defmethod ->tx-event :crux.tx/match [{:keys [op eid doc-id at-valid-time]}]
  (cond-> [op (c/new-id eid) doc-id]
    at-valid-time (conj at-valid-time)))

(defmethod ->tx-event :crux.tx/cas [{:keys [op eid old-doc-id new-doc-id at-valid-time]}]
  (cond-> [op (c/new-id eid) old-doc-id new-doc-id]
    at-valid-time (conj at-valid-time)))

(defmethod ->tx-event :crux.tx/evict [{:keys [op eid]}]
  [op eid])

(defmethod ->tx-event :crux.tx/fn [{:keys [op fn-eid arg-doc-id]}]
  (cond-> [op (c/new-id fn-eid)]
    arg-doc-id (conj arg-doc-id)))

(defmethod ->tx-event ::default [tx-op]
  (throw (err/illegal-arg :invalid-op
                          {::err/message (str "invalid op: " (pr-str tx-op))})))

(defmulti <-tx-event first
  :default ::default)

(defmethod <-tx-event :crux.tx/put [evt]
  (zipmap [:op :eid :content-hash :start-valid-time :end-valid-time] evt))

(defmethod <-tx-event :crux.tx/delete [evt]
  (zipmap [:op :eid :start-valid-time :end-valid-time] evt))

(defmethod <-tx-event :crux.tx/cas [evt]
  (zipmap [:op :eid :old-content-hash :new-content-hash :valid-time] evt))

(defmethod <-tx-event :crux.tx/match [evt]
  (zipmap [:op :eid :content-hash :valid-time] evt))

(defmethod <-tx-event :crux.tx/evict [[op eid & args]]
  (let [[start-valid-time end-valid-time] (filter #(instance? Date %) args)
        [keep-latest? keep-earliest?] (filter boolean? args)]
    {:op :crux.tx/evict
     :eid eid
     :start-valid-time start-valid-time, :end-valid-time end-valid-time
     :keep-latest? keep-latest?, :keep-earliest? keep-earliest?}))

(defmethod <-tx-event :crux.tx/fn [evt]
  (zipmap [:op :fn-eid :args-content-hash] evt))

(defn conformed-tx-events->doc-hashes [tx-events]
  (->> tx-events
       (mapcat #(keep % [:content-hash :old-content-hash :new-content-hash :args-content-hash]))
       (remove #{c/nil-id-buffer})))

(defn tx-events->doc-hashes [tx-events]
  (->> tx-events
       (map <-tx-event)
       (conformed-tx-events->doc-hashes)))

(defn tx-events->tx-ops [document-store tx-events]
  (let [docs (->> (db/fetch-docs document-store (tx-events->doc-hashes tx-events))
                  (cio/map-vals c/crux->xt))]
    (for [[op id & args] tx-events]
      (into [op]
            (concat (when (contains? #{:crux.tx/delete :crux.tx/evict :crux.tx/fn} op)
                      [(c/new-id id)])
                    (case op
                      :crux.tx/fn
                      (for [arg args]
                        (if-let [{:keys [:crux.db.fn/tx-events] :as tx-log-entry} (get docs arg)]
                          (-> tx-log-entry
                              (dissoc :xt/id :crux.db.fn/tx-events)
                              (assoc :crux.api/tx-ops (tx-events->tx-ops document-store tx-events)))
                          arg))

                      :crux.tx/match
                      (cons id
                            (for [arg args]
                              (get docs arg arg)))

                      (for [arg args]
                        (get docs arg arg))))))))

(defn flatten-tx-fn-ops [{:keys [op] :as tx-op}]
  (or (when (= :crux.tx/fn op)
        (when-let [tx-ops (:crux.api/tx-ops tx-op)]
          (mapcat flatten-tx-fn-ops tx-ops)))
      [tx-op]))
