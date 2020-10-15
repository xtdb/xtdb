(ns ^:no-doc crux.tx.conform
  (:require [crux.codec :as c]
            [crux.error :as err]
            [crux.db :as db]
            [clojure.instant :as inst])
  (:import (java.util UUID)))

(defn- check-eid [eid op]
  (when-not (and (some? eid) (c/valid-id? eid))
    (throw (ex-info "invalid entity id" {:eid eid, :op op})))
  eid)

(def ^:dynamic *xform-doc* identity)

(defn- check-doc [doc op]
  (let [doc (*xform-doc* doc)]
    (when-not (and (map? doc)
                   (every? keyword? (keys doc)))
      (throw (ex-info "invalid doc" {:op op, :doc doc})))

    (-> doc
        (update :crux.db/id check-eid op))))

(defn- check-valid-time [valid-time op]
  (or (when (inst? valid-time)
        valid-time)
      (when (string? valid-time)
        (try
          (inst/read-instant-date valid-time)
          (catch Exception _e)))
      (throw (ex-info "invalid valid-time" {:valid-time valid-time, :op op}))))

(defmulti ^:private conform-tx-op-type first
  :default ::default)

(defmethod conform-tx-op-type ::default [op]
  (throw (ex-info "Invalid tx op" {:op op})))

(defmethod conform-tx-op-type :crux.tx/put [[_ doc start-valid-time end-valid-time :as op]]
  (let [doc (check-doc doc op)
        doc-id (c/new-id doc)]
    {:op :crux.tx/put
     :eid (:crux.db/id doc)
     :doc-id doc-id
     :docs {doc-id doc}
     :start-valid-time (some-> start-valid-time (check-valid-time op))
     :end-valid-time (some-> end-valid-time (check-valid-time op))}))

(defmethod conform-tx-op-type :crux.tx/delete [[_ eid start-valid-time end-valid-time :as op]]
  {:op :crux.tx/delete
   :eid (check-eid eid op)
   :start-valid-time (some-> start-valid-time (check-valid-time op))
   :end-valid-time (some-> end-valid-time (check-valid-time op))})

(defmethod conform-tx-op-type :crux.tx/cas [[_ old-doc new-doc at-valid-time :as op]]
  (let [old-doc (some-> old-doc (check-doc op))
        new-doc (some-> new-doc (check-doc op))
        old-doc-id (some-> old-doc c/new-id)
        new-doc-id (some-> new-doc c/new-id)]
    (when-not (or (nil? (:crux.db/id old-doc))
                  (nil? (:crux.db/id new-doc))
                  (= (:crux.db/id old-doc) (:crux.db/id new-doc)))
      (throw (ex-info "CaS document IDs don't match" {:old-doc old-doc, :new-doc new-doc, :op op})))

    {:op :crux.tx/cas
     :eid (or (:crux.db/id old-doc) (:crux.db/id new-doc))
     :old-doc-id old-doc-id
     :new-doc-id new-doc-id
     :docs (into {} (filter val) {old-doc-id old-doc, new-doc-id new-doc})
     :at-valid-time (some-> at-valid-time (check-valid-time op))}))


(defmethod conform-tx-op-type :crux.tx/match [[_ eid doc at-valid-time :as op]]
  (let [doc (some-> doc (check-doc op))
        doc-id (c/new-id doc)]
    {:op :crux.tx/match
     :eid (check-eid eid op)
     :at-valid-time (some-> at-valid-time (check-valid-time op))
     :doc-id doc-id
     :docs (when doc
             {doc-id doc})}))

(defmethod conform-tx-op-type :crux.tx/evict [[_ eid :as op]]
  {:op :crux.tx/evict
   :eid (check-eid eid op)})

(defmethod conform-tx-op-type :crux.tx/fn [[_ fn-eid & args :as op]]
  (merge {:op :crux.tx/fn
          :fn-eid (check-eid fn-eid op)}
         (when (seq args)
           (let [arg-doc {:crux.db/id (UUID/randomUUID)
                          :crux.db.fn/args args}
                 arg-doc-id (c/new-id arg-doc)]
             {:arg-doc-id arg-doc-id
              :docs {arg-doc-id arg-doc}}))))

(defn conform-tx-op [op]
  (try
    (when-not (vector? op)
      (throw (ex-info "tx-op must be a vector" {:op op})))

    (conform-tx-op-type op)
    (catch Exception e
      (throw (err/illegal-arg :invalid-tx-op
                              {::err/message (str "invalid tx-op: " (.getMessage e))})))))

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
  (let [[start-valid-time end-valid-time] (filter inst? args)
        [keep-latest? keep-earliest?] (filter boolean? args)]
    {:op :crux.tx/evict
     :eid eid
     :start-valid-time start-valid-time, :end-valid-time end-valid-time
     :keep-latest? keep-latest?, :keep-earliest? keep-earliest?}))

(defmethod <-tx-event :crux.tx/fn [evt]
  (zipmap [:op :fn-eid :args-content-hash] evt))

(defn tx-events->doc-hashes [tx-events]
  (->> tx-events
       (map <-tx-event)
       (mapcat #(keep % [:content-hash :old-content-hash :new-content-hash :args-content-hash]))
       (remove #{c/nil-id-buffer})))

(defn tx-events->tx-ops [document-store tx-events]
  (let [docs (db/fetch-docs document-store (tx-events->doc-hashes tx-events))]
    (for [[op id & args] tx-events]
      (into [op]
            (concat (when (contains? #{:crux.tx/delete :crux.tx/evict :crux.tx/fn} op)
                      [(c/new-id id)])
                    (case op
                      :crux.tx/fn
                      (for [arg args]
                        (if-let [{:keys [:crux.db.fn/tx-events] :as tx-log-entry} (get docs arg)]
                          (-> tx-log-entry
                              (dissoc :crux.db/id :crux.db.fn/tx-events)
                              (assoc :crux.api/tx-ops (tx-events->tx-ops document-store tx-events)))
                          arg))

                      (for [arg args]
                        (get docs arg arg))))))))
