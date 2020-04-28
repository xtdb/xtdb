(ns crux.tx.conform
  (:require [crux.codec :as c]))

(defn- check-eid [eid op]
  (when-not (and (c/valid-id? eid) (not (string? eid)))
    (throw (ex-info "invalid entity id" {:eid eid, :op op}))))

(defn- check-doc [doc op]
  (when-not (and (map? doc)
                 (every? keyword (keys doc)))
    (throw (ex-info "invalid doc" {:op op, :doc doc})))

  (check-eid (:crux.db/id doc) op))

(defn- check-valid-time [valid-time op]
  (when-not (inst? valid-time)
    (throw (ex-info "invalid valid-time" {:valid-time valid-time, :op op}))))

(defmulti ^:private conform-tx-op-type first
  :default ::default)

(defmethod conform-tx-op-type ::default [op]
  (throw (ex-info "Invalid tx op" {:op op})))

(defmethod conform-tx-op-type :crux.tx/put [[_ doc start-valid-time end-valid-time :as op]]
  (check-doc doc op)
  (some-> start-valid-time (check-valid-time op))
  (some-> end-valid-time (check-valid-time op))

  (let [doc-id (c/new-id doc)]
    {:op :crux.tx/put
     :eid (:crux.db/id doc)
     :doc-id doc-id
     :docs {doc-id doc}
     :start-valid-time start-valid-time
     :end-valid-time end-valid-time}))

(defmethod conform-tx-op-type :crux.tx/delete [[_ eid start-valid-time end-valid-time :as op]]
  (check-eid eid op)
  (some-> start-valid-time (check-valid-time op))
  (some-> end-valid-time (check-valid-time op))

  {:op :crux.tx/delete
   :eid eid
   :start-valid-time start-valid-time
   :end-valid-time end-valid-time})

(defmethod conform-tx-op-type :crux.tx/cas [[_ old-doc new-doc at-valid-time :as op]]
  (some-> old-doc (check-doc op))
  (some-> new-doc (check-doc op))
  (some-> at-valid-time (check-valid-time op))
  (when-not (or (nil? (:crux.db/id old-doc))
                (nil? (:crux.db/id new-doc))
                (= (:crux.db/id old-doc) (:crux.db/id new-doc)))
    (throw (ex-info "CaS document IDs don't match" {:old-doc old-doc, :new-doc new-doc, :op op})))

  (let [old-doc-id (some-> old-doc c/new-id)
        new-doc-id (some-> new-doc c/new-id)]
    {:op :crux.tx/cas
     :eid (or (:crux.db/id old-doc) (:crux.db/id new-doc))
     :old-doc-id old-doc-id
     :new-doc-id new-doc-id
     :docs (into {} (filter val) {old-doc-id old-doc, new-doc-id new-doc})
     :at-valid-time at-valid-time}))


(defmethod conform-tx-op-type :crux.tx/match [[_ eid doc at-valid-time :as op]]
  (check-eid eid op)
  (some-> doc (check-doc op))
  (some-> at-valid-time (check-valid-time op))

  (let [doc-id (c/new-id doc)]
    {:op :crux.tx/match
     :eid eid
     :at-valid-time at-valid-time
     :doc-id doc-id
     :docs (when doc
             {doc-id doc})}))

(defmethod conform-tx-op-type :crux.tx/evict [[_ eid :as op]]
  (check-eid eid op)
  {:op :crux.tx/evict
   :eid eid})

(defmethod conform-tx-op-type :crux.tx/fn [[_ fn-eid arg-doc :as op]]
  (check-eid fn-eid op)

  (let [arg-doc-id (c/new-id arg-doc)]
    (merge {:op :crux.tx/fn
            :fn-eid fn-eid}
           (when arg-doc
             (check-doc arg-doc op)
             {:arg-doc-id arg-doc-id
              :docs {arg-doc-id arg-doc}}))))

(defn conform-tx-op [op]
  (try
    (when-not (vector? op)
      (throw (ex-info "tx-op must be a vector" {:op op})))

    (conform-tx-op-type op)
    (catch Exception e
      (throw (IllegalArgumentException. (str "invalid tx-op: " (.getMessage e)) e)))))

(defn ->tx-event [{:keys [op] :as conformed-op}]
  (case op
    :crux.tx/put
    (let [{:keys [op eid doc-id start-valid-time end-valid-time]} conformed-op]
      (cond-> [op eid doc-id]
        start-valid-time (conj start-valid-time)
        end-valid-time (conj end-valid-time)))

    :crux.tx/delete
    (let [{:keys [op eid start-valid-time end-valid-time]} conformed-op]
      (cond-> [op eid]
        start-valid-time (conj start-valid-time)
        end-valid-time (conj end-valid-time)))

    :crux.tx/match
    (let [{:keys [op eid doc-id at-valid-time]} conformed-op]
      (cond-> [op eid doc-id]
        at-valid-time (conj at-valid-time)))

    :crux.tx/cas
    (let [{:keys [op eid old-doc-id new-doc-id at-valid-time]} conformed-op]
      (cond-> [op eid old-doc-id new-doc-id]
        at-valid-time (conj at-valid-time)))

    :crux.tx/evict
    (let [{:keys [op eid]} conformed-op]
      [op eid])

    :crux.tx/fn
    (let [{:keys [op fn-eid arg-doc-id]} conformed-op]
      (cond-> [op fn-eid]
        arg-doc-id (conj arg-doc-id)))))
