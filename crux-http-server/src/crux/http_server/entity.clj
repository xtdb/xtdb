(ns crux.http-server.entity
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [cognitect.transit :as transit]
            [crux.api :as crux]
            [crux.codec :as c]
            [crux.error :as ce]
            [crux.http-server.entity-ref :as entity-ref]
            [crux.http-server.json :as http-json]
            [crux.http-server.util :as util]
            [crux.io :as cio]
            [juxt.clojars-mirrors.jsonista.v0v3v1.jsonista.core :as j]
            [juxt.clojars-mirrors.muuntaja.v0v6v8.muuntaja.core :as m]
            [juxt.clojars-mirrors.muuntaja.v0v6v8.muuntaja.format.core :as mfc]
            [juxt.clojars-mirrors.spec-tools.v0v10v5.spec-tools.core :as st])
  (:import crux.io.Cursor
           [java.io Closeable OutputStream]))

(s/def ::sort-order keyword?)
(s/def ::start-valid-time ::util/date)
(s/def ::end-valid-time ::util/date)
(s/def ::start-tx-time ::util/date)
(s/def ::end-tx-time ::util/date)
(s/def ::start-tx-id ::util/tx-id)
(s/def ::end-tx-id ::util/tx-id)
(s/def ::history boolean?)
(s/def ::with-corrections boolean?)
(s/def ::with-docs boolean?)
(s/def ::query-params
  (s/keys :opt-un [::util/eid
                   ::util/eid-edn
                   ::util/eid-json
                   ::history
                   ::sort-order
                   ::util/valid-time
                   ::util/tx-time
                   ::util/tx-id
                   ::start-valid-time
                   ::start-tx-time
                   ::start-tx-id
                   ::end-valid-time
                   ::end-tx-time
                   ::end-tx-id
                   ::with-corrections
                   ::with-docs
                   ::util/link-entities?]))

(s/def ::response-spec
  (st/spec
   {:spec map?}))

(defn entity-links
  [db result]
  (letfn [(recur-on-result [result & key]
            (if (and (c/valid-id? result) (crux/entity db result) (not= (first key) :xt/id))
              (entity-ref/->EntityRef result)
              (cond
                (map? result) (map (fn [[k v]] [k (recur-on-result v k)]) result)
                (sequential? result) (map recur-on-result result)
                :else result)))]
    (into {} (recur-on-result result))))

(defn ->entity-html-encoder [{:keys [crux-node http-options]}]
  (reify mfc/EncodeToBytes
    (encode-to-bytes [_ {:keys [eid no-entity? not-found? cause entity ^Closeable entity-history] :as res} charset]
      (let [^String resp (util/raw-html {:title "/_crux/entity"
                                         :crux-node crux-node
                                         :http-options http-options
                                         :results (cond
                                                    no-entity? nil
                                                    not-found? {:entity-results {"error" (str eid " entity not found")}}
                                                    entity-history (try
                                                                     {:entity-results (iterator-seq entity-history)}
                                                                     (finally
                                                                       (.close entity-history)))
                                                    entity {:entity-results entity}
                                                    :else  {:entity-results {"error" res}})})]
        (.getBytes resp ^String charset)))))

(defn ->edn-encoder [_]
  (reify
    mfc/EncodeToOutputStream
    (encode-to-output-stream [_ {:keys [entity ^Cursor entity-history] :as res} _]
      (fn [^OutputStream output-stream]
        (with-open [w (io/writer output-stream)]
          (cond
            entity-history (try
                             (print-method (or (iterator-seq entity-history) '()) w)
                             (finally
                               (cio/try-close entity-history)))
            entity (print-method entity w)
            :else (.write w ^String (pr-str res))))))))

(defn- ->tj-encoder [_]
  (reify
    mfc/EncodeToOutputStream
    (encode-to-output-stream [_ {:keys [entity ^Cursor entity-history] :as res} _]
      (fn [^OutputStream output-stream]
        (let [w (transit/writer output-stream :json {:handlers util/tj-write-handlers})]
          (cond
            entity (transit/write w entity)
            entity-history (try
                             (transit/write w (or (iterator-seq entity-history) '()))
                             (finally
                               (cio/try-close entity-history)))
            :else (transit/write w res)))))))

(defn- ->json-encoder [_]
  (reify
    mfc/EncodeToOutputStream
    (encode-to-output-stream [_ {:keys [entity ^Cursor entity-history] :as res} _]
      (fn [^OutputStream output-stream]
        (cond
          entity (j/write-value output-stream entity http-json/crux-object-mapper)
          entity-history (try
                           (j/write-value output-stream
                                          (->> (iterator-seq entity-history)
                                               (map http-json/camel-case-keys))
                                          http-json/crux-object-mapper)
                           (finally
                             (cio/try-close entity-history)))
          :else (j/write-value output-stream res http-json/crux-object-mapper))))))

(defn ->entity-muuntaja [opts]
  (m/create (-> m/default-options
                (dissoc :formats)
                (assoc :return :output-stream
                       :default-format "application/edn")
                (m/install {:name "text/html"
                            :encoder [->entity-html-encoder opts]
                            :return :bytes})
                (m/install {:name "application/transit+json"
                            :encoder [->tj-encoder]})
                (m/install {:name "application/edn"
                            :encoder [->edn-encoder]})
                (m/install {:name "application/json"
                            :encoder [->json-encoder]}))))

(defn search-entity-history [{:keys [crux-node]} {:keys [eid valid-time tx-time tx-id sort-order with-corrections with-docs
                                                         start-valid-time start-tx-time start-tx-id
                                                         end-valid-time end-tx-time end-tx-id]}]
  (try
    (let [db (util/db-for-request crux-node {:valid-time valid-time
                                             :tx-time tx-time
                                             :tx-id tx-id})
          history-opts {:with-corrections? with-corrections
                        :with-docs? with-docs
                        :start-valid-time start-valid-time
                        :start-tx (->> {:crux.tx/tx-time start-tx-time
                                        :crux.tx/tx-id start-tx-id}
                                       (into {} (filter val))
                                       not-empty)
                        :end-valid-time end-valid-time
                        :end-tx (->> {:crux.tx/tx-time end-tx-time
                                     :crux.tx/tx-id end-tx-id}
                                    (into {} (filter val))
                                    not-empty)}
          entity-history (crux/open-entity-history db eid sort-order history-opts)]
      {:entity-history entity-history})
    (catch Exception e
      {:error e})))

(defn search-entity [{:keys [crux-node]} {:keys [eid valid-time tx-time link-entities?]}]
  (try
    (let [db (util/db-for-request crux-node {:valid-time valid-time
                                             :tx-time tx-time})
          entity (crux/entity db eid)]
      (cond
        (empty? entity) {:eid eid :not-found? true}
        link-entities? {:entity (entity-links db entity)}
        :else {:entity entity}))
    (catch Exception e
      {:error e})))

(defn transform-query-params [req]
  (let [{:keys [eid eid-edn eid-json] :as query-params} (get-in req [:parameters :query])]
    (->
     (if (= "text/html" (get-in req [:muuntaja/response :format]))
       (assoc query-params :with-docs true :link-entities? true)
       query-params)
     (assoc :eid (or eid-edn eid-json eid)))))

(defmulti transform-query-resp
  (fn [resp req]
    (get-in req [:muuntaja/response :format])))

(defmethod transform-query-resp "text/html" [{:keys [error no-entity? not-found? error] :as res} _]
  (cond
    error (throw error)
    :else {:status (cond
                     no-entity? 400
                     not-found? 404
                     :else 200)
           :body res}))

(defmethod transform-query-resp :default [{:keys [error no-entity? eid not-found?] :as res} _]
  (cond
    no-entity? (throw (ce/illegal-arg :missing-eid {::ce/message "Missing eid"}))
    not-found? {:status 404, :body {:error (str eid " entity not found")}}
    error (throw error)
    :else {:status 200, :body res}))

(defn entity-state [options]
  (fn [req]
    (let [{:keys [eid history] :as query-params} (transform-query-params req)]
      (-> (cond
            (nil? eid) {:no-entity? true}
            history (search-entity-history options query-params)
            :else (search-entity options query-params))
          (transform-query-resp req)))))
