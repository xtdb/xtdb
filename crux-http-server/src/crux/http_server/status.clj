(ns crux.http-server.status
  (:require [clojure.pprint :as pp]
            [clojure.walk :as walk]
            [crux.api :as api]
            [crux.http-server.util :as util]
            [muuntaja.core :as m]
            [muuntaja.format.core :as mfc]))

(defn ->status-html-encoder [opts]
  (reify mfc/EncodeToBytes
    (encode-to-bytes [_ {:keys [status-map attribute-stats node-options] :as res} charset]
      (let [^String resp (util/raw-html {:title "/_status"
                                         :options opts
                                         :results {:status-results {:status-map status-map
                                                                    :attribute-stats attribute-stats}}})]
        (.getBytes resp ^String charset)))))

(defn ->status-muuntaja [opts]
  (m/create (-> m/default-options
                (update :formats select-keys ["application/edn" "application/transit+json"])
                (assoc :default-format "application/edn")
                (m/install {:name "text/html"
                            :encoder [->status-html-encoder opts]
                            :return :bytes}))))

(defmulti transform-query-resp
  (fn [resp req]
    (get-in req [:muuntaja/response :format])))

(defmethod transform-query-resp "text/html" [{:keys [status-map crux-node node-options] :as res} _]
  {:status (if (or (not (contains? status-map :crux.zk/zk-active?))
                   (:crux.zk/zk-active? status-map))
             200
             500)
   :body (merge res
                {:attribute-stats (api/attribute-stats crux-node)
                 :node-options node-options})})

(defmethod transform-query-resp :default [{:keys [status-map] :as res} _]
  {:status (if (or (not (contains? status-map :crux.zk/zk-active?))
                   (:crux.zk/zk-active? status-map))
             200
             500)
   :body status-map})

(defn status [{:keys [crux-node] :as options}]
  (let [status-muuntaja (->status-muuntaja options)]
    (fn [req]
      (let [req (cond->> req
                  (not (get-in req [:muuntaja/response :format])) (m/negotiate-and-format-request status-muuntaja))]
        (->> (transform-query-resp (assoc options :status-map (api/status crux-node)) req)
             (m/format-response status-muuntaja req))))))
