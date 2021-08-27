(ns xtdb.http-server.status
  (:require [xtdb.api :as xt]
            [xtdb.http-server.json :as http-json]
            [xtdb.http-server.util :as util]
            [juxt.clojars-mirrors.muuntaja.v0v6v8.muuntaja.core :as m]
            [juxt.clojars-mirrors.muuntaja.v0v6v8.muuntaja.format.core :as mfc]))

(defn ->status-html-encoder [{:keys [crux-node http-options]}]
  (reify mfc/EncodeToBytes
    (encode-to-bytes [_ {:keys [status-map attribute-stats] :as res} charset]
      (let [^String resp (util/raw-html {:title "/_status"
                                         :crux-node crux-node
                                         :http-options http-options
                                         :results {:status-results {:status-map status-map
                                                                    :attribute-stats attribute-stats}}})]
        (.getBytes resp ^String charset)))))

(defn ->status-muuntaja [opts]
  (m/create (-> (util/->default-muuntaja {:json-encode-fn http-json/camel-case-keys})
                (m/install {:name "text/html"
                            :encoder [->status-html-encoder opts]
                            :return :bytes}))))

(defmulti transform-query-resp
  (fn [resp req]
    (get-in req [:muuntaja/response :format])))

(defmethod transform-query-resp "text/html" [{:keys [status-map crux-node http-options] :as res} _]
  {:status (if (or (not (contains? status-map :xtdb.zk/zk-active?))
                   (:xtdb.zk/zk-active? status-map))
             200
             500)
   :body (merge res
                {:attribute-stats (xt/attribute-stats crux-node)
                 :http-options http-options})})

(defmethod transform-query-resp :default [{:keys [status-map]} _]
  {:status (if (or (not (contains? status-map :xtdb.zk/zk-active?))
                   (:xtdb.zk/zk-active? status-map))
             200
             500)
   :body status-map})

(defn status [{:keys [crux-node] :as options}]
  (let [status-muuntaja (->status-muuntaja options)]
    (fn [req]
      (let [req (cond->> req
                  (not (get-in req [:muuntaja/response :format])) (m/negotiate-and-format-request status-muuntaja))]
        (->> (transform-query-resp (assoc options :status-map (xt/status crux-node)) req)
             (m/format-response status-muuntaja req))))))
