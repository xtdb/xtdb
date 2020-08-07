(ns crux.http-server.status
  (:require [clojure.pprint :as pp]
            [clojure.walk :as walk]
            [crux.api :as api]
            [crux.http-server.util :as util]
            [muuntaja.core :as m]
            [muuntaja.format.core :as mfc]))

(defn sort-map [map]
  (->> map
       (walk/postwalk
        (fn [map] (cond->> map
                  (map? map) (into (sorted-map)))))))

(defn attribute-stats->html-elements [stats-map]
  [:div.node-info__content
   [:table.table
    [:thead.table__head
     [:tr
      [:th "Attribute"]
      [:th "Count (across all versions)"]]]
    (into
     [:tbody.table__body]
     (for [[key value] (sort-by (juxt val key) #(compare %2 %1) stats-map)]
       (when value
         [:tr.table__row.body__row
          [:td.table__cell.body__cell
           [:a
            {:href (format "/_crux/query?find=%s&where=%s" (format "[%s]" (name key)) (format "[e %s %s]" key (name key)))}
            (with-out-str (pp/pprint key))]]
          [:td.table__cell.body__cell (with-out-str (pp/pprint value))]])))]])

(defn status-map->html-elements [status-map]
  (into
   [:div.node-info__content
    (for [[key value] (sort-map status-map)]
      (when value
        [:p
         [:span.node-info__key (with-out-str (pp/pprint key))]
         [:span.node-info__value (with-out-str (pp/pprint value))]]))]))

(defn ->status-html-encoder [opts]
  (reify mfc/EncodeToBytes
    (encode-to-bytes [_ {:keys [status-map attribute-stats node-options] :as res} charset]
      (let [^String resp (util/raw-html {:body
                                         [:div.status
                                          [:h1 "Status"]
                                          [:div.node-info__container
                                           [:div.node-info
                                            [:h2.node-info__title "Overview"]
                                            (status-map->html-elements status-map)]
                                           [:div.node-info
                                            [:h2.node-info__title "Current Configuration"]
                                            (status-map->html-elements node-options)]]
                                          [:div.node-attributes
                                           [:h2.node-info__title "Attribute Cardinalities"]
                                           (attribute-stats->html-elements attribute-stats)]]
                                         :title "/_status"
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

(defn status [req {:keys [status-muuntaja crux-node] :as options}]
  (let [req (cond->> req
              (not (get-in req [:muuntaja/response :format])) (m/negotiate-and-format-request status-muuntaja))]
    (->> (transform-query-resp (assoc options :status-map (api/status crux-node)) req)
         (m/format-response status-muuntaja req))))
