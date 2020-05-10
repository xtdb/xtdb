(ns crux.ui.events
  (:require
   [ajax.edn :as ajax-edn]
   [cljs.reader :as reader]
   [clojure.string :as string]
   [crux.ui.common :as common]
   [crux.ui.http]
   [re-frame.core :as rf]
   [tick.alpha.api :as t]))

(rf/reg-fx
 :scroll-top
 common/scroll-top)

(rf/reg-event-fx
 ::inject-metadata
 (fn [{:keys [db]} [_ title handler]]
   (let [result-meta (some-> (js/document.querySelector
                              (str "meta[title=" title "]"))
                             (.getAttribute "content"))
         edn-content (reader/read-string result-meta)]
     (if edn-content
       {:db (assoc db handler edn-content)}
       (js/console.warn "Metadata not found")))))

(defn cast-to-query-params
  [query valid-time transaction-time]
  (let [{:keys [find where limit
                offset args order-by]} query]
    (str
     (cond-> (js/URLSearchParams.)
       find (doto (.append "find" find))
       where ((fn [params] (reduce (fn [params clause] (doto params (.append "where" clause))) params where)))
       limit (doto (.append "limit" limit))
       offset (doto (.append "offset" offset))
       args (doto (.append "args" args))
       order-by ((fn [params] (reduce (fn [params clause] (doto params (.append "order-by" clause))) params order-by)))
       valid-time (doto (.append "valid-time" valid-time))
       transaction-time (doto (.append "transaction-time" transaction-time))))))

(rf/reg-event-fx
 ::go-to-query
 (fn [{:keys [db]} [_ {:keys [values]}]]
   (let [{:strs [valid-date valid-time transaction-date transaction-time]} values
         query-map (reader/read-string (get values "q"))
         parsed-valid-time (when (and valid-date valid-time)
                             (t/instant (str valid-date "T" valid-time)))
         parsed-transaction-time (when (and transaction-date transaction-time)
                                   (t/instant (str transaction-date "T" transaction-time)))
         query-params (cast-to-query-params query-map parsed-valid-time parsed-transaction-time)]
     {:dispatch [:navigate {:page :query
                            :query-params query-params}]})))

(rf/reg-event-fx
 ::fetch-query-table
 (fn [{:keys [db]} _]
   (let [query-params (doto (js/URLSearchParams. js/window.location.search) (.delete "full-results"))
         find (reader/read-string (.get query-params "find"))
         link-entities? (.get query-params "link-entities?")]
     (when (seq (str query-params))
       {:db (assoc db :table-loading? true)
        :http-xhrio {:method :get
                     :uri (str "/_query?" query-params (when-not link-entities?
                                                         "&link-entities?=true"))
                     :response-format (ajax-edn/edn-response-format)
                     :on-success [::success-fetch-query-table find]
                     :on-failure [::fail-fetch-query-table]}}))))

(rf/reg-event-fx
 ::success-fetch-query-table
 (fn [{:keys [db]} [_ find-clause result]]
   (prn "fetch query table success!")
   {:db (-> db
            (assoc :query-data
                   (assoc result "find-clause" find-clause))
            (assoc :table-loading? false))}))

(rf/reg-event-db
 ::fail-fetch-query-table
 (fn [db [_ result]]
   (prn "Failure: get query table result: " result)
   (-> db
       (assoc-in [:query-data :error]
                 (get-in result [:response :via 0 :message]))
       (assoc :table-loading? false))))

(rf/reg-event-fx
 ::go-to-entity-view
 (fn [{:keys [db]} [_ {:keys [values]}]]
   (let [{:strs [valid-date valid-time transaction-date transaction-time]} values
         eid (reader/read-string (get values "eid"))
         ;; reitit casts keywords to strings so we need to return a string for
         ;; the router
         parsed-eid (if (keyword? eid) (str ":" (name eid)) eid)
         parsed-valid-time (when (and valid-date valid-time)
                             {:valid-time
                              (t/instant (str valid-date  "T" valid-time))})
         parsed-transaction-time (when (and transaction-date transaction-time)
                                   {:transaction-time
                                    (t/instant (str transaction-date  "T"
                                                    transaction-time))})]
     {:dispatch [:navigate :entity
                 {:eid parsed-eid}
                 (merge
                  parsed-valid-time
                  parsed-transaction-time)]})))

(rf/reg-event-db
 ::query-pane-toggle
 (fn [db _]
   (update db :query-pane-show? not)))

(rf/reg-event-db
 ::set-query-view
 (fn [db [_ view]]
   (assoc db :query-view view)))

(rf/reg-event-db
 ::set-entity-view
 (fn [db [_ view]]
   (assoc db :entity-view view)))

(rf/reg-event-db
 ::set-search-view
 (fn [db [_ view]]
   (assoc db :search-view view)))
