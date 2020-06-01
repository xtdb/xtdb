(ns crux.ui.subscriptions
  (:require
   [clojure.string :as string]
   [cljs.reader :as reader]
   [crux.ui.common :as common]
   [re-frame.core :as rf]
   [tick.alpha.api :as t]
   [clojure.data :as data]))

(rf/reg-sub
 :db
 (fn [db _] db))

(rf/reg-sub
 ::current-route
 (fn [db _]
   (:current-route db)))

(rf/reg-sub
 ::initial-values-query
 (fn [db _]
   (let [query-params (get-in db [:current-route :query-params])
         handler (get-in db [:current-route :data :name])
         valid-time (common/datetime->date-time
                     (str (:valid-time query-params (t/now))))
         transaction-time (common/datetime->date-time
                           (:transaction-time query-params))]
     (when (= :query handler)
       {"q" (common/query-params->formatted-edn-string
             (dissoc query-params :valid-time :transaction-time))
        "vtd" (:date valid-time)
        "vtt" (:time valid-time)
        "ttd" (:date transaction-time)
        "ttt" (:time transaction-time)}))))

(rf/reg-sub
 ::initial-values-entity
 (fn [db _]
   (let [query-params (get-in db [:current-route :query-params])
         handler (get-in db [:current-route :data :name])
         valid-time (common/datetime->date-time
                     (str (:valid-time query-params (t/now))))
         transaction-time (common/datetime->date-time
                           (:transaction-time query-params))]
     (when (= :entity handler)
       {"eid" (:eid query-params)
        "vtd" (:date valid-time)
        "vtt" (:time valid-time)
        "ttd" (:date transaction-time)
        "ttt" (:time transaction-time)}))))

;; wrap this in reg-sub-raw and replace get-in with subs
(rf/reg-sub
 ::query-data-table
 (fn [db _]
   (if-let [error (get-in db [:query :error])]
     {:error error}
     (let [{:strs [query-results linked-entities]}
           (get-in db [:query :http])
           find-clause (reader/read-string (get-in db [:current-route :query-params :find]))
           table-loading? (get-in db [:query :result-pane :loading?])
           offset (->> (or (get-in db [:current-route :query-params :offset]) "0")
                       (js/parseInt))
           columns (map (fn [column]
                          {:column-key column
                           :column-name (str column)
                           :render-fn
                           (fn [_ v]
                             (if-let [link (get linked-entities v)]
                               [:a {:href link}
                                (str v)]
                               v))
                           :render-only #{:filter :sort}})
                        find-clause)
           rows (map #(zipmap find-clause %) query-results)]
       {:data
        {:columns columns
         :rows rows
         :offset offset
         :loading? table-loading?
         :filters {:input (into #{} find-clause)}}}))))

(rf/reg-sub
 ::query-result-pane-loading?
 (fn [db _]
   (get-in db [:query :result-pane :loading?])))

(rf/reg-sub
 ::entity-result-pane-loading?
 (fn [db _]
   (get-in db [:entity :result-pane :loading?])))

(rf/reg-sub
 ::entity-pane-view
 (fn [db _]
   (if (nil? (get-in db [:current-route :query-params :eid]))
     :entity-root
     (or (get-in db [:entity :right-pane :view]) :document))))

(rf/reg-sub
 ::query-data-download-link
 (fn [db [_ link-type]]
   (let [query-params (get-in db [:current-route :query-params])]
     (-> (common/route->url :query {} query-params)
         (string/replace #"query" (str "query." link-type))))))

(rf/reg-sub
 ::entity-result-pane-document
 (fn [db _]
   (if-let [error (get-in db [:entity :error])]
     {:error error}
     (let [query-params (get-in db [:current-route :query-params])
         document (get-in db [:entity :http :document "entity"])]
     {:eid (:eid query-params)
      :vt (or (:valid-time query-params) (str (t/now)))
      :tt (or (:transaction-time query-params) "Not Specified")
      :document document
      :linked-entities (get-in db [:entity :http :document "linked-entities"])}))))

(rf/reg-sub
 ::entity-result-pane-history-diffs?
 (fn [db _]
   (or (get-in db [:entity :result-pane :diffs?]) false)))

(rf/reg-sub
 ::entity-result-pane-document-error
 (fn [db _]
   (get-in db [:entity :error])))

(rf/reg-sub
 ::entity-result-pane-history
 (fn [db _]
   (let [eid (get-in db [:current-route :query-params :eid])
         history (get-in db [:entity :http :history])]
     {:eid eid
      :entity-history history})))

(defn- history-docs->diffs [entity-history]
  (map
   (fn [[x y]]
     (let [[deletions additions]
           (data/diff (:crux.db/doc x) (:crux.db/doc y))]
       (merge
        (select-keys y [:crux.tx/tx-time :crux.db/valid-time])
        {:deletions deletions
         :additions additions})))
   (partition 2 1 entity-history)))

(rf/reg-sub
 ::entity-result-pane-history-diffs
 (fn [db _]
   (let [eid (get-in db [:current-route :query-params :eid])
         history (get-in db [:entity :http :history])
         entity-history (history-docs->diffs history)]
     {:eid eid
      :up-to-date-doc (first history)
      :history-diffs entity-history})))

(rf/reg-sub
 ::form-pane-entity-view
 (fn [db _]
   (get-in db [:form-pane :entity :view])))

(rf/reg-sub
 ::form-pane-query-view
 (fn [db _]
   (get-in db [:form-pane :query :view])))

(rf/reg-sub
 ::form-pane-history
 (fn [db [_ component]]
   (get-in db [:form-pane :history component])))

(rf/reg-sub
 ::query-form-history
 (fn [db _]
   (let [valid-time #(common/datetime->date-time
                      (str (:valid-time % (t/now))))
         transaction-time #(common/datetime->date-time
                            (:transaction-time %))]
     (reverse
      (mapv
       (fn [x]
         {"q" (common/query-params->formatted-edn-string
               (dissoc x :valid-time :transaction-time))
          "vtd" (:date (valid-time x))
          "vtt" (:time (valid-time x))
          "ttd" (:date (transaction-time x))
          "ttt" (:time (transaction-time x))})
       (:query-history db))))))

(rf/reg-sub
 ::entity-form-history
 (fn [db _]
   (:entity-history db)))

(rf/reg-sub
 ::form-pane-hidden?
 (fn [db _]
   (get-in db [:form-pane :hidden?] true)))
