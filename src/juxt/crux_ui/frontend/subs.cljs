(ns juxt.crux-ui.frontend.subs
  (:require [re-frame.core :as rf]
            [juxt.crux-ui.frontend.logic.example-queries :as ex]
            [juxt.crux-ui.frontend.logic.plotting-data-calc :as pd]
            [juxt.crux-ui.frontend.logic.query-analysis :as qa]
            [cljs.reader :as reader]
            [juxt.crux-ui.frontend.logic.time :as time]))



; this is to force editor rerenders
(rf/reg-sub :subs.ui/editor-key  (fnil :db.ui/editor-key 0))
(rf/reg-sub :subs.sys/route  #(:db.sys/route % {:r/handler :rd/query-ui}))

(rf/reg-sub :subs.db.ui/output-side-tab
            (fnil :db.ui/output-side-tab
                  {:db.ui/output-side-tab
                   :db.ui.output-tab/table}))

(rf/reg-sub
  :subs.sys.route/query-ui-tab
  #(get-in % [:db.sys/route :r/route-params :r/output-tab] :db.ui.output-tab/table))

(rf/reg-sub
  :subs.ui/root-tab
  :<- [:subs.sys/route]
  (fn [route]
    (case (:r/handler route)
      :rd/query-perf :db.ui.root-tab/query-perf
      :rd/query-ui :db.ui.root-tab/query-ui
      :rd/query-ui-output-tab :db.ui.root-tab/query-ui
      :rd/settings :db.ui.root-tab/settings
      :db.ui.root-tab/query-ui)))



; query related root subscriptions
(rf/reg-sub :subs.query/stats  (fnil :db.meta/stats {}))
(rf/reg-sub :subs.query/input-committed  (fnil :db.query/input-committed  {}))
(rf/reg-sub :subs.query/network-in-progress?  #(:db.query/network-in-progress? % false))
(rf/reg-sub :subs.query/time  #(:db.query/time % {}))
(rf/reg-sub :subs.query/limit  #(:db.query/limit % 10000))
(rf/reg-sub :subs.sys/host  #(:db.sys/host % nil))
(rf/reg-sub :subs.sys.host/status  #(:db.sys.host/status % nil))
(rf/reg-sub :subs.query/input  (fnil :db.query/input  {}))
; (rf/reg-sub :subs.query/examples-imported (fnil :db.ui.examples/imported false))
(rf/reg-sub :subs.query/result #(:db.query/result % nil))
(rf/reg-sub :subs.query/error  #(:db.query/error % false))
(rf/reg-sub :subs.query/analysis-committed #(:db.query/analysis-committed % nil))
(rf/reg-sub :subs.db.ui.attr-history/hint? #(:db.ui.attr-history/hint? % nil))
(rf/reg-sub :subs.db.ui/side-bar #(:db.ui/sidebar % false))
(rf/reg-sub :subs.query/result-analysis (fnil :db.query/result-analysis {}))

; returns a map entity-id->simple-histories
(rf/reg-sub :subs.query/entities-simple-histories (fnil :db.query/eid->simple-history {}))
(rf/reg-sub :subs.query/entities-txes (fnil :db.query/histories {}))


; query derivatives
(rf/reg-sub
  :subs.query/input-edn-committed
  :<- [:subs.query/input-committed]
  qa/try-parse-edn-string)

(rf/reg-sub
  :subs.sys/settings
  :<- [:subs.sys/host]
  :<- [:subs.sys.host/status]
  :<- [:subs.query/limit]
  (fn [[host status limit]]
    {:db.sys/host host
     :db.sys.host/status status
     :db.query/limit limit}))

(rf/reg-sub
  :subs.query/input-edn
  :<- [:subs.query/input]
  qa/try-parse-edn-string)

(defn scalar? [v]
  (or (string? v) (number? v) (keyword? v)))

(defmulti make-tree
  (fn [m]
    (cond
      (map? m) :type/map
      (scalar? m) :type/scalar
      (seq m) :type/seq
      :else :type/unknown)))

(defmethod make-tree :type/map [m]
  (reduce-kv
    (fn [m k v]
      (conj
        m
        (if (scalar? v)
          {:title (str k " " (pr-str v))
           :leaf true}
          {:title (str k)
           :children (make-tree v)})))
    []
    m))

(defmethod make-tree :type/seq [m]
  (reduce-kv
    (fn [m k v]
      (conj
        m
        (if (scalar? v)
          {:title (str k " " v)
           :leaf true}
          {:title (str k)
           :children (make-tree v)})))
    []
    (vec m)))

(defmethod make-tree :default [m]
  {:title (pr-str m)})

#_{:children (make-tree {:price 1
                         :child {:dd {:ee 33}}})}

; (make-tree [0 011])

; (sequential? [0 12 3])



(rf/reg-sub
  :subs.query/result-tree
  :<- [:subs.query/result]
  (fn [result]
    (try
      (if (< (count result) 2)
        {:title "results"
         :children (make-tree (first result))}
        {:title "results"
         :children (make-tree result)})
      (catch js/Error e
        {:title "Failed to produce a tree"}))))

(rf/reg-sub
  :subs.query/result-count
  :<- [:subs.query/result]
  (fn [res]
    (when (not-empty res)
      (str (count res) " rows"))))

(rf/reg-sub
  :subs.query.time/vt
  :<- [:subs.query/time]
  #(:time/vt % (js/Date.)))

(rf/reg-sub
  :subs.query.time/tt
  :<- [:subs.query/time]
  #(:time/tt % (js/Date.)))

(rf/reg-sub
  :subs.query.time/vtc
  :<- [:subs.query.time/vt]
  time/date->comps)

(rf/reg-sub
  :subs.query.time/ttc
  :<- [:subs.query.time/tt]
  time/date->comps)

(rf/reg-sub
  :subs.query/input-malformed?
  :<- [:subs.query/input-edn]
  #(:error % false))

(rf/reg-sub
  :subs.query/analysis
  :<- [:subs.query/input-edn]
  (fn [input-edn]
    (cond
      (not input-edn) nil
      (:error input-edn) nil
      :else (qa/analyse-any-query input-edn))))

(rf/reg-sub
  :subs.query/is-query-map?
  :<- [:subs.query/analysis]
  (fn [analysis]
    (= :query.style/map (:query/style analysis))))

(rf/reg-sub
  :subs.query/headers
  :<- [:subs.query/result]
  :<- [:subs.query/analysis-committed]
  (fn [[q-res q-info]]
    (when q-info
      (if (:full-results? q-info)
         (vec (qa/analyse-full-results-headers q-res))
         (:find q-info)))))

(rf/reg-sub
  :subs.query/results-maps
  :<- [:subs.query/analysis-committed]
  :<- [:subs.query/result]
  (fn [[q-info q-res :as args]]
    (when (every? some? args)
      (let [attr-vec (:query/attr-vec q-info)]
        (if (:full-results? q-info)
          (map first q-res)
          (map #(zipmap attr-vec %) q-res))))))

(rf/reg-sub
  :subs.query/results-table
  :<- [:subs.query/headers]
  :<- [:subs.query/analysis-committed]
  :<- [:subs.query/results-maps]
  (fn [[q-headers q-info res-maps :as args]]
    (when (every? some? args)
      {:headers q-headers
       :attr-headers
       (if (:full-results? q-info)
         q-headers
         (:query/attr-vec q-info))
       :rows res-maps})))

(rf/reg-sub
  :subs.query/attr-stats-table
  :<- [:subs.query/stats]
  (fn [stats]
    {:headers [:attribute :frequency]
     :attr-headers [:attribute :frequency]
     :value-is-the-attribute? true
     :rows (if-not (map? stats)
             []
             (into [{:attribute :crux.db/id
                     :frequency (:crux.db/id stats)}]
                   (map #(zipmap [:attribute :frequency] %) (dissoc stats :crux.db/id))))}))


(rf/reg-sub
  :subs.output/tx-history-plot-data
  :<- [:subs.query/entities-txes]
  (fn [eids->txes]
    (if eids->txes
      (map (fn [[k v]] (pd/calc-plotly-trace--tx-scatter k v)) eids->txes))))

(rf/reg-sub
  :subs.query/attr-history-plot-data
  :<- [:subs.query/result-analysis]
  :<- [:subs.db.ui.attr-history/hint?]
  :<- [:subs.query/entities-simple-histories]
  (fn [[result-analysis hint? eids->simple-history]]
    (let [first-numeric (first (:ra/numeric-attrs result-analysis))]
      (if (and first-numeric (map? eids->simple-history))
        {:attribute first-numeric
         :hint? hint?
         :traces (map (fn [[k v]] (pd/calc-plotly-trace--attr first-numeric k v)) eids->simple-history)}))))

(rf/reg-sub
  :subs.query/examples
  (fn [{:db.ui.examples/keys [imported closed?] :as db}]
    (if-not closed?
      (or imported ex/examples))))



; UI derivative subs

(rf/reg-sub
  :subs.ui/output-side-tab
  :<- [:subs.query/result]
  :<- [:subs.db.ui/output-side-tab]
  (fn [[q-res out-tab]]
    (cond
      out-tab out-tab
      ;q-res   :db.ui.output-tab/tree
      :else   :db.ui.output-tab/attr-stats)))

(rf/reg-sub
  :subs.db.ui/screen-size
  #(:db.ui/screen-size %
     {:ui.screen/inner-height 1
      :ui.screen/inner-width 1}))

(rf/reg-sub
  :subs.ui.screen/width
  :<- [:subs.db.ui/screen-size]
  #(:ui.screen/inner-width % 1))

(rf/reg-sub
  :subs.db.ui/display-mode
  #(:db.ui/display-mode % :ui.display-mode/query))

(rf/reg-sub
  :subs.ui/display-mode
  :<- [:subs.ui.screen/width]
  :<- [:subs.db.ui/display-mode]
  (fn [[width display-mode]]
    (cond
      (> width 1200) :ui.display-mode/all
      :else display-mode)))

(rf/reg-sub
  :subs.ui.responsive-breakpoints/width-lt-800
  :<- [:subs.ui.screen/width]
  #(< % 800))

(rf/reg-sub
  :subs.ui.responsive-breakpoints/width-lt-375
  :<- [:subs.ui.screen/width]
  #(< % 375))



(rf/reg-sub
  :subs.query/error-improved
  :<- [:subs.query/error]
  (fn [err-event]
    (when err-event
      (let [fetch-err (:evt/error err-event)
            status    (:status fetch-err)
            body-raw  (:body fetch-err)
            body-edn  (reader/read-string body-raw)]

        {:query-type (:evt/query-type err-event)
         :err/http-status  status
         :err/type
         (if (<= 500 status)
           :err.type/server
           :err.type/client)
         :err/exception body-edn}))))

(rf/reg-sub
  :subs.ui/output-main-tab
  :<- [:subs.sys.route/query-ui-tab]
  :<- [:subs.query/analysis-committed]
  :<- [:subs.query/result]
  :<- [:subs.query/error]
  (fn [[out-tab q-info q-res q-err :as args]]
    (cond
      q-err :db.ui.output-tab/error
      (#{:db.ui.output-tab/attr-stats :db.ui.output-tab/attr-history} out-tab) out-tab
      (not q-res) :db.ui.output-tab/empty
      (= 0 (count q-res)) :db.ui.output-tab/empty-result
      (= :crux.ui.query-type/tx (:crux.ui/query-type q-info)) :db.ui.output-tab/edn
      out-tab out-tab
      (= 1 (count q-res)) :db.ui.output-tab/tree
      :else :db.ui.output-tab/table)))

