(ns juxt.crux-ui.frontend.subs
  (:require [re-frame.core :as rf]
            [juxt.crux-ui.frontend.logic.query-analysis :as qa]))

(rf/reg-sub :subs.query/stats  (fnil :db.meta/stats  false))

(rf/reg-sub :subs.query/input-committed  (fnil :db.query/input-committed  false))
(rf/reg-sub :subs.query/input  (fnil :db.query/input  false))
(rf/reg-sub :subs.query/result (fnil :db.query/result false))
(rf/reg-sub :subs.query/error  (fnil :db.query/error  false))
(rf/reg-sub :subs.ui/editor-key  (fnil :db.ui/editor-key 0))

(rf/reg-sub :subs.query/input-edn-committed
            :<- [:subs.query/input-committed]
            qa/try-read-string)

(rf/reg-sub :subs.query/input-edn
            :<- [:subs.query/input]
            qa/try-read-string)

(rf/reg-sub :subs.query/input-malformed?
            :<- [:subs.query/input-edn]
            #(:error % false))

(rf/reg-sub
  :subs.query/analysis
  :<- [:subs.query/input-edn]
  (fn [input-edn]
    (cond
      (not input-edn) nil
      (:error input-edn) nil
      :else (qa/analyse-query input-edn))))

(rf/reg-sub :subs.query/analysis-committed (fnil :db.query/analysis-committed false))

(rf/reg-sub
  :subs.query/headers
  :<- [:subs.query/result]
  :<- [:subs.query/analysis-committed]
  (fn [[q-res q-info]]
    (when q-info
      (if (:full-results? q-info)
         (qa/analyze-full-results-headers q-res)
         (:find q-info)))))


(rf/reg-sub
  :subs.query/results-table
  :<- [:subs.query/headers]
  :<- [:subs.query/analysis-committed]
  :<- [:subs.query/result]
  (fn [[q-headers q-info q-res :as args]]
    (when (every? some? args)
      {:headers q-headers
       :rows (if (:full-results? q-info)
               (->> q-res
                    (map :crux.query/doc)
                    (map #(map % q-headers)))
               q-res)})))

(rf/reg-sub :subs.db.ui/output-tab (fnil :db.ui/output-tab :db.ui.output-tab/table))

(rf/reg-sub
  :subs.ui/output-tab
  :<- [:subs.db.ui/output-tab]
  :<- [:subs.query/analysis-committed]
  :<- [:subs.query/result]
  (fn [[out-tab q-info q-res :as args]]
    (cond
      (not q-res)         :db.ui.output-tab/empty
      (= 0 (count q-res)) :db.ui.output-tab/empty
      out-tab out-tab
      (= :crux.ui.query-type/tx-multi (:crux.ui/query-type q-info)) :db.ui.output-tab/edn
      (= 1 (count q-res)) :db.ui.output-tab/tree
      :else    :db.ui.output-tab/table)))


























