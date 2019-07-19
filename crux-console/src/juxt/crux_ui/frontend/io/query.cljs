(ns juxt.crux-ui.frontend.io.query
  (:require [clojure.core.async :as async
             :refer [take! put! <! >! timeout chan alt! go go-loop]]
            [re-frame.core :as rf]
            [medley.core :as m]
            [juxt.crux-lib.async-http-client :as crux-api]
            [promesa.core :as p]))

(defn post-opts [body]
  #js {:method "POST"
       :body body
       :headers #js {:Content-Type "application/edn"}})

(def node-client (crux-api/new-api-client "http://localhost:8080"))

(defn on-exec-success [resp]
  (rf/dispatch [:evt.io/query-success resp]))

(defn on-stats-success [resp]
  (rf/dispatch [:evt.io/stats-success resp]))

(defn on-tx-success [resp]
  (rf/dispatch [:evt.io/tx-success resp]))

(defn on-histories-success [eid->history]
  (rf/dispatch [:evt.io/histories-fetch-success eid->history]))

(defn on-histories-docs-success [eid->history]
  (rf/dispatch [:evt.io/histories-with-docs-fetch-success eid->history]))



(defn exec-q [query-text vt tt]
  (-> node-client
      (crux-api/db vt tt)
      (crux-api/q query-text)
      (p/then on-exec-success)))

(defn exec-tx [query-text]
  (-> node-client
      (crux-api/submitTx query-text)
      (p/then on-tx-success)))

(defn exec [{:keys [query-vt query-tt raw-input query-analysis] :as query}]
  (let [qtype (:crux.ui/query-type query-analysis)]
    (if (false? query-analysis)
      (println "err") ; TODO feedback to UI, or rather, UI shouldn't let it get this far
      (case qtype
        :crux.ui.query-type/query     (exec-q raw-input query-vt query-tt)
        :crux.ui.query-type/tx-multi  (exec-tx raw-input)
        :crux.ui.query-type/tx-single (exec-tx raw-input)))))

(defn fetch-stats []
  (-> node-client
      (crux-api/attributeStats)
      (p/then on-stats-success)))

(defn fetch-history [eid]
  (crux-api/historyRange node-client eid nil nil nil nil))
; (fetch-history :ids/fashion-ticker-2)

(defn fetch-histories
  "Fetches histories, without docs"
  [eids]
  (-> (p/all (map fetch-history eids))
      (p/then #(zipmap eids %))
      (p/then on-histories-success)))
; (fetch-histories [:ids/fashion-ticker-2])

(defn fetch-docs [hashes]
  (p/all (map #(crux-api/document node-client %) hashes)))

(defn- merge-docs-into-histories [eids->histories hash->doc]
  (let [with-doc
        (fn [{ch :crux.db/content-hash :as history-entry}]
          (assoc history-entry :crux.query/doc (hash->doc ch)))
        merge-docs-into-history
        (fn [eid history]
          (map with-doc history))]
    (m/map-kv-vals merge-docs-into-history eids->histories)))

#_(merge-docs-into-histories
    {:ids/fashion-ticker-2 [{:crux.db/content-hash "49556fb568926e9f4d1d053f65fe86969baed9ab"}
                            {:crux.db/content-hash "41f5d2050100fa23c0084042bda7a59d8d41d07b"}]}
    {"49556fb568926e9f4d1d053f65fe86969baed9ab" {:crux.db/id :ids/fashion-ticker-2,
                                                 :ticker/price 77,
                                                 :ticker/currency :currency/rub},
     "41f5d2050100fa23c0084042bda7a59d8d41d07b" {:crux.db/id :ids/fashion-ticker-2,
                                                 :ticker/price 75,
                                                 :ticker/currency :currency/rub}})


(defn on-histories-docs-error [err]
  (println :on-histories-docs-error err))

(defn fetch-histories-docs
  "Fetches histories, without docs"
  [eids->histories]
  (let [doc-hashes (->> eids->histories vals flatten (map :crux.db/content-hash))]
    (-> (fetch-docs doc-hashes)
        (p/then #(zipmap doc-hashes %))
        (p/then (fn [hash->doc] (merge-docs-into-histories eids->histories hash->doc)))
        (p/then on-histories-docs-success)
        (p/catch on-histories-docs-error))))


#_(fetch-histories-docs
    {:ids/fashion-ticker-2
     [{:crux.db/content-hash "49556fb568926e9f4d1d053f65fe86969baed9ab"}
      {:crux.db/content-hash "41f5d2050100fa23c0084042bda7a59d8d41d07b"}]})

(comment
  (exec-q (pr-str '{:full-results? true
                    :find [e]
                    :where [[e :name "Pablo"]]})))
