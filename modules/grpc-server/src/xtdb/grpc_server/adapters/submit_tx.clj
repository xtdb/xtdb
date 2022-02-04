(ns xtdb.grpc-server.adapters.submit-tx
  (:require [xtdb.grpc-server.utils :as utils]
            [xtdb.api :as xt])
  (:gen-class))

(defn ->document [{:keys [document]}]
  (utils/value->edn document))

(defn ->put [transaction]
  (let [record         (get transaction :put)
        document       (->document record)
        valid-time     (-> record :valid-time  utils/->inst)
        end-valid-time (when (utils/not-nil? valid-time) (-> record :end-valid-time  utils/->inst))]
    #_{:clj-kondo/ignore [:unresolved-namespace]}
    (filterv utils/not-nil? [::xt/put document valid-time end-valid-time])))

(defn transaction-type [transaction]
  (condp #(get  %2 %1) transaction
    :put      (->put transaction)
    :match    transaction
    :delete   transaction
    :evict    transaction
    :function transaction
    :else     (throw
               (ex-info "Unknown transaction type"
                        {:execution-id        :unknown-transaction-type
                         :available-transactions #{:put :match :delete :evict :function}}))))

(defn edn->grpc [edn]
  {:tx-id (:xtdb.api/tx-id edn)
   :tx-time (str (:xtdb.api/tx-time edn))})

(defn grpc->edn [grpc]
  (->> grpc
       (map (fn [x] (get x :transaction-type)))
       (mapv #(transaction-type %))))
