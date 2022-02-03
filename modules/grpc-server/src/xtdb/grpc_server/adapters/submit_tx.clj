(ns xtdb.grpc-server.adapters.submit-tx
  (:require [xtdb.grpc-server.utils :as utils]
            [xtdb.api :as xt])
  (:gen-class))

(defmacro dbg [x] `(let [x# ~x] (println "put->:\n" '~x "=\n" x# "\n") x#))
(defn ->document [{:keys [document]}]
  (->> document :kind :struct-value :fields
     (reduce (fn [coll [k v]] (assoc coll (keyword k) (str \" (-> v :kind :string-value) \"))) {})))


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
    :else     (throw "Unknown transaction type")))

(defn edn->grpc [edn])

(defn grpc->edn [grpc]
  (->> grpc
    (map (fn [x] (get x :transaction-type)))
    (mapv #(transaction-type %))))
