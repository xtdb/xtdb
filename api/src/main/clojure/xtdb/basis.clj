(ns xtdb.basis
  (:import [java.util Base64]
           [xtdb.proto.basis TxBasis MessageIds]))

(set! *unchecked-math* :warn-on-boxed)

(defn ->tx-basis-str
  "db-msg-ids :: {db-name [msg-id]}"
  [db-msg-ids]

  (let [builder (TxBasis/newBuilder)]
    (doseq [[db-name message-ids] db-msg-ids]
      (let [message-ids-msg (-> (MessageIds/newBuilder)
                                (.addAllMessageIds message-ids)
                                (.build))]
        (.putDatabases builder db-name message-ids-msg)))
    (-> (.build builder)
        (.toByteArray)
        (->> (.encodeToString (Base64/getEncoder))))))

(defn <-tx-basis-str [^String basis-str]
  (let [basis (TxBasis/parseFrom (.decode (Base64/getDecoder) basis-str))]
    (into {}
          (map (fn [[db-name ^MessageIds message-ids-msg]]
                 [db-name (vec (.getMessageIdsList message-ids-msg))]))
          (.getDatabasesMap basis))))

(defn merge-tx-tokens [l r]
  (let [l (<-tx-basis-str l)
        r (<-tx-basis-str r)]
    (->tx-basis-str
     (merge-with (fn [l-msg-ids r-msg-ids]
                   (assert (= (count l-msg-ids) (count r-msg-ids))
                            "Merging db-bases with different number of partitions")
                   (mapv max l-msg-ids r-msg-ids))
                 l r))))
