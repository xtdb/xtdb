(ns xtdb.basis
  (:require [xtdb.error :as err]
            [xtdb.time :as time])
  (:import [com.google.protobuf Timestamp]
           [java.time Instant]
           [java.util Base64]
           [xtdb.proto.basis MessageIds SystemTimes TimeBasis TxBasis]))

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
  (when basis-str
    (try
      (let [basis (TxBasis/parseFrom (.decode (Base64/getDecoder) basis-str))]
        (into {}
              (map (fn [[db-name ^MessageIds message-ids-msg]]
                     [db-name (vec (.getMessageIdsList message-ids-msg))]))
              (.getDatabasesMap basis)))
      (catch Exception e
        (throw (err/incorrect ::invalid-basis (str "Invalid basis: " (.getMessage e))
                              {::err/cause e, :basis basis-str}))))))

(defn merge-tx-tokens [l r]
  (let [l (<-tx-basis-str l)
        r (<-tx-basis-str r)]
    (->tx-basis-str
     (merge-with (fn [l-msg-ids r-msg-ids]
                   (assert (= (count l-msg-ids) (count r-msg-ids))
                           "Merging db-bases with different number of partitions")
                   (mapv max l-msg-ids r-msg-ids))
                 l r))))

(defn ->time-basis-str
  "db-system-times :: {db-name [system-time]}"
  [db-system-times]
  
  (let [builder (TimeBasis/newBuilder)]
    (doseq [[db-name system-times] db-system-times]
      (let [system-times-msg (-> (SystemTimes/newBuilder)
                                 (.addAllSystemTimes
                                  (map (fn [ts]
                                         (if ts
                                           (let [instant (time/->instant ts)]
                                             (-> (Timestamp/newBuilder)
                                                 (.setSeconds (.getEpochSecond instant))
                                                 (.setNanos (.getNano instant))
                                                 (.build)))
                                           (Timestamp/getDefaultInstance)))
                                       system-times))
                                 (.build))]
        (.putDatabases builder db-name system-times-msg)))
    (-> (.build builder)
        (.toByteArray)
        (->> (.encodeToString (Base64/getEncoder))))))

(defn <-time-basis-str [^String basis-str]
  (when basis-str
    (try
      (let [basis (TimeBasis/parseFrom (.decode (Base64/getDecoder) basis-str))]
        (into {}
              (map (fn [[db-name ^SystemTimes system-times-msg]]
                     [db-name (vec (map (fn [^Timestamp timestamp]
                                          (when-not (= timestamp (Timestamp/getDefaultInstance))
                                            (Instant/ofEpochSecond (.getSeconds timestamp) (.getNanos timestamp))))
                                        (.getSystemTimesList system-times-msg)))]))
              (.getDatabasesMap basis)))
      (catch Exception e
        (throw (err/incorrect ::invalid-basis (str "Invalid basis: " (.getMessage e))
                              {::err/cause e, :basis basis-str}))))))
