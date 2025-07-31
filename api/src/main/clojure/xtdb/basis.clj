(ns xtdb.basis
  (:require [xtdb.error :as err]
            [xtdb.time :as time])
  (:import [com.google.protobuf Timestamp]
           [java.time Instant]
           [java.util Base64]
           [xtdb.proto.basis SystemTimes TimeBasis]
           xtdb.database.Basis))

(set! *unchecked-math* :warn-on-boxed)

(defn ->tx-basis-str
  "db-msg-ids :: {db-name [msg-id]}"
  [db-msg-ids]

  (Basis/encodeTxBasisToken db-msg-ids))

(defn merge-tx-tokens [l r]
  (Basis/mergeTxBasisTokens l r))

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
