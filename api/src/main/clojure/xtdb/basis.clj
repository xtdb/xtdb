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

(defn- ->timestamp ^Timestamp [ts]
  (if ts
    (let [instant (time/->instant ts)]
      (-> (Timestamp/newBuilder)
          (.setSeconds (.getEpochSecond instant))
          (.setNanos (.getNano instant))
          (.build)))
    (Timestamp/getDefaultInstance)))

(defn- <-timestamp [^Timestamp timestamp]
  (when-not (= timestamp (Timestamp/getDefaultInstance))
    (Instant/ofEpochSecond (.getSeconds timestamp) (.getNanos timestamp))))

(defn ->time-basis-str
  "db-system-times :: {db-name [system-time]}
   db-ingest-times :: {db-name [ingest-time]} - where omitted, each partition's ingest time is its system time."
  ([db-system-times] (->time-basis-str db-system-times nil))

  ([db-system-times db-ingest-times]
   (let [builder (TimeBasis/newBuilder)]
     (doseq [db-name (distinct (concat (keys db-system-times) (keys db-ingest-times)))
             :let [ingest-times (mapv ->timestamp (get db-ingest-times db-name))
                   system-times (or (some->> (get db-system-times db-name) (mapv ->timestamp))
                                    (mapv (constantly (Timestamp/getDefaultInstance)) ingest-times))]]
       (let [system-times-msg (-> (SystemTimes/newBuilder)
                                  (.addAllSystemTimes system-times)
                                  (cond-> (and (seq ingest-times) (not= ingest-times system-times))
                                    (.addAllIngestTimes ingest-times))
                                  (.build))]
         (.putDatabases builder db-name system-times-msg)))
     (-> (.build builder)
         (.toByteArray)
         (->> (.encodeToString (Base64/getEncoder)))))))

(defn <-clock-basis-str
  "-> {db-name [{:system-time, :ingest-time}]}, one per partition"
  [^String basis-str]
  (when basis-str
    (try
      (let [basis (TimeBasis/parseFrom (.decode (Base64/getDecoder) basis-str))]
        (into {}
              (map (fn [[db-name ^SystemTimes system-times-msg]]
                     (let [system-times (mapv <-timestamp (.getSystemTimesList system-times-msg))
                           ingest-times (or (not-empty (mapv <-timestamp (.getIngestTimesList system-times-msg)))
                                            system-times)]
                       [db-name (mapv (fn [system-time ingest-time]
                                        {:system-time system-time, :ingest-time ingest-time})
                                      system-times ingest-times)])))
              (.getDatabasesMap basis)))
      (catch Exception e
        (throw (err/incorrect ::invalid-basis (str "Invalid basis: " (.getMessage e))
                              {::err/cause e, :basis basis-str}))))))

(defn- clock-times [clock-basis k]
  (update-vals clock-basis #(mapv k %)))

(defn system-times [clock-basis] (clock-times clock-basis :system-time))
(defn ingest-times [clock-basis] (clock-times clock-basis :ingest-time))

(defn <-time-basis-str [^String basis-str]
  (some-> (<-clock-basis-str basis-str) system-times))

(defn cap-basis [basis ^Instant inst]
  (-> basis
      (update-vals (fn [sys-times]
                     (mapv (fn [^Instant ts]
                             (if (and ts (.isBefore ts inst))
                               ts
                               inst))
                           sys-times)))))

