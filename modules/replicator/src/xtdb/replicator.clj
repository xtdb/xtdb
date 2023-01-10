(ns xtdb.replicator
  (:require [cognitect.transit :as t]
            [xtdb.api :as xt]
            [xtdb.codec :as c]
            [xtdb.db :as db]
            [xtdb.system :as sys])
  (:import clojure.lang.MapEntry
           java.io.OutputStream
           java.lang.AutoCloseable
           (java.nio.file CopyOption Files OpenOption Path StandardCopyOption StandardOpenOption)
           java.nio.file.attribute.FileAttribute
           [java.time DayOfWeek Duration Instant LocalDate LocalDateTime LocalTime Month MonthDay OffsetDateTime OffsetTime Period Year YearMonth ZoneId ZonedDateTime]
           (xtdb.codec Id EDNId)))

(defprotocol TestReplicator
  (force-close-file! [test-replicator]))

(def tj-write-handlers
  (merge {Id (t/write-handler "xtdb/oid" str)
          EDNId (t/write-handler "xtdb/oid" str)
          (Class/forName "[B") (t/write-handler "xtdb/base64" c/base64-writer)}

         (-> {Period "time/period"
              LocalDate "time/date"
              LocalDateTime  "time/date-time"
              ZonedDateTime "time/zoned-date-time"
              OffsetTime "time/offset-time"
              Instant "time/instant"
              OffsetDateTime "time/offset-date-time"
              ZoneId "time/zone"
              DayOfWeek "time/day-of-week"
              LocalTime "time/time"
              Month "time/month"
              Duration "time/duration"
              Year "time/year"
              YearMonth "time/year-month"
              MonthDay "time/month-day"}
             (update-vals #(t/write-handler % str)))))

(def ^"[Ljava.nio.file.attribute.FileAttribute;" empty-file-attrs
  (make-array FileAttribute 0))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn ->local-dir-replicator {::sys/args {:path {:spec ::sys/path, :required? true}
                                          :tx-events-per-file {:spec ::sys/pos-int, :required? true, :default 10000}}}
  [{:keys [^Path path, ^long tx-events-per-file]}]

  (Files/createDirectories path empty-file-attrs)

  (with-open [dir-stream (Files/newDirectoryStream path)]
    (assert (empty? dir-stream)
            (format "already exists: '%s'" path)))

  (let [!next-file-idx (atom 0)
        !out-file (atom nil)
        !ev-count (atom 0)

        ;; could write these to the local KV store if we think they'll grow beyond memory
        !eids (atom {})]

    (letfn [(open-new-file! []
              (let [tmp-path (Files/createTempFile "tmp-xtdb-log" ".transit.json" empty-file-attrs)
                    os (Files/newOutputStream tmp-path
                                              (into-array OpenOption #{StandardOpenOption/CREATE
                                                                       StandardOpenOption/WRITE
                                                                       StandardOpenOption/TRUNCATE_EXISTING}))
                    w (t/writer os :json {:handlers tj-write-handlers})]
                {:tmp-path tmp-path, :os os, :w w}))

            (close-file! []
              (when-let [{:keys [tmp-path ^OutputStream os]} (first (reset-vals! !out-file nil))]
                (reset! !ev-count 0)
                (let [next-idx (first (swap-vals! !next-file-idx inc))]
                  (.close os)
                  (Files/move tmp-path (.resolve path (format "log.%d.transit.json" next-idx))
                              (into-array CopyOption [StandardCopyOption/ATOMIC_MOVE])))))

            (write-obj! [obj ev-count]
              (let [{:keys [^OutputStream os w]} (or @!out-file
                                                     (reset! !out-file (open-new-file!)))
                    total-ev-count (swap! !ev-count + ev-count)]
                (t/write w obj)
                (.write os (int \newline))
                (.flush os)
                (when (>= total-ev-count tx-events-per-file)
                  (close-file!))))]

      (reify
        db/Replicator
        (begin-replicator-tx [_]
          (let [!tx (atom nil)
                !docs (atom {})
                !coords (atom [])]
            (reify db/ReplicatorTx
              (index-replicator-tx [_ tx] (reset! !tx (select-keys tx [::xt/tx-id ::xt/tx-time])))

              (index-replicator-docs [_ docs]
                (swap! !docs into docs)
                (swap! !eids into
                       (comp (keep (fn [[_ doc]]
                                     (when-let [id (:crux.db/id doc)]
                                       (MapEntry/create (c/new-id id) id)))))
                       docs))

              (index-coords [_ coords] (swap! !coords into coords))

              (commit-replicator-tx [_]
                (let [docs @!docs, eids @!eids, coords @!coords]
                  (write-obj! [:commit @!tx
                               (vec (for [[tx-op {:keys [content-hash] :as coord}] coords]
                                      [tx-op (-> (dissoc coord :content-hash)
                                                 (assoc :doc (get docs content-hash))
                                                 (update :eid (some-fn eids identity)))]))]
                              (count coords))))

              (abort-replicator-tx [_]
                (write-obj! [:abort @!tx] 1)))))

        TestReplicator
        (force-close-file! [_] (close-file!))

        AutoCloseable
        (close [_] (close-file!))))))
