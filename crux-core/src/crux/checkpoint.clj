(ns crux.checkpoint
  (:require [clojure.tools.logging :as log]
            [crux.io :as cio]
            [crux.system :as sys])
  (:import [java.io Closeable File]
           java.nio.file.Path
           [java.time Duration Instant]
           [java.util.concurrent Executors TimeUnit]
           java.util.Date))

(defprotocol CheckpointStore
  (available-checkpoints [store opts])
  (download-checkpoint [store checkpoint dir])
  (upload-checkpoint [store dir opts]))

(alter-meta! #'available-checkpoints assoc :arglists '([store {:keys [::cp-format]}]))
(alter-meta! #'upload-checkpoint assoc :arglists '([store dir {:keys [::cp-format tx]}]))

(defprotocol CheckpointSource
  (save-checkpoint [_ dir]))

(defprotocol Checkpointer
  (try-restore [checkpointer dir cp-format])
  (start ^java.io.Closeable [checkpointer src opts]))

(alter-meta! #'start assoc :arglists '([checkpointer src {:keys [::cp-format]}]))

(defn recent-cp? [{::keys [^Date checkpoint-at]} ^Duration approx-frequency]
  (and checkpoint-at
       (< (.getSeconds (Duration/between (.toInstant checkpoint-at) (Instant/now)))
          (/ (.getSeconds approx-frequency) 2))))

(defn checkpoint [{:keys [dir src store ::cp-format approx-frequency]}]
  (when-not (recent-cp? (first (available-checkpoints store {::cp-format cp-format})) approx-frequency)
    (try
      (when-let [{:keys [tx]} (save-checkpoint src dir)]
        (when tx
          (log/infof "Uploading checkpoint at '%s'" tx)
          (doto (upload-checkpoint store dir {:tx tx, ::cp-format cp-format})
            (->> pr-str (log/info "Uploaded checkpoint:")))))
      (finally
        (cio/delete-dir dir)))))

(defn cp-seq [^Instant start ^Duration freq]
  (lazy-seq
   (cons (.plus start (Duration/ofSeconds (rand-int (.getSeconds freq))))
         (cp-seq (.plus start freq) freq))))

(defrecord ScheduledCheckpointer [store, ^Path checkpoint-dir, ^Duration approx-frequency
                                  keep-dir-between-checkpoints? keep-dir-on-close?]
  Checkpointer
  (try-restore [_ dir cp-format]
    (when (or (not (.exists ^File dir))
              (empty? (.listFiles ^File dir)))
      (log/debug "checking for checkpoints to restore from")
      (when-let [cp (first (available-checkpoints store {::cp-format cp-format}))]
        (log/infof "restoring from %s to %s" cp dir)
        (download-checkpoint store cp dir)
        cp)))

  (start [this src {::keys [cp-format]}]
    (let [checkpoint-dir (or (some-> checkpoint-dir .toFile) (cio/create-tmpdir "checkpointing"))
          ses (Executors/newSingleThreadScheduledExecutor (cio/thread-factory "crux-checkpoint"))]
      (letfn [(run [[time & more-times]]
                (try
                  (checkpoint {:approx-frequency approx-frequency,
                               :dir checkpoint-dir,
                               :src src,
                               :store store,
                               ::cp-format cp-format})
                  (catch Exception e
                    (log/warn e "Checkpointing failed"))
                  (catch Throwable t
                    (log/warn t "Checkpointing failed, stopping checkpointing")
                    (throw t))
                  (finally
                    (when-not keep-dir-between-checkpoints?
                      (cio/delete-dir checkpoint-dir))))
                (schedule more-times))
              (schedule [times]
                (when (seq times)
                  (let [now (Instant/now)]
                    (if (.isAfter now (first times))
                      (recur (rest times))
                      (.schedule ses
                                 ^Runnable #(run times)
                                 (.toMillis (Duration/between now (first times)))
                                 TimeUnit/MILLISECONDS)))))]
        (schedule (cp-seq (Instant/now) approx-frequency)))
      (reify Closeable
        (close [_]
          (.shutdownNow ses)
          (when-not keep-dir-on-close?
            (cio/delete-dir checkpoint-dir)))))))

(defn ->checkpointer {::sys/deps {:store {:crux/module (fn [_])}}
                      ::sys/args {:checkpoint-dir {:spec ::sys/path
                                                   :required? false}
                                  :keep-dir-between-checkpoints? {:spec ::sys/boolean
                                                                  :required? true
                                                                  :default true}
                                  :keep-dir-on-close? {:spec ::sys/boolean
                                                       :required? true
                                                       :default false}
                                  :approx-frequency {:spec ::sys/duration
                                                     :required? true}}}
  [opts]
  (map->ScheduledCheckpointer opts))
