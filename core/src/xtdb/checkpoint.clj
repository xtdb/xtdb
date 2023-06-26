(ns xtdb.checkpoint
  (:require [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s]
            [clojure.java.io :as io]
            [xtdb.api :as xt]
            [xtdb.io :as xio]
            [xtdb.bus :as bus]
            [xtdb.error :as err]
            [xtdb.system :as sys])
  (:import [java.io Closeable File]
           java.net.URI
           java.nio.charset.StandardCharsets
           [java.nio.file CopyOption Files FileVisitOption LinkOption OpenOption Path Paths StandardOpenOption]
           java.nio.file.attribute.FileAttribute
           [java.time Duration Instant]
           [java.util.concurrent Executors TimeUnit]
           java.util.Date))

(defprotocol CheckpointStore
  (available-checkpoints [store opts])
  (download-checkpoint [store checkpoint dir])
  (upload-checkpoint [store dir opts])
  (cleanup-checkpoint [store opts]))

(alter-meta! #'available-checkpoints assoc :arglists '([store {:keys [::cp-format]}]))
(alter-meta! #'upload-checkpoint assoc :arglists '([store dir {:keys [::cp-format cp-at tx]}]))
(alter-meta! #'cleanup-checkpoint assoc :arglists '([store {:keys [::cp-format cp-at tx]}]))

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

(defn cp-exists-for-tx? [cp {:keys [::xt/tx-id] :as tx}]
  (= tx-id (get-in cp [:tx ::xt/tx-id])))

;; This function takes a list of checkpoints, and calculates the `checkpoints` to be deleted based on the provided retention-policy settings.
;; It expects the provided `checkpoints` to be in order of most-to-least recent.
;; We first 'split-at' the list based on `retain-at-least` (if unset, this is set to 0) - this returns two lists:
;; - The "safe" list - these will not be cleaned up, as we wish to keep at least `retain-at-least` checkpoints
;; - The "potentially deletable" list - how this is handled is dependant on whether `retain-newer-than` is set:
;; --> If unset, we return the whole "potentially deletable" list, as we only want to keep `retain-at-least` values
;; --> If set, we calculate the earliest valid date we'll accept checkpoints for and use 'drop-while' on the list for any checkpoints 
;;     newer than that date. Once this completes, whatever remains in the list is what will be deleted.
(defn calculate-deleteable-checkpoints [checkpoints {:keys [retain-newer-than retain-at-least]}]
  (let [split-point (or retain-at-least 0)
        [_latest to-check] (split-at split-point checkpoints)]
    (if-not retain-newer-than
      to-check
      (let [earliest-acceptable-date (-> (Date.)
                                         (.toInstant)
                                         (.minus retain-newer-than)
                                         (Date/from))
            checkpoint-newer-than-date (fn [{::keys [^Date checkpoint-at]}]
                                         (.before earliest-acceptable-date checkpoint-at))]
        (drop-while checkpoint-newer-than-date to-check)))))

(defn apply-retention-policy [{:keys [store ::cp-format retention-policy]}]
  (when retention-policy
    (let [all-checkpoints (available-checkpoints store {::cp-format cp-format})
          checkpoints-to-cleanup (calculate-deleteable-checkpoints all-checkpoints retention-policy)]
      (doseq [checkpoint checkpoints-to-cleanup]
        (let [checkpoint-opts {:tx (:tx checkpoint)
                               :cp-at (::checkpoint-at checkpoint)
                               ::cp-format cp-format}]
          (log/infof "Clearing up old checkpoint, %s, based on `retention-policy`" checkpoint-opts)
          (cleanup-checkpoint store checkpoint-opts))))))

(defn checkpoint [{:keys [dir bus src store ::cp-format approx-frequency] :as checkpoint-opts}]
  (let [latest-cp (first (available-checkpoints store {::cp-format cp-format}))]
    (when-not (recent-cp? latest-cp approx-frequency)
      (bus/send bus (bus/->event :healthz :begin-checkpoint))
      (when-let [{:keys [tx]} (save-checkpoint src dir)]
        (when tx
          (if (cp-exists-for-tx? latest-cp tx)
            (do
              (log/infof "Checkpoint already exists for '%s', skipping..." tx)
              (xio/delete-dir dir))
            (let [cp-at (java.util.Date.)
                  opts {:tx tx :cp-at cp-at ::cp-format cp-format}]
              (try
                (log/infof "Uploading checkpoint at '%s'" tx)
                (doto (upload-checkpoint store dir opts)
                  (->> pr-str (log/info "Uploaded checkpoint:")))
                (apply-retention-policy checkpoint-opts)
                (catch Throwable t
                  (xio/delete-dir dir)
                  (cleanup-checkpoint store opts)
                  (log/warn "Cleaned-up failed checkpoint:" opts)
                  (throw t)))))))
      (bus/send bus (bus/->event :healthz :end-checkpoint)))))

(defn cp-seq [^Instant start ^Duration freq]
  (lazy-seq
   (cons (.plus start (Duration/ofSeconds (rand-int (.getSeconds freq))))
         (cp-seq (.plus start freq) freq))))

(defrecord ScheduledCheckpointer [store bus ^Path checkpoint-dir, ^Duration approx-frequency
                                  keep-dir-between-checkpoints? keep-dir-on-close? retention-policy]
  Checkpointer
  (try-restore [_ dir cp-format]
    (when (or (not (.exists ^File dir))
              (empty? (.listFiles ^File dir)))
      (log/debug "checking for checkpoints to restore from")
      (when-let [cp (first (available-checkpoints store {::cp-format cp-format}))]
        (log/infof "restoring from %s to %s" cp dir)
        (bus/send bus (bus/->event :healthz :begin-restore))
        (download-checkpoint store cp dir)
        (bus/send bus (bus/->event :healthz :end-restore))
        cp)))

  (start [this src {::keys [cp-format]}]
    (let [checkpoint-dir (or (some-> checkpoint-dir .toFile) (xio/create-tmpdir "checkpointing"))
          ses (Executors/newSingleThreadScheduledExecutor (xio/thread-factory "xtdb-checkpoint"))]
      (letfn [(run [[time & more-times]]
                (try
                  (checkpoint {:approx-frequency approx-frequency,
                               :dir checkpoint-dir,
                               :bus bus
                               :src src,
                               :store store,
                               :retention-policy retention-policy
                               ::cp-format cp-format})
                  (catch Exception e
                    (log/warn e "Checkpointing failed"))
                  (catch Throwable t
                    (log/warn t "Checkpointing failed, stopping checkpointing")
                    (throw t))
                  (finally
                    (when-not keep-dir-between-checkpoints?
                      (xio/delete-dir checkpoint-dir))))
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
            (xio/delete-dir checkpoint-dir)))))))

(s/def ::retain-newer-than ::sys/duration)
(s/def ::retain-at-least ::sys/int)
(s/def ::retention-policy
  (s/keys :opt [::retain-newer-than ::retain-at-least]))

(defn validate-retention-policy [{:keys [retention-policy]}]
  (when retention-policy
    (when-not (or (contains? retention-policy :retain-newer-than)
                  (contains? retention-policy :retain-at-least))
      (throw (err/illegal-arg :retention-policy-missing-keys
                              {::err/message ":retention-policy requires at least one of 'retain-newer-than' or 'retain-at-least'"})))))

(defn ->checkpointer {::sys/deps {:store {:xtdb/module (fn [_])}
                                  :bus :xtdb/bus}
                      ::sys/args {:checkpoint-dir {:spec ::sys/path
                                                   :required? false}
                                  :keep-dir-between-checkpoints? {:spec ::sys/boolean
                                                                  :required? true
                                                                  :default true}
                                  :keep-dir-on-close? {:spec ::sys/boolean
                                                       :required? true
                                                       :default false}
                                  :approx-frequency {:spec ::sys/duration
                                                     :required? true}
                                  :retention-policy {:spec ::retention-policy
                                                     :required? false}}}
  [opts]
  (validate-retention-policy opts)
  (map->ScheduledCheckpointer opts))

(defn- sync-path [^Path from-root-path ^Path to-root-path]
  (doseq [^Path from-path (-> (Files/walk from-root-path Integer/MAX_VALUE (make-array FileVisitOption 0))
                              .iterator
                              iterator-seq)
          :let [to-path (.resolve to-root-path (str (.relativize from-root-path from-path)))]]
    (cond
      (Files/isDirectory from-path (make-array LinkOption 0))
      (Files/createDirectories to-path (make-array FileAttribute 0))

      (Files/isRegularFile from-path (make-array LinkOption 0))
      (Files/copy from-path to-path ^"[Ljava.nio.file.CopyOption;" (make-array CopyOption 0)))))

(defrecord FileSystemCheckpointStore [^Path root-path]
  CheckpointStore
  (available-checkpoints [_ {::keys [cp-format]}]
    (when (Files/exists root-path (make-array LinkOption 0))
      (for [metadata-path (->> (Files/newDirectoryStream root-path "checkpoint-*.edn")
                               .iterator iterator-seq
                               (sort-by (fn [^Path path]
                                          (let [[_ tx-id-str checkpoint-at] (re-matches #"checkpoint-(\d+)-(.+).edn" (str (.getFileName path)))]
                                            [(Long/parseLong tx-id-str) checkpoint-at]))
                                        #(compare %2 %1)))
            ;; Files/readString only added in JDK 11
            :let [cp (-> (Files/readAllBytes metadata-path)
                         (String. StandardCharsets/UTF_8)
                         read-string
                         (update ::cp-path #(Paths/get (URI. %))))]
            :when (= cp-format (::cp-format cp))]
        cp)))

  (download-checkpoint [_ {::keys [cp-path]} dir]
    (let [to-path (.toPath ^File dir)]
      (when-not (or (not (Files/exists to-path (make-array LinkOption 0)))
                    (empty? (.toArray (Files/list to-path))))
        (throw (IllegalArgumentException. (str "non-empty checkpoint restore dir: " to-path))))

      (try
        (sync-path cp-path to-path)
        (catch Throwable t
          (xio/delete-dir dir)
          (throw (ex-info "incomplete checkpoint restore"
                          {:cp-path cp-path
                           :local-dir to-path}
                          t))))))

  (upload-checkpoint [_ dir {:keys [tx cp-at ::cp-format]}]
    (let [from-path (.toPath ^File dir)
          cp-prefix (format "checkpoint-%s-%s" (::xt/tx-id tx) (xio/format-rfc3339-date cp-at))
          to-path (.resolve root-path cp-prefix)]

      (sync-path from-path to-path)

      (let [cp {::cp-format cp-format,
                :tx tx
                ::cp-uri (str to-path)
                ::checkpoint-at cp-at}]
        (Files/write (.resolve root-path (str cp-prefix ".edn"))
                     (.getBytes (pr-str {::cp-format cp-format,
                                         :tx tx
                                         ::cp-path (str (.toUri to-path))
                                         ::checkpoint-at cp-at})
                                StandardCharsets/UTF_8)
                     ^"[Ljava.nio.file.OpenOption;"
                     (into-array OpenOption #{StandardOpenOption/WRITE StandardOpenOption/CREATE_NEW}))
        cp)))

  (cleanup-checkpoint [_ {:keys [tx cp-at]}]
    (let [cp-prefix (format "checkpoint-%s-%s" (::xt/tx-id tx) (xio/format-rfc3339-date cp-at))
          to-path (io/file (.toString root-path) cp-prefix)]
      (xio/delete-file (str to-path ".edn"))
      (xio/delete-dir to-path))))

(defn ->filesystem-checkpoint-store {::sys/args {:path {:spec ::sys/path, :required? true}}} [{:keys [path]}]
  (->FileSystemCheckpointStore path))
